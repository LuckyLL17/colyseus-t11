import { decode, encode, Encoder, Reflection, type Iterator } from '@colyseus/schema';
import { InputDecoder } from '@colyseus/schema/input';
import { HandshakeSection, InputFlags, ProtocolModifier } from '@colyseus/shared-types';

import {
  InputAccessorImpl, InputBufferImpl, NO_OP_INPUT_ACCESSOR,
  compileSanitizer, seedInputZeroValues, validateSubSteps,
} from './InputBuffer.ts';
import type { OrderedPayload } from './InputBuffer.ts';
import { OrderedWindow } from './OrderedWindow.ts';
import type {
  InputAccessor, InputAPI, NormalizedInputOptions,
  DefineInputOptions, IdleDeclared,
} from './types.ts';
import type { Client, ClientPrivate } from '../Transport.ts';
import type { Room } from '../Room.ts';
import { debugAndPrintError } from '../Debug.ts';

/**
 * Module-level cache of `Reflection.encode` output keyed by input
 * constructor — pays the encoding cost once per Room class regardless of
 * room instance count. WeakMap so unused classes can be GC'd.
 */
const _inputReflectionCache = new WeakMap<Function, Uint8Array>();

/** Default ordered-window capacity (seqs) — covers a typical RTT burst at 30–60 Hz. */
const DEFAULT_RECEIVE_WINDOW = 64;
/** Default gap expiry (ms): a missing seq older than this is declared lost. */
const DEFAULT_GAP_AGE_MS = 1000;
/** Hard ceiling so a misconfigured window can't grow the park without bound. */
const MAX_RECEIVE_WINDOW = 4096;

/** @internal Clamp the configured window to a positive integer in range. */
export function normalizeWindowSize(value: number | undefined): number {
  if (value === undefined) { return DEFAULT_RECEIVE_WINDOW; }
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`[defineInput] reliable.windowSize must be an integer >= 1 (got ${value}).`);
  }
  return Math.min(value, MAX_RECEIVE_WINDOW);
}

/** @internal Clamp the configured gap age to a positive number. */
export function normalizeGapAge(value: number | undefined): number {
  if (value === undefined) { return DEFAULT_GAP_AGE_MS; }
  if (!(value > 0) || !Number.isFinite(value)) {
    throw new Error(`[defineInput] reliable.maxGapAgeMs must be a positive number of ms (got ${value}).`);
  }
  return value;
}

/**
 * Rebuild one `k`-length series of the unreliable stamp block: `newest` is the
 * absolute anchor, and each wire delta walks one slot older
 * (`out[i] = out[i+1] − Δ`). Mirrors the SDK's `_writeSeriesDeltas`.
 */
function readSeriesDeltas(buffer: Buffer, it: Iterator, k: number, newest: number): number[] {
  const out = new Array<number>(k);
  out[k - 1] = newest;
  for (let i = k - 2; i >= 0; i--) {
    out[i] = out[i + 1] - decode.number(buffer, it);
  }
  return out;
}

/**
 * Runtime behind {@link InputAPI}. A class, not a per-`define()` object literal:
 * literal (and `defineProperty`) accessors carry their closure identity in the
 * hidden class, so every room instance would get a UNIQUE map — sending shared
 * `this.inputs.*` call sites megamorphic. Prototype getters are created once,
 * every instance shares one hidden class (reads stay monomorphic), and
 * `define()` allocates a single 1-field object instead of 7 closures + a map
 * lineage per room.
 *
 * @internal
 */
class InputAPIImpl {
  private input: RoomInput;
  constructor(input: RoomInput) { this.input = input; }
  /** Registry, not room.clients: an accessor outlives its client through the
   *  reconnection window, so a dropped seat still synthesizes the idle policy. */
  get(sessionId: string): InputAccessor<any> {
    return this.input.accessors.get(sessionId) ?? NO_OP_INPUT_ACCESSOR;
  }
  // Live reads off `options` (mutated in place by a later setTimestep/
  // setFixedTimestep back-fill, so a derived tickRate is reflected); 1/hz is
  // correctly-rounded IEEE-754 → bit-identical to the client's stepSeconds.
  get tickRate(): number | undefined { return this.input.options.tickRate; }
  get stepSeconds(): number | undefined { const hz = this.input.options.tickRate; return hz ? 1 / hz : undefined; }
  get stepMs(): number | undefined { const hz = this.input.options.tickRate; return hz ? 1000 / hz : undefined; }
  // Sub-step trio: same derivation as the client handle ((1/hz)/n) — bit-identical dt.
  get subSteps(): number { return this.input.options.subSteps ?? 1; }
  get subStepSeconds(): number | undefined { const hz = this.input.options.tickRate; return hz ? (1 / hz) / (this.input.options.subSteps ?? 1) : undefined; }
  get subStepMs(): number | undefined { const hz = this.input.options.tickRate; return hz ? (1000 / hz) / (this.input.options.subSteps ?? 1) : undefined; }
}

/**
 * Per-room input subsystem, owned by {@link Room} and created lazily on the
 * first {@link Room.defineInput} call — rooms without inputs allocate none of
 * it. Owns the input options, the per-session accessor registry (decoupled
 * from `this.clients` so an entry outlives a dropped client across its
 * reconnection window), the wire stamp mode, and the encode/decode/handshake
 * machinery. Reads back into its Room for context it doesn't own (the patch
 * rate and the rewind timeline mode).
 *
 * @internal
 */
export class RoomInput {
  /** Owning room — for `patchRate` and the rewind timeline mode. */
  private room: Room<any>;

  /** Input configuration (ctor, seqField, bufferMaxSize, idle, sanitize,
   *  tickRate, subSteps). `tickRate`/`subSteps` may be back-filled by
   *  `setTimestep`/`setFixedTimestep`. Set in {@link define}. */
  options!: NormalizedInputOptions;

  /** The `InputAPI` returned to userland from `defineInput`, also the
   *  framework-owned handle the rewind binding resolves accessors through. */
  api!: InputAPI<any>;

  /** sessionId → accessor. Decoupled from `this.clients`: an entry outlives its
   *  client across the reconnection window, so {@link InputAPI.get} keeps
   *  synthesizing the idle policy for an absent seat (its buffer was cleared on
   *  leave → idle). Set on join, deleted on full leave. Read by
   *  {@link InputAPIImpl.get} (hence not `private`). */
  readonly accessors: Map<string, InputAccessor<any>> = new Map();

  /**
   * sessionId → ordered receive window for the sequenced reliable channel.
   * Keyed by SESSION (not by the client object) so it SURVIVES a reconnect:
   * the new connection allocates a fresh input buffer and re-binds the same
   * window, and the handshake negotiates its last-consumed seq with the
   * client. Deleted on full leave. Empty for rooms without
   * `defineInput({ reliable })`.
   */
  readonly #windows = new Map<string, OrderedWindow>();

  // Wire stamp mode derived from the rewind timeline, resolved once on first
  // handshake/decode then frozen so advertise + decode never disagree mid-session:
  // the renderTime/reckonTime prefix per input.
  #stampRender = false;
  #stampReckon = false;
  #stampResolved = false;

  /** One shared now-resolver handed to every accessor (see {@link InputAccessorImpl}). */
  private nowOf: () => number;

  constructor(room: Room<any>) {
    this.room = room;
    this.nowOf = () => this.room.clock.elapsedTime;
  }

  /**
   * Build the input configuration + `InputAPI` (the body of
   * {@link Room.defineInput}). Returns the api; the Room hands it back to
   * userland and keeps the framework-owned reference.
   */
  define<
    C extends new () => any,
    O extends DefineInputOptions<InstanceType<C>> = DefineInputOptions<InstanceType<C>>,
  >(
    type: C,
    opts?: O,
  ): InputAPI<InstanceType<C>, IdleDeclared<O, InstanceType<C>>> {
    // Normalize the step declaration to the canonical wire form (Hz). stepSeconds
    // / stepMs are the unit-safe spellings; both sides derive dt = 1/tickRate, so
    // one declared number drives the server's physics step AND the client's.
    const tickRate =
      opts?.stepSeconds !== undefined ? 1 / opts.stepSeconds :
      opts?.stepMs !== undefined ? 1000 / opts.stepMs :
      opts?.tickRate;
    if (
      opts?.tickRate !== undefined && opts.stepMs === undefined && opts.stepSeconds === undefined &&
      (!Number.isInteger(opts.tickRate) || opts.tickRate > 240)
    ) {
      console.warn(
        `[defineInput] tickRate is a rate in Hz (got ${opts.tickRate}); a value ` +
        `like 1000/rate is a step interval in ms — pass { stepMs } instead.`,
      );
    }
    this.options = {
      ctor: type,
      // Opt-in: a default would silently drop frames for any app with a numeric `seq` field.
      seqField: opts?.seqField,
      bufferMaxSize: opts?.bufferMaxSize ?? 32,
      idle: opts?.idle,
      // Compiled once (map → dense min/max walk); applied per decoded frame.
      sanitize: opts?.sanitize !== undefined ? compileSanitizer(opts.sanitize) : undefined,
      tickRate,
      subSteps: validateSubSteps(opts?.subSteps, 'defineInput'),
      reliable: opts?.reliable !== undefined ? {
        windowSize: normalizeWindowSize(opts.reliable.windowSize),
        maxGapAgeMs: normalizeGapAge(opts.reliable.maxGapAgeMs),
      } : undefined,
    };
    // The ordered channel releases frames INTO the per-client input buffer —
    // there is nowhere to park/release with bufferMaxSize: 0 (a latest-only
    // config). Reject the contradiction up front rather than silently dropping
    // every sequenced frame.
    if (this.options.reliable !== undefined && this.options.bufferMaxSize === 0) {
      throw new Error(
        "[defineInput] `reliable` requires `bufferMaxSize > 0` (the ordered " +
        "window releases into the input buffer). Drop `reliable` for a latest-only input.",
      );
    }
    if (!_inputReflectionCache.has(type)) {
      // SDK-deserializable ctor bytes (Reflection.decode rebuilds the ctor client-side).
      _inputReflectionCache.set(type, Reflection.encode(new Encoder(new type())));
    }
    const api = new InputAPIImpl(this) as InputAPI<InstanceType<C>, IdleDeclared<O, InstanceType<C>>>;
    this.api = api;
    return api;
  }

  // --- per-client lifecycle (called from Room._onJoin/_onLeave/etc.) ---

  /** Allocate the per-client input instance + decoder, the ring buffer (opt-in
   *  via `bufferMaxSize > 0`, for rollback/lockstep), and the accessor. On a
   *  RECONNECT this re-binds the session's surviving ordered window (the
   *  handshake then negotiates its last-consumed seq — see
   *  {@link reconnectionAck}). */
  allocate(client: Client & ClientPrivate): void {
    client._input = new this.options.ctor();
    // Wire-neutral zero values for fields with no construction default: a
    // client only transmits fields it assigns, and `undefined` here would
    // NaN-propagate into the sim (or get floor-clamped by a range sanitizer
    // into a PHANTOM held input).
    seedInputZeroValues(client._input, this.options.ctor);
    client._inputDecoder = new InputDecoder(client._input);
    client._reckonBaseline = 0; // mirrors the SDK's delta-coded stamp baseline (reset together on (re)connect)
    const maxSize = this.options.bufferMaxSize;
    if (maxSize > 0) {
      const reliable = this.options.reliable;
      // ctor → idle synthesis; client ref → idle ctx; idle policy → total drain()/next().
      // Sequenced channel: re-bind the session's surviving window (or mint one
      // on the fresh join) so the ordered queue resumes at the negotiated ack.
      let window: OrderedWindow | undefined;
      if (reliable !== undefined) {
        window = this.#windows.get(client.sessionId) ?? new OrderedWindow(
          reliable.windowSize,
          reliable.maxGapAgeMs,
          () => performance.now(),
        );
        this.#windows.set(client.sessionId, window);
      }
      client._inputBuffer = new InputBufferImpl(
        maxSize, this.options.seqField, this.options.ctor, client, this.options.idle, window,
      );
    }
    client._inputAccessor = new InputAccessorImpl(client, this.nowOf);
  }

  /** @internal Raw reckon stamp for {@link Room.allowRewindState}'s
   *  `bindReckonTime` — bypasses the accessor's resolved getter so the rewind
   *  midpoint fallback still engages for unstamped clients. instanceof-narrowed:
   *  the registry only ever holds `InputAccessorImpl` (see {@link allocate}),
   *  so the 0 fallback covers only a missing session. */
  rawReckonTime(sessionId: string): number {
    const acc = this.accessors.get(sessionId);
    return acc instanceof InputAccessorImpl ? acc.rawReckonTime : 0;
  }

  /** Register a (re)joined client's accessor. On reconnect this overwrites the
   *  dropped session's stale entry. Called after the client is in `this.clients`
   *  (so a pre-push failure can't leak it) and before onJoin (so `get()` resolves
   *  there). */
  register(sessionId: string, client: ClientPrivate): void {
    this.accessors.set(sessionId, client._inputAccessor!);
  }

  /** Freeze a leaving seat: drop pending inputs so a held (reconnecting) session
   *  idles from the first tick rather than replaying last-known moves. In the
   *  ordered channel the buffer clear also collapses the session's window to
   *  its consumed frontier — the reconnect handshake negotiates THAT seq and
   *  the client replays everything above it. `_input` (the idle ctx's
   *  `latest`) is left intact. */
  freeze(client: ClientPrivate): void {
    client._inputBuffer?.clear();
  }

  /** Drop a fully-gone session's accessor AND its ordered window (delete of a
   *  missing key is a no-op). A seat HELD for reconnection stays alive in both
   *  maps (see {@link freeze}); this runs only when the seat is released. */
  release(sessionId: string): void {
    this.accessors.delete(sessionId);
    this.#windows.get(sessionId)?.dispose();
    this.#windows.delete(sessionId);
  }

  /** Release every held accessor and ordered window (room dispose). */
  dispose(): void {
    this.accessors.clear();
    this.#windows.clear();
  }

  /**
   * Fixed-timestep hook: age out expired gaps for every held ordered window
   * BEFORE the step callback runs, so the sim only ever sees inputs the policy
   * has released (in-order fills and due expiries) this tick. No-op without
   * `defineInput({ reliable })`.
   */
  tick(): void {
    if (this.#windows.size === 0) { return; }
    const now = performance.now();
    for (const accessor of this.accessors.values()) {
      if (accessor instanceof InputAccessorImpl) {
        accessor.releaseExpired(now);
      }
    }
  }

  /** The reconnection negotiation point for a session: the last seq its sim
   *  consumed under the ordered channel (`0` for fresh joins / legacy rooms).
   *  Carried as `InputFlags.RECONNECT_ACK` so the client replays precisely the
   *  inputs above it. */
  reconnectionAck(sessionId: string): number {
    return this.#windows.get(sessionId)?.consumedSeq ?? 0;
  }

  // --- encode / decode (called from Room._onMessage) ---

  /**
   * Sanitize the freshly-decoded `client._input`, then route it by channel:
   *
   * - **ordered reliable** (`appSeq` set, opt-in `defineInput({ reliable })`):
   *   the snapshot is cloned and handed to the per-session {@link OrderedWindow}
   *   — the window dedupes redeliveries/replays, parks out-of-order frames,
   *   and only released frames enter the sim queue.
   * - **legacy reliable** (no `appSeq`): honors an optional user `seqField`
   *   dedupe, then appends in receive order.
   * - **unreliable** (`wireSeq` set): dedupes the redundancy ring by the
   *   framework wire seq (user `seqField` is for `.at()` only).
   */
  capture(
    client: ClientPrivate,
    renderTime: number = 0,
    reckonTime: number = 0,
    wireSeq?: number,
    appSeq?: number,
  ): "accepted" | "duplicate" {
    // Sanitize before anything reads it (latest, the clone below, the idle ctx).
    this.options.sanitize?.(client._input);
    const buf = client._inputBuffer;
    if (!buf) { return "accepted"; } // no consumer registered — skip the clone allocation
    const inst = client._input!;
    if (appSeq !== undefined) {
      // Ordered reliable: clone FIRST (a redelivery of the same seq still
      // decoded over `latest`, but the window keeps the already-released
      // snapshot), then let the window admit it. Stamps ride with the value
      // through the park so a frame unlocking a later gap keeps its own instant.
      const payload: OrderedPayload<any> = {
        input: inst.clone() as any,
        renderTime,
        reckonTime,
      };
      const verdict = buf.pushOrdered(appSeq, payload);
      return verdict === "duplicate" ? "duplicate" : "accepted";
    }
    if (wireSeq !== undefined) {
      // Unreliable: dedup the ring by the framework wire seq (user seqField is for .at() only).
      if (!buf.accept(wireSeq)) { return "duplicate"; }
    } else {
      // Legacy reliable: no framework seq (implicit count). Honor a user `seqField` if set.
      const seqField = this.options.seqField;
      if (seqField !== undefined) {
        const value = (inst as any)[seqField] as number;
        if (typeof value === 'number' && !buf.accept(value)) { return "duplicate"; }
      }
    }
    buf.push(inst.clone() as any, renderTime, reckonTime, wireSeq);
    return "accepted";
  }

  /** Decode a `ROOM_INPUT_RELIABLE` frame.
   *
   * Legacy shape: an optional TIMED stamp prefix then the input body.
   * Sequenced shape (opt-in, `ProtocolModifier.SEQUENCED`): the stamp prefix
   * is preceded by an explicit `[varint appSeq]` —
   *
   *   [SEQUENCED][varint seq][TIMED stamp?][...body]
   *
   * A sequenced frame on a room WITHOUT the ordered channel (or vice versa)
   * is dropped: the handshake pins the mode for the room, so a mismatch means
   * a buggy/forged client rather than something to paper over. `it` starts at
   * offset 1, so the body begins at `it.offset`. */
  decodeReliable(client: ClientPrivate, buffer: Buffer, it: Iterator, modifiers: number): void {
    if (!client._inputDecoder) { return; }

    // Optional explicit application seq (SEQUENCED bit) — read FIRST, ahead of
    // the TIMED stamp. Defines which admission path the body takes.
    let appSeq: number | undefined;
    if (modifiers & ProtocolModifier.SEQUENCED) {
      if (this.options.reliable === undefined || !client._inputBuffer?.ordered) {
        debugAndPrintError(new Error(
          "@colyseus/core: SEQUENCED reliable input on a non-sequenced room/session — dropping frame.",
        ));
        return;
      }
      appSeq = decode.number(buffer, it);
    } else if (this.options.reliable !== undefined && client._inputBuffer?.ordered) {
      debugAndPrintError(new Error(
        "@colyseus/core: legacy (unsequenced) reliable input on a sequenced session — dropping frame.",
      ));
      return;
    }

    // Optional stamp prefix (TIMED bit), DELTA-CODED, length set by this room's
    // stamp mode (the timeline value is reconstructed against the per-client
    // baseline; the wire carries only the signed change from the previous frame):
    //   both:        [varint Δreckon][uint16 renderDelta] → renderTime = reckon − renderDelta
    //   reckon-only: [varint Δreckon]
    //   render-only: [varint Δrender]
    let renderTime = 0;
    let reckonTime = 0;
    if (modifiers & ProtocolModifier.TIMED) {
      this.#resolveWireModes();
      // Reconstruct the absolute timeline value: baseline += signed delta. Both
      // sides start at 0 (so the first delta is absolute) and re-zero together on
      // (re)connect; reliable+in-order keeps the mirror locked.
      const stamp = client._reckonBaseline! += decode.number(buffer, it);
      if (this.#stampReckon && this.#stampRender) {
        reckonTime = stamp;
        const renderDelta = decode.uint16(buffer, it);
        renderTime = stamp > renderDelta ? stamp - renderDelta : 0;
      } else if (this.#stampReckon) {
        reckonTime = stamp;
      } else {
        renderTime = stamp;
      }
    }
    try {
      client._inputDecoder.decode(buffer.subarray(it.offset, buffer.byteLength));
    } catch (e: any) {
      debugAndPrintError(e);
      return;
    }
    client._lastInputReceivedAt = performance.now();
    // Mirrors the SDK's sent count; echoed back as lastInputSeq for RTT.
    // A sequenced duplicate (redelivery/replay) was decoded but not admitted —
    // the receive-time counter leads the consumed ack, so don't double-count it.
    const admitted = this.capture(client, renderTime, reckonTime, undefined, appSeq);
    if (admitted === "accepted") {
      client._receivedInputCount = (client._receivedInputCount ?? 0) + 1;
    }
  }

  /** Decode a `ROOM_INPUT_UNRELIABLE` redundancy ring — each slot carries its
   *  framework seq (base seq + position) for ring dedupe, no user seqField.
   *
   *  With the TIMED bit, a self-contained lag-comp stamp block precedes the ring:
   *
   *      [varint k][uint32 newest][varint Δ]×(k−1)
   *      [uint16 rdNewest][varint Δrd]×(k−1)        ← BOTH mode only
   *
   *  One stamp per slot, because a packet carries k inputs sampled at k
   *  different instants. The anchor is absolute and the deltas never leave the
   *  packet, so — unlike the reliable channel's running baseline — no amount of
   *  loss or reordering can desync it, and an input recovered redundantly from a
   *  later packet still arrives with its own instant. Stamps are paired
   *  positionally with `decodeAll`'s oldest→newest yields; a `k` that disagrees
   *  with the decoded slot count means a malformed packet, so the stamps are
   *  dropped rather than misapplied (inputs still land, read live). */
  decodeUnreliable(client: ClientPrivate, buffer: Buffer, modifiers: number): void {
    if (!client._inputDecoder) { return; }

    const it: Iterator = { offset: 1 };
    let stamps: number[] | undefined;
    let renderDeltas: number[] | undefined;

    if (modifiers & ProtocolModifier.TIMED) {
      this.#resolveWireModes();
      try {
        const k = decode.number(buffer, it);
        stamps = readSeriesDeltas(buffer, it, k, decode.uint32(buffer, it));
        // BOTH mode trails the renderDelta series in the same shape, so each
        // slot keeps the latency term it was actually sampled with.
        if (this.#stampReckon && this.#stampRender) {
          renderDeltas = readSeriesDeltas(buffer, it, k, decode.uint16(buffer, it));
        }
      } catch (e: any) {
        debugAndPrintError(e);
        return;
      }
    }

    let i = 0;
    try {
      const count = client._inputDecoder.decodeAll(buffer.subarray(it.offset), (_inst, seq) => {
        // Positional pairing — `decodeAll` yields oldest→newest, the order the
        // block was written in.
        const slot = i++;
        const stamp = stamps?.[slot] ?? 0;
        let renderTime = 0, reckonTime = 0;
        if (stamp > 0) {
          if (this.#stampReckon && this.#stampRender) {
            const rd = renderDeltas?.[slot] ?? 0;
            reckonTime = stamp;
            renderTime = stamp > rd ? stamp - rd : 0;
          } else if (this.#stampReckon) {
            reckonTime = stamp;
          } else {
            renderTime = stamp;
          }
        }
        this.capture(client, renderTime, reckonTime, seq);
      });
      if (stamps !== undefined && count !== stamps.length) {
        debugAndPrintError(new Error(
          `@colyseus/core: unreliable input stamp block declared ${stamps.length} slots, decoded ${count}`
        ));
      }
    } catch (e: any) {
      debugAndPrintError(e);
      return;
    }
    client._lastInputReceivedAt = performance.now();
  }

  // --- handshake + stamp mode ---

  /** Resolve the wire stamp mode from the rewind timeline (once, then frozen). */
  #resolveWireModes(): void {
    if (this.#stampResolved) return;
    this.#stampResolved = true;
    const tl = this.room._timelineMode();
    this.#stampRender = tl?.snapshot ?? false;
    this.#stampReckon = tl?.reckon ?? false;
  }

  /**
   * The join-handshake input sections: INPUT_REFLECTION (the SDK-deserializable
   * input ctor bytes) and, when any runtime config differs from defaults,
   * INPUT_OPTIONS (`[flags uint8][tickRate varint?][patchRate varint?]
   * [subSteps varint?][receiveWindow varint?][reconnectAck varint?]`). The
   * client mirrors RENDER_TIME/RECKON_TIME (auto-stamp timeline), tickRate
   * (predict at dt=1/tickRate), patchRate (reconcile cadence), subSteps
   * (physics sub-steps per input, omitted when 1), SEQUENCED + the receive
   * window (ordered reliable channel), and RECONNECT_ACK (the one negotiated
   * value — ONLY for a session resuming a held seat). Returns `undefined`
   * when there's nothing to add.
   */
  handshakeSections(sessionId?: string, isReconnect: boolean = false): Array<{ tag: number; bytes: Uint8Array }> | undefined {
    let sections: Array<{ tag: number; bytes: Uint8Array }> | undefined;
    const inputBytes = _inputReflectionCache.get(this.options.ctor);
    if (inputBytes !== undefined) {
      sections = [{ tag: HandshakeSection.INPUT_REFLECTION, bytes: inputBytes }];
    }
    this.#resolveWireModes();
    const stampRender = this.#stampRender;
    const stampReckon = this.#stampReckon;
    const tickRate = this.options.tickRate;
    const patchRate = (typeof this.room.patchRate === "number" && this.room.patchRate > 0)
      ? Math.round(this.room.patchRate) : undefined;
    const subSteps = (this.options.subSteps !== undefined && this.options.subSteps > 1)
      ? this.options.subSteps : undefined;
    const reliable = this.options.reliable;
    // The negotiated ack point exists ONLY for a surviving ordered session
    // (frozen at the consumed frontier). Fresh joins / legacy rooms omit it.
    const reconnectAck = (reliable !== undefined && isReconnect && sessionId !== undefined)
      ? this.#windows.get(sessionId)?.consumedSeq ?? 0
      : 0;
    if (stampRender || stampReckon || tickRate || patchRate || subSteps
      || reliable !== undefined || reconnectAck > 0) {
      let flags = 0;
      if (stampRender) { flags |= InputFlags.RENDER_TIME; }
      if (stampReckon) { flags |= InputFlags.RECKON_TIME; }
      if (tickRate) { flags |= InputFlags.FIXED_TIMESTEP; }
      if (patchRate) { flags |= InputFlags.PATCH_RATE; }
      if (subSteps) { flags |= InputFlags.SUB_STEPS; }
      if (reliable !== undefined) { flags |= InputFlags.SEQUENCED; }
      if (reconnectAck > 0) { flags |= InputFlags.RECONNECT_ACK; }
      // Worst case = flags + float64 tickRate (9) + u32 patchRate (5) + 3 varints.
      const buf = new Uint8Array(40);
      const sit = { offset: 0 };
      buf[sit.offset++] = flags;
      if (tickRate) { encode.number(buf, tickRate, sit); }
      if (patchRate) { encode.number(buf, patchRate, sit); }
      if (subSteps) { encode.number(buf, subSteps, sit); }
      if (reliable !== undefined) { encode.number(buf, reliable.windowSize, sit); }
      if (reconnectAck > 0) { encode.number(buf, reconnectAck, sit); }
      (sections ??= []).push({
        tag: HandshakeSection.INPUT_OPTIONS,
        bytes: buf.subarray(0, sit.offset),
      });
    }
    return sections;
  }
}
