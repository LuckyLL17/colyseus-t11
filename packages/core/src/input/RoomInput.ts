import { decode, Decoder, encode, Encoder, Reflection, type Iterator } from '@colyseus/schema';
import { InputDecoder } from '@colyseus/schema/input';
import { HandshakeSection, InputFlags, ProtocolModifier } from '@colyseus/shared-types';

import {
  InputAccessorImpl, InputBufferImpl, NO_OP_INPUT_ACCESSOR,
  compileSanitizer, seedInputZeroValues, validateSubSteps,
} from './InputBuffer.ts';
import { ReliableSequencer } from './ReliableSequencer.ts';
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

  /** sessionId → last ORDERED (confirmed) seq for the explicit-seq reliable
   *  channel. Unlike {@link accessors} this maps the SESSION, not the
   *  connection: the value is captured at freeze (leave) and consumed at
   *  rejoin so a reconnecting sequencer restarts exactly at the confirmation
   *  point the old connection reached — "重连时双方只协商最后确认点". Only
   *  populated in {@link options.reliableSequence} mode. */
  private readonly sessionAcks: Map<string, number> = new Map();

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
      // Explicit-seq reliable channel is opt-in (see DefineInputOptions): off by
      // default so legacy clients/rooms keep the implicit receive-count path.
      reliableSequence: opts?.reliableSequence === true,
      reliableWindow: opts?.reliableWindow !== undefined
        ? Math.max(1, Math.floor(opts.reliableWindow))
        : ReliableSequencer.DEFAULT_WINDOW,
    };
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
   *  via `bufferMaxSize > 0`, for rollback/lockstep), and the accessor. A
   *  fresh connection always starts at confirmation point 0; a reconnection
   *  rebuilds at the frozen point via {@link restoreSession} once the room has
   *  proven the seat. */
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
      // ctor → idle synthesis; client ref → idle ctx; idle policy → total drain()/next().
      client._inputBuffer = new InputBufferImpl(
        maxSize, this.options.seqField, this.options.ctor, client, this.options.idle,
        this.options.reliableSequence, 0,
      );
    }
    if (this.options.reliableSequence) {
      client._inputSequencer = new ReliableSequencer(this.options.reliableWindow, 0);
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
   *  idles from the first tick rather than replaying last-known moves.
   *  `_input` (the idle ctx's `latest`) is left intact. In explicit-seq mode
   *  the session's confirmation point is captured FIRST — it is the one value
   *  reconnection restores from. */
  freeze(client: ClientPrivate, sessionId: string): void {
    const seq = client._inputSequencer;
    if (seq !== undefined) { this.sessionAcks.set(sessionId, seq.baseSeq); }
    client._inputBuffer?.clear();
    // Drop the parked out-of-order window but KEEP the confirmation point
    // (baseSeq): the wait on missing predecessors ends with the connection.
    seq?.reset(seq.baseSeq);
  }

  /** The session's frozen confirmation point for an explicit-seq reconnect
   *  (0 for a fresh join / legacy room) — consumed once when the replacement
   *  connection is allocated. */
  consumeSessionAck(sessionId: string): number {
    const ack = this.sessionAcks.get(sessionId) ?? 0;
    this.sessionAcks.delete(sessionId);
    return ack;
  }

  /** Re-initialize a reconnecting client's input state at the session's frozen
   *  confirmation point. The replacement {@link allocate} ran with baseSeq 0
   *  (the fresh-join default) before the room could prove the reconnection;
   *  once proven, this rebuilds the explicit-seq buffer + sequencer with the
   *  negotiated base so post-reconnect seqs line up — nothing older is
   *  replayed into the sim. No-op for legacy (implicit-seq) rooms. */
  restoreSession(client: Client & ClientPrivate, sessionId: string): void {
    if (!this.options.reliableSequence) { return; }
    const baseSeq = this.consumeSessionAck(sessionId);
    const maxSize = this.options.bufferMaxSize;
    if (maxSize > 0) {
      client._inputBuffer = new InputBufferImpl(
        maxSize, this.options.seqField, this.options.ctor, client, this.options.idle,
        true, baseSeq,
      );
    }
    client._inputSequencer = new ReliableSequencer(this.options.reliableWindow, baseSeq);
    client._reckonBaseline = 0;
  }

  /** Drop a fully-gone session's accessor AND any held confirmation point
   *  (delete of a missing key is a no-op). */
  release(sessionId: string): void {
    this.accessors.delete(sessionId);
    this.sessionAcks.delete(sessionId);
  }

  /** Release every held accessor (room dispose). */
  dispose(): void {
    this.accessors.clear();
    this.sessionAcks.clear();
  }

  // --- encode / decode (called from Room._onMessage) ---

  /**
   * Sanitize the freshly-decoded `client._input`, then (when buffering is on)
   * push a clone into the per-client buffer. Honors the framework seq
   * (unreliable) / a user `inputOptions.seqField` (reliable) to dedupe the
   * redundancy ring. This is the LEGACY implicit-seq admission path — the
   * explicit-seq reliable channel goes through {@link captureSequenced}.
   */
  capture(client: ClientPrivate, renderTime: number = 0, reckonTime: number = 0, seq?: number): void {
    // Sanitize before anything reads it (latest, the clone below, the idle ctx).
    this.options.sanitize?.(client._input);
    const buf = client._inputBuffer;
    if (!buf) { return; } // no consumer registered — skip the clone allocation
    const inst = client._input!;
    if (seq !== undefined) {
      // Unreliable: dedup the ring by the framework wire seq (user seqField is for .at() only).
      if (!buf.accept(seq)) { return; }
    } else {
      // Reliable: no framework seq (implicit count). Honor a user `seqField` if set.
      const seqField = this.options.seqField;
      if (seqField !== undefined) {
        const value = (inst as any)[seqField] as number;
        if (typeof value === 'number' && !buf.accept(value)) { return; }
      }
    }
    buf.push(inst.clone() as any, renderTime, reckonTime, seq);
  }

  /**
   * Explicit-seq reliable admission. The body was decoded into a scratch
   * instance BEFORE this call (out-of-order frames therefore never mutated the
   * live `latest`); this method:
   * 1. asks the per-session {@link ReliableSequencer} what is admittable
   *    (duplicates/stale frames are filtered pre-decode in
   *    {@link decodeReliable}),
   * 2. sanitizes each admitted REAL snapshot before it becomes visible,
   * 3. records skipped seqs as gaps and pushes real ones ORDERED into the
   *    buffer the fixed-timestep loop consumes.
   * The contiguous run released by one delivery is pushed oldest → newest so
   * the sim applies them in seq order.
   */
  private captureSequenced(
    client: ClientPrivate,
    sequencer: ReliableSequencer,
    seq: number,
    snapshot: any,
    renderTime: number,
    reckonTime: number,
  ): void {
    const buf = client._inputBuffer;
    const admissions = sequencer.admit(seq, { snapshot, renderTime, reckonTime });
    if (admissions.length === 0) { return; }
    for (const a of admissions) {
      if (a.snapshot === undefined) {
        buf?.pushGap(a.seq);               // expired gap: ack advances, sim applies nothing
        continue;
      }
      this.options.sanitize?.(a.snapshot);
      // The newest REAL admitted frame becomes `latest` (idle ctx, `.latest`
      // reads). Admissions run oldest → newest, so the last assignment wins.
      client._input = a.snapshot;
      if (buf) {
        // The buffer owns a CLONE — the scratch instance may be released, and
        // later admissions decode into fresh scratch instances.
        buf.pushOrdered(a.snapshot.clone(), a.seq, a.renderTime, a.reckonTime);
      }
    }
  }

  /** Decode a `ROOM_INPUT_RELIABLE` frame: an optional TIMED stamp prefix (shape
   *  set by this room's derived stamp mode), an optional SEQUENCED explicit seq
   *  (opt-in, after the stamp), then the input body. `it` starts at offset 1,
   *  so the body begins at `it.offset`. */
  decodeReliable(client: ClientPrivate, buffer: Buffer, it: Iterator, modifiers: number): void {
    if (!client._inputDecoder) { return; }
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

    // Explicit-seq channel (opt-in per room): the seq varint sits between the
    // stamp and the body. A client that sends the bit to a room which didn't
    // enable it (or vice versa) is a protocol mismatch — drop the frame rather
    // than decoding the seq bytes as schema fields.
    const sequenced = (modifiers & ProtocolModifier.SEQUENCED) !== 0;
    const sequencer = client._inputSequencer;
    if (sequenced && (sequencer === undefined || !this.options.reliableSequence)) {
      debugAndPrintError(new Error(
        '@colyseus/core: SEQUENCED reliable input from a session in a non-sequenced room — frame dropped.',
      ));
      return;
    }
    let orderedSeq = 0;
    if (sequenced) {
      orderedSeq = decode.number(buffer, it) >>> 0;
      // Duplicate / stale / already-parked → drop BEFORE decoding: a resent
      // input must not even overwrite `latest`, let alone re-apply a side effect.
      if (sequencer!.isDuplicate(orderedSeq)) { return; }
    } else if (sequencer !== undefined) {
      // Legacy client against a sequenced ROOM: no explicit seq. Keep it on
      // the ordered admission path (so it enters the explicit buffer and the
      // confirmation point stays continuous) by numbering the frame implicitly
      // — baseSeq+1, the transport's arrival order. The channel is ordered, so
      // an implicit seq is always contiguous and admits immediately.
      orderedSeq = (sequencer.baseSeq + 1) >>> 0;
    }

    // Any admission through the per-session policy decodes into a FRESH scratch
    // instance (a legacy client in a sequenced room included): out-of-order
    // frames never touch client._input until the policy admits them. A pure
    // legacy room (no sequencer) decodes in place exactly as before.
    const policyAdmission = sequencer !== undefined;
    let scratch: any = undefined;
    if (policyAdmission) {
      scratch = new this.options.ctor();
      seedInputZeroValues(scratch, this.options.ctor);
    }
    const body = buffer.subarray(it.offset, buffer.byteLength);
    try {
      if (scratch !== undefined) {
        // A scratch Decoder: the bound decoder's ChangeTree targets client._input.
        new Decoder(scratch).decode(body);
      } else {
        client._inputDecoder!.decode(body);
      }
    } catch (e: any) {
      debugAndPrintError(e);
      return;
    }
    client._lastInputReceivedAt = performance.now();
    // Mirrors the SDK's sent count; echoed back as lastInputSeq for RTT.
    client._receivedInputCount = (client._receivedInputCount ?? 0) + 1;

    if (policyAdmission) {
      this.captureSequenced(client, sequencer!, orderedSeq, scratch, renderTime, reckonTime);
    } else {
      this.capture(client, renderTime, reckonTime);
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
   * INPUT_OPTIONS (`[flags uint8][tickRate varint?][patchRate varint?][subSteps
   * varint?][lastAck varint?]`). The client mirrors RENDER_TIME/RECKON_TIME
   * (auto-stamp timeline), tickRate (predict at dt=1/tickRate), patchRate
   * (reconcile cadence), and subSteps (physics sub-steps per input, omitted
   * when 1). On a RECONNECTION in explicit-seq reliable mode it additionally
   * carries LAST_ACK + the session's frozen confirmation point — the one value
   * the two sides negotiate; a fresh join carries neither. Returns `undefined`
   * when there's nothing to add.
   */
  handshakeSections(client?: ClientPrivate, isReconnect: boolean = false): Array<{ tag: number; bytes: Uint8Array }> | undefined {
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
    // Reconnection only: the last ORDERED seq the frozen session reached. The
    // ack was consumed by Room.allocate() when the replacement connection was
    // built; read the sequencer's live base (identical) here.
    const lastAck = (isReconnect && this.options.reliableSequence)
      ? (client?._inputSequencer?.baseSeq ?? 0)
      : undefined;
    if (stampRender || stampReckon || tickRate || patchRate || subSteps
      || this.options.reliableSequence || lastAck !== undefined) {
      let flags = 0;
      if (stampRender) { flags |= InputFlags.RENDER_TIME; }
      if (stampReckon) { flags |= InputFlags.RECKON_TIME; }
      if (tickRate) { flags |= InputFlags.FIXED_TIMESTEP; }
      if (patchRate) { flags |= InputFlags.PATCH_RATE; }
      if (subSteps) { flags |= InputFlags.SUB_STEPS; }
      if (this.options.reliableSequence) { flags |= InputFlags.RELIABLE_SEQUENCE; }
      if (lastAck !== undefined) { flags |= InputFlags.LAST_ACK; }
      // 24B: worst case = flags + float64 tickRate (9) + uint32 patchRate (5) + subSteps (2) + lastAck (5).
      const buf = new Uint8Array(24);
      const sit = { offset: 0 };
      buf[sit.offset++] = flags;
      if (tickRate) { encode.number(buf, tickRate, sit); }
      if (patchRate) { encode.number(buf, patchRate, sit); }
      if (subSteps) { encode.number(buf, subSteps, sit); }
      if (lastAck !== undefined) { encode.number(buf, lastAck >>> 0, sit); }
      (sections ??= []).push({
        tag: HandshakeSection.INPUT_OPTIONS,
        bytes: buf.subarray(0, sit.offset),
      });
    }
    return sections;
  }
}
