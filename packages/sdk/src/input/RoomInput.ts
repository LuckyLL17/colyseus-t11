import { decode, Iterator, Reflection } from '@colyseus/schema';
import { InputEncoder } from '@colyseus/schema/input';
import { InputFlags } from '@colyseus/shared-types';

import { InputHandleImpl, type InputHandle, type InputOptions } from './InputHandle.ts';
import { NULL_CLOCK, RoomClockImpl } from '../RoomClock.ts';

// Type-only: avoids a runtime import cycle (Room imports RoomInput as a value).
import type { Room } from '../Room.ts';

/**
 * Owns all per-room input state, kept off the {@link Room} class itself: the
 * schema constructor recovered from the JOIN handshake, the server-advertised
 * stamp/rate flags, and the lazily-created {@link InputHandle}.
 *
 * Created lazily by {@link Room} only when the room declares input (an
 * `INPUT_*` handshake section arrives) or the app calls `room.input(...)`.
 * Rooms that never use input (chat / lobby / turn-based) never allocate one.
 * @internal
 */
export class RoomInput {
    #room: Room;

    // Impl type (not the public interface) so the TIMED decode can feed it the
    // server ack via the internal `ackInput()`.
    #handle?: InputHandleImpl<any>;

    // First-call context, kept only to diagnose later room.input(options) calls
    // whose options are ignored (the handle is created once — first call wins).
    #options?: InputOptions<any>;
    #encoder?: InputEncoder<any>;
    #warnedIgnoredOptions = false;

    /**
     * Schema constructor recovered via Reflection from the server's handshake
     * (the `INPUT_REFLECTION` tagged section). Falls back to `undefined` when
     * the server room didn't call `defineInput()`.
     *
     * Typed as `new () => any` (not `Schema`) on purpose — pinning to this
     * SDK's Schema type would clash with user instances coming from a
     * different copy of `@colyseus/schema` under multi-version installs.
     */
    #ctorFromReflection?: new () => any;

    /** `true` when the handshake advertised the SNAPSHOT-timeline stamp
     *  (`INPUT_OPTIONS`, `InputFlags.RENDER_TIME`). */
    #stampRender = false;

    /** `true` when the handshake advertised the RECKON-timeline stamp
     *  (`INPUT_OPTIONS`, `InputFlags.RECKON_TIME`). Set together with
     *  {@link #stampRender} ⇒ the 6-byte `[reckonTime][renderDelta]` prefix. */
    #stampReckon = false;

    /** Server-advertised fixed simulation/input step rate (Hz) from
     *  `defineInput({ tickRate })`. */
    #tickRate?: number;

    /** Server-advertised state-patch interval (ms) = the reconcile cadence. */
    #patchRate?: number;

    /** Server-advertised physics sub-steps per input tick
     *  (`setFixedTimestep(..., { subSteps })`). */
    #subSteps?: number;

    /** Server advertised the explicit-seq reliable channel
     *  (`defineInput({ reliableSequence: true })`, InputFlags.RELIABLE_SEQUENCE).
     *  When set, the handle numbers every reliable input with its own seq and
     *  negotiates/replays around the server's confirmation point. */
    #reliableSequence = false;

    /** Last ORDERED seq the server reported for this session on a reconnection
     *  handshake (InputFlags.LAST_ACK); `undefined` on a fresh join. The handle
     *  adopts it when created/reset so the replay starts exactly above the
     *  server's confirmation point. */
    #lastAck?: number;

    constructor(room: Room) {
        this.#room = room;
    }

    /** Snapshot cadence (ms), so {@link Room} can feed `clock.setPatchInterval`. */
    get patchRate(): number | undefined {
        return this.#patchRate;
    }

    /**
     * Decode the `INPUT_REFLECTION` handshake section: install schema-builder
     * field descriptors on the reconstructed class so {@link InputEncoder} can
     * read its `$values` and emit non-empty packets, then cache the ctor.
     *
     * INPUT_REFLECTION is also the signal that the server called `defineInput()`
     * and will emit TIMED-prefixed state messages — so swap the default stub
     * clock for a real {@link RoomClockImpl} unless the user already replaced
     * `room.clock` with their own. Advances `it`.
     */
    applyReflection(buffer: Uint8Array, it: Iterator, sectionEnd: number): void {
        const inputDecoder = Reflection.decode(buffer.subarray(0, sectionEnd) as any, it);
        Reflection.makeEncodable(inputDecoder.state.constructor as any);
        this.#ctorFromReflection = inputDecoder.state.constructor as new () => any;

        if (this.#room.clock === NULL_CLOCK) this.#room.clock = new RoomClockImpl();
    }

    /**
     * Decode the `INPUT_OPTIONS` handshake section.
     * `[flags uint8][tickRate varint?][patchRate varint?][subSteps varint?][lastAck varint?]`,
     * varints in bit order (the trailing `lastAck` appears only on a
     * reconnection handshake — see InputFlags.LAST_ACK). Advances `it`.
     */
    applyOptions(buffer: Uint8Array, it: Iterator): void {
        const flags = buffer[it.offset++];
        this.#stampRender = (flags & InputFlags.RENDER_TIME) !== 0;
        this.#stampReckon = (flags & InputFlags.RECKON_TIME) !== 0;
        if (flags & InputFlags.FIXED_TIMESTEP) this.#tickRate = decode.number(buffer as Buffer, it);
        if (flags & InputFlags.PATCH_RATE) this.#patchRate = decode.number(buffer as Buffer, it);
        if (flags & InputFlags.SUB_STEPS) this.#subSteps = decode.number(buffer as Buffer, it);
        // Capability bits: the sequenced reliable channel is server-driven.
        this.#reliableSequence = (flags & InputFlags.RELIABLE_SEQUENCE) !== 0;
        if (flags & InputFlags.LAST_ACK) {
            // Reconnection: the one negotiated value. Stored for the handle
            // whether it already exists or is created next.
            this.#lastAck = decode.number(buffer as Buffer, it) >>> 0;
            this.#handle?.adoptServerAck(this.#lastAck);
        } else {
            // A fresh (non-reconnect) handshake resets the negotiated point —
            // otherwise a LAST_ACK from an earlier reconnect would leak into a
            // handle created later on a fresh connection.
            this.#lastAck = 0;
        }
    }

    /** Feed the server's last-processed input seq to the handle; returns the RTT
     *  sample (or `-1` before the handle exists / when the seq aged out). */
    ackInput(inputSeq: number): number {
        return this.#handle ? this.#handle.ackInput(inputSeq) : -1;
    }

    /** Reset the input round-trip on reconnect (see {@link Room} reconnection).
     *  In sequenced reliable mode this is deliberately light: the client's seq
     *  numbering and replay ring survive the connection (the confirmation
     *  point is re-negotiated via LAST_ACK when the handshake arrives, which
     *  drives the ordered replay) — only the encoder's delta baseline is
     *  cleared. Legacy handles do the full counter restart. */
    reset(): void {
        this.#handle?.reset();
    }

    /** Whether the (possibly existing) handle runs the explicit-seq channel —
     *  lets {@link Room} decide its reconnect policy before the handle exists
     *  (a client that never called room.input() has none). */
    get sequenced(): boolean {
        return this.#reliableSequence;
    }

    /**
     * Lazily create and cache the per-room {@link InputHandle}; subsequent calls
     * return the same handle (options on later calls are ignored — a warning
     * fires once if they differ from the constructed handle's). Backs
     * `room.input(...)` — see that method for the full discovery/usage docs.
     */
    handle<I>(options?: InputOptions<I>): InputHandle<I> {
        if (this.#handle) {
            if (options !== undefined) this.#warnIfIgnored(options);
            return this.#handle as InputHandle<I>;
        }

        const Ctor = (options?.type ?? this.#ctorFromReflection) as (new () => I) | undefined;
        if (!Ctor) {
            throw new Error(
                "room.input(): no input schema available. The server room must call " +
                "`defineInput(YourInput)`, or you can pass `{ type: YourInput }` explicitly."
            );
        }

        const instance = new Ctor();
        // The InputEncoder always delta-encodes (no full-snapshot mode), so
        // there's nothing to configure here beyond mode/historySize.
        const encoder = new InputEncoder(instance as any, options);
        // The handle owns the input round-trip (send counter, RTT send-times, server-acked count);
        // the TIMED decode feeds it the ack and it produces RTT samples for the clock (see Room.onMessage).
        // `stampRender`/`stampReckon` are server-driven (INPUT_OPTIONS flags); `renderDelay` lets the app subtract its interp buffer.
        this.#handle = new InputHandleImpl(this.#room, instance, encoder, {
            stampRender: this.#stampRender,
            stampReckon: this.#stampReckon,
            renderDelay: options?.renderDelay,
            allowRewind: options?.allowRewind,   // app gate: skip the stamp on inputs the server won't rewind
            tickRate: this.#tickRate,
            patchRate: this.#patchRate,
            subSteps: this.#subSteps,
            // Opt-in per room; when on, a reconnection handshake may already
            // carry the negotiated last ack.
            reliableSequence: this.#reliableSequence,
            lastAck: this.#lastAck ?? 0,
        });
        this.#options = options;
        this.#encoder = encoder;
        return this.#handle as InputHandle<I>;
    }

    /** Warn (once) when a later `room.input(options)` call would have produced a
     *  different handle — those options are silently ignored (first call wins),
     *  which is invisible without this. Value fields compare against the RESOLVED
     *  config (ctor/mode/historySize), not the first call's raw options, so a
     *  textually identical second call stays silent. */
    #warnIfIgnored(later: InputOptions<any>): void {
        if (this.#warnedIgnoredOptions) return;
        const handle = this.#handle!;
        const diffs: string[] = [];
        if (later.type !== undefined && later.type !== (handle.data as any)?.constructor) diffs.push("type");
        if (later.mode !== undefined && later.mode !== handle.mode) diffs.push("mode");
        // historySize is inert in reliable mode (for BOTH calls) — only compare when unreliable.
        if (later.historySize !== undefined && handle.mode === "unreliable"
            && later.historySize !== this.#encoder!.historySize) diffs.push("historySize");
        if (later.renderDelay !== undefined && later.renderDelay !== this.#options?.renderDelay) diffs.push("renderDelay");
        // Function identity is meaningless across call sites — compare presence only.
        if ((later.allowRewind !== undefined) !== (this.#options?.allowRewind !== undefined)) diffs.push("allowRewind");
        if (diffs.length === 0) return;
        this.#warnedIgnoredOptions = true;
        console.warn(
            "@colyseus/sdk: room.input() options ignored — the input handle was already " +
            `created by an earlier call (first call wins). Differing: ${diffs.join(", ")}.`
        );
    }
}
