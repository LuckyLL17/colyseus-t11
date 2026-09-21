import type { InputBufferImpl } from './InputBuffer.ts';

/**
 * Per-session gate for the explicit-seq reliable input channel
 * (`defineInput({ reliableSequence: true })`). Sits in front of the
 * {@link InputBufferImpl}: a decoded reliable frame carrying the
 * SEQUENCED modifier is offered here FIRST, and only what the policy admits
 * reaches the buffer the fixed-timestep loop consumes. The sim therefore can
 * never see a duplicate or an out-of-order input — "只消费已经被策略允许的输入".
 *
 * State kept per session:
 * - {@link baseSeq} — the last ORDERED seq (the confirmation point). Every seq
 *   `<= baseSeq` has either been applied (pushed into the buffer and consumed
 *   later by the sim) or explicitly SKIPPED (an expired gap); it will never be
 *   admitted twice. This is the single value the two sides negotiate on
 *   reconnect.
 * - a bounded receive WINDOW over the seq space directly ahead of
 *   {@link baseSeq}: out-of-order arrivals are parked until their predecessor
 *   shows up; a frame landing past the window's far edge means the missing gap
 *   is older than anything we are willing to remember — those seqs are expired
 *   (skipped, never applied) and the frame is admitted in their place instead
 *   of blocking forever.
 *
 * Seqs live in a uint32 wrap space. All comparisons go through
 * {@link seqDistance}, a signed modular delta, so freshness is correct across
 * the 2^32 boundary.
 *
 * @internal
 */
export class ReliableSequencer {
  /** Default receive-window depth (in seqs) when the room doesn't name one —
   *  the same order of magnitude as the default input buffer: generous enough
   *  to absorb realistic reliable-channel reordering, bounded so a client can
   *  never pin unbounded server memory. */
  static readonly DEFAULT_WINDOW = 32;

  /** Last ORDERED seq — the confirmation point (see class docs). `0` on a fresh
   *  join; restored to the frozen session's last value on reconnect. A seq is
   *  "ordered" when every seq up to it has been either admitted or skipped. */
  baseSeq: number;

  /** Receive-window depth. A frame more than this many seqs ahead of
   *  {@link baseSeq} expires the gap in between. */
  private readonly window: number;

  /** Parked out-of-order frames: seq → decoded snapshot, seqs strictly greater
   *  than {@link baseSeq} and inside the window. A plain Map keyed by the
   *  unwrapped seq — bounded by `window` entries by construction. */
  private pending: Map<number, IEntry> = new Map();

  constructor(windowSize: number = ReliableSequencer.DEFAULT_WINDOW, baseSeq: number = 0) {
    this.window = Math.max(1, Math.floor(windowSize));
    this.baseSeq = baseSeq >>> 0;
  }

  /**
   * Offer one decoded frame with its EXPLICIT seq. Returns the admissions the
   * policy allows — an array handed straight to the input buffer in order:
   *
   * - `[]` — duplicate (`seq` already ordered), stale (behind a later window),
   *   or a re-park of a seq already waiting: the frame had NO side effect and
   *   MUST NOT be applied. Duplicates are detected BEFORE the caller decodes
   *   body side effects — see {@link isDuplicate}.
   * - one or more `{ seq, snapshot, renderTime, reckonTime }` — a contiguous
   *   run oldest → newest. A simple in-order delivery yields `[frame]`; a frame
   *   that closes (or expires past) a gap yields the whole run it releases.
   *   Entries with `snapshot === undefined` mark SKIPPED seqs (expired gaps):
   *   the buffer advances its ack over them but the sim applies nothing for
   *   them — "不能无限等待已经过期的序列".
   */
  admit(seqInput: number, entry: Omit<IEntry, 'snapshot'> & { snapshot: any }): IAdmission[] {
    const seq = seqInput >>> 0;
    // At or behind the confirmation point → already applied or already
    // expired. Never re-admit: this is the duplicate/side-effect guard.
    const d = seqDistance(seq, this.baseSeq);
    if (d <= 0) { return NO_ADMISSIONS; }

    // Already parked (a reliable redelivery of an out-of-order frame) → the
    // first copy wins; don't overwrite, don't re-admit.
    if (this.pending.has(seq)) { return NO_ADMISSIONS; }

    // Past the far edge of the receive window → expire the gap between
    // baseSeq and this frame. Waiting any longer would bound neither latency
    // nor memory. The frame is admitted only as far inside the window as
    // allowed: seqs in `(baseSeq, seq-window]` are SKIPPED; the frame and
    // anything parked above the cutoff stay in the window.
    const gap = d - 1;
    if (gap > this.window) {
      const lastSkipped = (seq - this.window) >>> 0;
      // A parked frame inside the expired range can never be delivered now
      // (its predecessor is gone) — discard it.
      for (const s of this.pending.keys()) {
        if (seqDistance(s, lastSkipped) <= 0) { this.pending.delete(s); }
      }
      const out: IAdmission[] = [];
      for (let s = (this.baseSeq + 1) >>> 0; seqDistance(s, lastSkipped) <= 0; s = (s + 1) >>> 0) {
        out.push({ seq: s, snapshot: undefined, renderTime: 0, reckonTime: 0 });
        this.baseSeq = s;
      }
      // Fall through: park THIS frame, then drain the now-contiguous head.
      return this.parkAndDrain(seq, entry, out);
    }

    return this.parkAndDrain(seq, entry, undefined);
  }

  /** Park `entry` at `seq`, then append the contiguous run it releases
   *  (oldest → newest) to `prefix`. A private helper shared by the plain path
   *  and the gap-expiry path. */
  private parkAndDrain(seq: number, entry: Omit<IEntry, 'snapshot'> & { snapshot: any }, prefix?: IAdmission[]): IAdmission[] {
    this.pending.set(seq, {
      snapshot: entry.snapshot,
      renderTime: entry.renderTime,
      reckonTime: entry.reckonTime,
    });
    const run = this.drain();
    if (prefix === undefined || prefix.length === 0) { return run; }
    return prefix.concat(run);
  }

  /**
   * Cheap pre-decode duplicate/stale probe — `true` when a frame carrying
   * `seq` cannot possibly be admitted (already ordered, or its slot is
   * already parked). The decode path calls this BEFORE decoding the body into
   * the shared `client._input` instance, so a resent packet can't even
   * overwrite `latest` — the strongest form of the "no duplicate side effect"
   * guarantee.
   */
  isDuplicate(seqInput: number): boolean {
    const seq = seqInput >>> 0;
    return seqDistance(seq, this.baseSeq) <= 0 || this.pending.has(seq);
  }

  /** Remove every parked frame and SHRINK the window to the given base (a gap
   *  that will never be filled — disconnect/reconnect, room freeze). Keeps the
   *  confirmation point (the ack survives) but drops unordered waits. */
  reset(baseSeq: number = this.baseSeq): void {
    this.baseSeq = baseSeq >>> 0;
    this.pending.clear();
  }

  /** Number of out-of-order frames currently parked (tests/diagnostics). */
  get pendingSize(): number { return this.pending.size; }

  /** Drain the contiguous head of {@link pending}: pop baseSeq+1, +2, … while
   *  present (each becomes ordered), and return them oldest → newest. Stops at
   *  the first hole. */
  private drain(): IAdmission[] {
    let next = (this.baseSeq + 1) >>> 0;
    let parked = this.pending.get(next);
    if (parked === undefined) { return NO_ADMISSIONS; }
    const out: IAdmission[] = [];
    while (parked !== undefined) {
      out.push({ seq: next, snapshot: parked.snapshot, renderTime: parked.renderTime, reckonTime: parked.reckonTime });
      this.pending.delete(next);
      this.baseSeq = next;
      next = (next + 1) >>> 0;
      parked = this.pending.get(next);
    }
    return out;
  }
}

/** A parked or admitted decoded frame. `snapshot` is a CLONE owned by the
 *  buffer path for real inputs; `undefined` marks a skipped (expired) seq. */
export interface IEntry {
  snapshot: any;
  renderTime: number;
  reckonTime: number;
}

/** One ordered delivery from {@link ReliableSequencer.admit}. `snapshot`
 *  `undefined` ⇒ this seq was skipped (expired gap) — advance the ack, apply
 *  nothing. */
export interface IAdmission {
  seq: number;
  snapshot: any;
  renderTime: number;
  reckonTime: number;
}

const NO_ADMISSIONS: IAdmission[] = [];

/** Signed distance `(from → to)` in the uint32 seq space, interpreted as a
 *  32-bit modular delta: positive ⇒ `to` is AHEAD of `from` (newer), negative
 *  ⇒ behind (older/duplicate), 0 ⇒ equal. Correct across the 2^32 wrap — the
 *  seq space is treated as a circle and only deltas smaller than 2^31 in
 *  magnitude are meaningful (a window never spans half the space). */
export function seqDistance(to: number, from: number): number {
  return (((to - from) >>> 0) << 0); // reinterpret uint32 subtraction as int32
}
