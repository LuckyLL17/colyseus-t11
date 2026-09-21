/**
 * Bounded, per-session ordering window for the OPT-IN sequenced reliable
 * input channel (`defineInput({ reliable: { windowSize, maxGapAgeMs } })`,
 * advertised via `InputFlags.SEQUENCED`).
 *
 * Contract:
 * - Seq is a client-owned monotonic uint32 counter, one per reliable input
 *   (first seq = 1).
 * - A frame is RELEASED (eligible for the fixed-step sim) only once it is the
 *   immediate successor of the last confirmed seq — or becomes one when the
 *   hole ahead of it fills or expires. Out-of-order arrivals are PARKED, so
 *   the sim sees ordered inputs only.
 * - A seq at or below the confirmed frontier (or parked already) is a
 *   DUPLICATE — never released twice. A redelivered "fire"/"spend" input
 *   cannot double-apply its side effect.
 * - A hole does NOT stall the stream forever: when its oldest parked
 *   successor has waited longer than `maxGapAgeMs` (checked per sim tick via
 *   {@link sweep}) the missing seqs are declared LOST and skipped; a frame
 *   arriving beyond {@link windowSize} forces the intervening gap to skip
 *   immediately. Parked REAL frames inside the skipped span are preserved —
 *   only missing seqs are jumped over.
 *
 * Pure logic — no schema/IO. {@link RoomInput} binds one instance per
 * (reconnectable) session to that session's `InputBufferImpl`.
 *
 * @internal
 */

/** One released frame, in the order the sim must apply it. */
export interface ReleasedInput<I = unknown> {
  seq: number;
  value: I;
}

export type AdmitResult<I = unknown> =
  /** The frame (and every frame in `chain`) became releasable in order. */
  | { kind: "release"; seq: number; chain: ReleasedInput<I>[] }
  /** A future seq parked to await its predecessors. */
  | { kind: "park"; seq: number }
  /** Redelivery of a confirmed or already-parked seq — never re-apply. */
  | { kind: "duplicate"; seq: number }
  /** Beyond the window: the gap was force-skipped; `chain` is releasable. */
  | { kind: "expired"; seq: number; chain: ReleasedInput<I>[] };

interface ParkedEntry {
  value: unknown;
  at: number;
}

export class OrderedWindow {
  /** Release frontier: highest seq confirmed releasable in order. The sim is
   *  allowed to see through this point and no further. */
  private _confirmed = 0;

  /** Consumption frontier: highest seq the sim actually consumed (the
   *  reconnection negotiation point). Always ≤ {@link _confirmed}. */
  private _consumed = 0;

  /** Parked out-of-order successors. Key order is arbitrary (scanned, not
   *  iterated) — window sizes stay small. */
  private _parked = new Map<number, ParkedEntry>();

  private readonly _windowSize: number;
  private readonly _maxGapAgeMs: number;
  private readonly _now: () => number;

  constructor(
    windowSize: number,
    maxGapAgeMs: number,
    now: () => number = () => performance.now(),
  ) {
    /** Max distance (in seqs) a frame may sit ahead of the frontier before its
     *  arrival force-expires the intervening gap. */
    this._windowSize = windowSize;
    /** Max ms the sim waits for the missing predecessors of the oldest parked
     *  frame before the gap is declared lost. */
    this._maxGapAgeMs = maxGapAgeMs;
    /** Clock seam (production: performance.now; tests inject their own). */
    this._now = now;
  }

  /** Release frontier — everything through here has been handed to the sim in
   *  order (or skipped as a lost gap). */
  get confirmedSeq(): number { return this._confirmed; }

  /** Consumption frontier — the ack negotiated on reconnect. */
  get consumedSeq(): number { return this._consumed; }

  /** Depth of the current hole (parked successors awaiting predecessors). */
  get parkedSize(): number { return this._parked.size; }

  /** The sim reports how far it consumed (monotonic). */
  markConsumed(seq: number): void {
    if (seq > this._consumed) { this._consumed = seq; }
  }

  /** Smallest parked seq, or +Infinity when nothing is parked. */
  private smallestParked(): number {
    let smallest = Infinity;
    for (const k of this._parked.keys()) {
      if (k < smallest) { smallest = k; }
    }
    return smallest;
  }

  /**
   * Admit one decoded frame. See {@link AdmitResult}.
   */
  admit(value: unknown, seq: number, now: number = this._now()): AdmitResult {
    if (seq <= this._confirmed || this._parked.has(seq)) {
      return { kind: "duplicate", seq };
    }

    this._parked.set(seq, { value, at: now });

    if (seq > this._confirmed + this._windowSize) {
      // Pressure: jump the frontier over the gap so this frame fits. Every
      // PARKED frame up through `seq` rides out in order (real inputs the
      // server holds are never thrown away); only missing seqs are skipped.
      const chain = this.releaseParkedThrough(seq);
      return { kind: "expired", seq, chain };
    }

    if (seq !== this._confirmed + 1) {
      return { kind: "park", seq };
    }

    // Head filled — release it and every contiguous parked successor.
    return { kind: "release", seq, chain: this.drainContiguous() };
  }

  /** Pop the contiguous parked run starting at the frontier's successor and
   *  advance the frontier to its tail. Missing seqs (holes) stop the run. */
  private drainContiguous(): ReleasedInput[] {
    const chain: ReleasedInput[] = [];
    let next = this._confirmed + 1;
    let entry = this._parked.get(next);
    while (entry !== undefined) {
      this._parked.delete(next);
      chain.push({ seq: next, value: entry.value });
      this._confirmed = next;
      entry = this._parked.get(++next);
    }
    return chain;
  }

  /** Force the release frontier through {@link target}: parked frames in
   *  `(confirmed, target]` come out IN ORDER, missing seqs are skipped as
   *  lost, then any contiguous parked run beyond `target` drains as well. */
  private releaseParkedThrough(target: number): ReleasedInput[] {
    const chain: ReleasedInput[] = [];
    while (this._confirmed < target) {
      const next = this._confirmed + 1;
      const entry = this._parked.get(next);
      if (entry === undefined) {
        // Missing seq — declare it lost and jump over.
        this._confirmed = next;
      } else {
        this._parked.delete(next);
        chain.push({ seq: next, value: entry.value });
        this._confirmed = next;
      }
    }
    // `target` itself (and any contiguous successor) is parked — drain the run.
    chain.push(...this.drainContiguous());
    return chain;
  }

  /**
   * Age out holes — call once per sim tick. If the oldest parked successor's
   * predecessors have been missing longer than `maxGapAgeMs`, those seqs are
   * declared lost and the now-adjacent run (plus any further contiguous
   * parked run) is released. Repeats while another aged gap heads the queue,
   * so one tick fully resolves everything due. Returns released frames in
   * apply order; `[]` when nothing was due.
   */
  sweep(now: number = this._now()): ReleasedInput[] {
    let out: ReleasedInput[] | undefined;
    while (this._parked.size > 0) {
      const smallest = this.smallestParked();
      if (smallest === this._confirmed + 1) {
        // Adjacent with no hole — drain (defensive; admit normally does this).
        const run = this.drainContiguous();
        (out ??= []).push(...run);
        continue;
      }
      const age = now - this._parked.get(smallest)!.at;
      if (age < this._maxGapAgeMs) { break; }
      const run = this.releaseParkedThrough(smallest);
      (out ??= []).push(...run);
    }
    return out ?? [];
  }

  /**
   * Freeze a dropping seat for its reconnection window: discard parked
   * successors (never simulated — the client re-sends them after reconnect)
   * and collapse the RELEASE frontier to the consumption frontier. The
   * consumption frontier itself is RETAINED: it is the ack point negotiated
   * with the client in the reconnect handshake (everything ≤ it is settled,
   * even though the live input buffer was just cleared).
   */
  reset(): void {
    this._parked.clear();
    this._confirmed = this._consumed;
  }

  /** Full leave — forget every frontier. */
  dispose(): void {
    this._parked.clear();
    this._confirmed = 0;
    this._consumed = 0;
  }
}
