import { describe, expect, it } from "vitest";
import { OrderedWindow } from "../src/input/OrderedWindow.ts";

/** Injectable monotonic clock for deterministic gap-age sweeps. */
function fakeClock(start = 0) {
  let t = start;
  return {
    now: () => t,
    advance: (ms: number) => { t += ms; },
  };
}

describe("OrderedWindow", () => {
  it("releases the immediate successor and chains contiguous parked frames", () => {
    const clock = fakeClock();
    const w = new OrderedWindow(8, 1000, clock.now);

    // Out of order: 3 parks, 2 parks, 1 fills the hole → 1,2,3 release together.
    expect(w.admit("c", 3).kind).toBe("park");
    expect(w.admit("b", 2).kind).toBe("park");
    const r = w.admit("a", 1);
    expect(r.kind).toBe("release");
    expect(r.chain.map((x) => x.seq)).toEqual([1, 2, 3]);
    expect(r.chain.map((x) => x.value)).toEqual(["a", "b", "c"]);
    expect(w.confirmedSeq).toBe(3);
  });

  it("drops redeliveries of confirmed and parked seqs (no double application)", () => {
    const clock = fakeClock();
    const w = new OrderedWindow(8, 1000, clock.now);

    expect(w.admit("a", 1).kind).toBe("release");
    // Redelivery after release — duplicate, must not enter the chain again.
    expect(w.admit("a-redelivered", 1).kind).toBe("duplicate");

    expect(w.admit("c", 3).kind).toBe("park");
    // A second copy of the same FUTURE frame is also a duplicate.
    expect(w.admit("c-again", 3).kind).toBe("duplicate");
    expect(w.parkedSize).toBe(1);

    // Filling the hole releases the parked frame exactly once.
    const r = w.admit("b", 2);
    expect(r.kind).toBe("release");
    expect(r.chain.map((x) => x.seq)).toEqual([2, 3]);
    expect(w.confirmedSeq).toBe(3);
  });

  it("ages out a gap after maxGapAgeMs, releasing the parked run in order", () => {
    const clock = fakeClock();
    const w = new OrderedWindow(64, 1000, clock.now);

    // Seq 2 arrives; seq 1 is missing.
    expect(w.admit("b", 2).kind).toBe("park");
    expect(w.sweep()).toEqual([]);           // not aged yet
    clock.advance(999);
    expect(w.sweep()).toEqual([]);           // still inside the budget
    clock.advance(2);
    const released = w.sweep();              // 1001 ms old → seq 1 declared lost
    expect(released.map((x) => x.seq)).toEqual([2]);
    expect(released[0]!.value).toBe("b");
    expect(w.confirmedSeq).toBe(2);
  });

  it("aged expiry skips missing seqs but preserves every parked real frame", () => {
    const clock = fakeClock();
    const w = new OrderedWindow(64, 100, clock.now);

    // 1 arrives+releases; 2 lost; 3,4 parked; 5 later arrives (2 still missing).
    expect(w.admit("a", 1).kind).toBe("release");
    expect(w.admit("c", 3).kind).toBe("park");
    clock.advance(10);
    expect(w.admit("d", 4).kind).toBe("park");
    clock.advance(100);
    // Seq 5 arriving now should NOT expire (within window); it parks behind the hole.
    expect(w.admit("e", 5).kind).toBe("park");
    const released = w.sweep();
    // Seq 2 skipped (lost); 3,4,5 ride out in order — none of the held inputs is dropped.
    expect(released.map((x) => [x.seq, x.value])).toEqual([
      [3, "c"], [4, "d"], [5, "e"],
    ]);
    expect(w.confirmedSeq).toBe(5);
  });

  it("force-expires an over-window gap instead of growing the park unbounded", () => {
    const clock = fakeClock();
    const w = new OrderedWindow(4, 100_000, clock.now); // age never triggers here

    // 1 released, then seq 7 jumps 5 past the frontier (window = 4).
    expect(w.admit("a", 1).kind).toBe("release");
    const r = w.admit("g", 7);
    expect(r.kind).toBe("expired");
    // 2..6 declared lost; 7 released — window can't be pushed indefinitely.
    expect(r.chain.map((x) => x.seq)).toEqual([7]);
    expect(w.confirmedSeq).toBe(7);
  });

  it("window pressure preserves parked frames inside the skipped span", () => {
    const clock = fakeClock();
    const w = new OrderedWindow(4, 100_000, clock.now);

    expect(w.admit("a", 1).kind).toBe("release");
    // 2 lost, 3 parked — both within window.
    expect(w.admit("c", 3).kind).toBe("park");
    // Seq 6 arrives: gap 2 missing, but 3 is a real frame the server holds.
    // 4..5 (missing) get skipped; 3 rides out before 6.
    const r = w.admit("f", 6);
    expect(r.kind).toBe("expired");
    expect(r.chain.map((x) => [x.seq, x.value])).toEqual([
      [3, "c"], [6, "f"],
    ]);
    expect(w.confirmedSeq).toBe(6);
  });

  it("tracks the consumption frontier and collapses to it on reset (freeze)", () => {
    const clock = fakeClock();
    const w = new OrderedWindow(8, 1000, clock.now);

    w.admit("a", 1); w.admit("b", 2); w.admit("c", 3);
    expect(w.confirmedSeq).toBe(3);
    // Sim consumed only through seq 2 (released but not yet drained).
    w.markConsumed(2);
    expect(w.consumedSeq).toBe(2);

    // Freeze for reconnect: parked 4 dropped, frontier collapses to consumed.
    w.admit("d", 5);
    w.reset();
    expect(w.confirmedSeq).toBe(2);
    expect(w.consumedSeq).toBe(2);
    expect(w.parkedSize).toBe(0);

    // Client replay: seq 3 re-sent — releasable; seq ≤2 is a duplicate.
    expect(w.admit("b-again", 2).kind).toBe("duplicate");
    const r = w.admit("c-replay", 3);
    expect(r.kind).toBe("release");
    expect(r.chain.map((x) => x.seq)).toEqual([3]);
  });

  it("sweep releasing an adjacent-but-not-drained run is defensive", () => {
    const clock = fakeClock();
    const w = new OrderedWindow(8, 50, clock.now);
    // Nothing parked → no-op.
    expect(w.sweep()).toEqual([]);
  });
});
