import { describe, expect, it } from "vitest";
import { InputBufferImpl, type OrderedPayload } from "../src/input/InputBuffer.ts";
import { OrderedWindow } from "../src/input/OrderedWindow.ts";

/** Minimal input shape used through the ordered queue. */
interface Move {
  vx: number;
  fire?: boolean;
}

function makeBuf(opts?: { windowSize?: number; maxGapAgeMs?: number; now?: () => number }) {
  const window = new OrderedWindow(
    opts?.windowSize ?? 16,
    opts?.maxGapAgeMs ?? 1000,
    opts?.now ?? (() => 0),
  );
  // ctor undefined → no idle synthesis; window is what defines ordered mode.
  const buf = new InputBufferImpl<Move>(32, undefined, undefined, undefined, undefined, window);
  return { buf, window };
}

/** The ordered channel parks {input, stamps} payloads (RoomInput.capture builds these). */
function payload(vx: number, fire = false): OrderedPayload<Move> {
  return { input: { vx, fire }, renderTime: 0, reckonTime: 0 };
}

describe("InputBufferImpl ordered channel", () => {
  it("queues only released frames, in seq order — a parked hole stays invisible", () => {
    const { buf } = makeBuf();

    expect(buf.pushOrdered(3, payload(30))).toBe("park");
    expect(buf.pushOrdered(2, payload(20))).toBe("park");
    expect(buf.size).toBe(0); // the sim must not see anything through the gap

    expect(buf.pushOrdered(1, payload(10))).toBe("release");
    expect(buf.drain().map((m) => m.vx)).toEqual([10, 20, 30]);
  });

  it("never queues a duplicated side-effectful input twice", () => {
    const { buf } = makeBuf();

    buf.pushOrdered(1, payload(1, true));   // fire
    expect(buf.pushOrdered(1, payload(1, true))).toBe("duplicate"); // redelivered
    const applied = buf.drain();
    expect(applied).toHaveLength(1);
    expect(applied[0]!.fire).toBe(true);
  });

  it("advances the ack (ackSeq) only over inputs the sim consumed", () => {
    const { buf } = makeBuf();

    buf.pushOrdered(1, payload(10));
    buf.pushOrdered(2, payload(20));
    buf.pushOrdered(4, payload(40)); // parks: 3 missing
    expect(buf.ackSeq).toBe(0);

    const first = buf.next();
    expect(first!.vx).toBe(10);
    expect(buf.ackSeq).toBe(1);

    expect(buf.next()!.vx).toBe(20);
    expect(buf.ackSeq).toBe(2);
    expect(buf.size).toBe(0); // seq 4 still parked behind missing 3
  });

  it("releases an expired gap into the queue on releaseExpired (sweep hook)", () => {
    let t = 0;
    const { buf } = makeBuf({ maxGapAgeMs: 100, now: () => t });

    buf.pushOrdered(1, payload(10));
    buf.next(); // consumed 1
    // Seq 3 parks (2 lost); its age runs from arrival.
    buf.pushOrdered(3, payload(30));
    t = 50;
    buf.releaseExpired(t);
    expect(buf.size).toBe(0); // not due yet

    t = 101;
    buf.releaseExpired(t); // gap (seq 2) expires → 3 becomes eligible
    expect(buf.next()!.vx).toBe(30);
    expect(buf.ackSeq).toBe(3); // ack jumps over the lost seq
  });

  it("ordered freeze (clear) does NOT count unconsumed inputs as acked", () => {
    const { buf, window } = makeBuf();

    buf.pushOrdered(1, payload(10));
    buf.pushOrdered(2, payload(20));
    buf.next(); // sim consumed seq 1 only
    // Seq 4 parks behind a hole (3 missing).
    buf.pushOrdered(4, payload(40));

    // Seat drops: queued seq 2 and parked seq 4 are NOT settled.
    buf.clear();
    expect(buf.size).toBe(0);
    expect(buf.ackSeq).toBe(1); // negotiation point = consumed, not newest-seen
    expect(window.confirmedSeq).toBe(1);
    expect(window.consumedSeq).toBe(1);

    // Reconnect replay: 2 re-sent is releasable now; 4 parks again (the fresh
    // server buffer never simulated it).
    expect(buf.pushOrdered(2, payload(20))).toBe("release");
    expect(buf.pushOrdered(4, payload(40))).toBe("park");
    expect(buf.drain().map((m) => m.vx)).toEqual([20]);
  });

  it("overflow of the sim queue settles the oldest seq (finite buffer, no stall)", () => {
    const { buf } = makeBuf({ windowSize: 128 });
    // Feed 40 released seqs into a 32-capacity sim queue without draining.
    for (let seq = 1; seq <= 40; seq++) {
      buf.pushOrdered(seq, payload(seq * 10));
    }
    // 8 oldest dropped → ack moved past them; the buffer holds the newest 32.
    expect(buf.size).toBe(32);
    expect(buf.ackSeq).toBe(8);
    const remaining = buf.drain().map((m) => m.vx);
    expect(remaining[0]).toBe(90);
    expect(remaining[remaining.length - 1]).toBe(400);
  });
});
