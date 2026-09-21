import assert from "assert";
import { before, beforeEach, describe, it } from "mocha";
import WebSocket from "ws";

import { boot, ColyseusTestServer } from "../src/index.ts";
import appConfig from "./app1/app.config.ts";
import { SequencedInputRoom, SeqState, SeqInput } from "./app1/SequencedInputRoom.ts";

// Node 20 has no global WebSocket (the SDK reconnect path news one up).
(globalThis as any).WebSocket ??= WebSocket;

// Protocol seam for raw, out-of-order frames (the SDK handle only sends in order).
import { Protocol, ProtocolModifier } from "@colyseus/shared-types";
import { encode } from "@colyseus/schema";
import { InputEncoder } from "@colyseus/schema/input";
import { JWT } from "@colyseus/auth";

JWT.settings.secret = "secret";

/**
 * Encode a SEQUENCED reliable input frame straight to the wire, with the
 * caller choosing seq and send order — used to inject a gap the server has to
 * park/expire, and a duplicate it must not double-apply.
 */
function rawSequencedFrame(seq: number, x: number, fire: boolean): Uint8Array {
  const input = new SeqInput();
  (input as any).seq = seq;
  (input as any).x = x;
  (input as any).fire = fire;
  // First (and only) encode on a fresh encoder is a full snapshot.
  const encoder = new InputEncoder(input as any, { mode: "reliable" });
  const body = encoder.encode();

  const frame = new Uint8Array(1 + 9 + body.length);
  frame[0] = Protocol.ROOM_INPUT_RELIABLE | ProtocolModifier.SEQUENCED;
  const it = { offset: 1 };
  encode.number(frame, seq, it);
  frame.set(body, it.offset);
  return frame.subarray(0, it.offset + body.length);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("sequenced reliable input (E2E)", () => {
  let colyseus: ColyseusTestServer<typeof appConfig>;

  before(async () => { colyseus = await boot(appConfig); });
  after(async () => colyseus.shutdown());
  beforeEach(async () => { await colyseus.cleanup(); });

  it("applies in-order inputs and echoes the confirmed seq", async () => {
    const room = await colyseus.createRoom<SeqState>("sequenced_input", {});
    const client = await colyseus.connectTo(room);

    const handle = client.input({ type: SeqInput } as any);
    assert.strictEqual(handle.sequenced, true, "handshake enabled the ordered channel");

    (handle.data as any).seq = 1; (handle.data as any).x = 10; handle.send();
    (handle.data as any).seq = 2; (handle.data as any).x = 20; handle.send();
    (handle.data as any).seq = 3; (handle.data as any).x = 30; handle.send();

    // 20 Hz step; patchRate is 1000ms — wait for ≥3 steps AND a TIMED patch
    // carrying the per-recipient consumed-seq ack.
    await sleep(1200);
    const player = room.state.players.get(client.sessionId)!;
    assert.strictEqual(player.x, 30);
    assert.strictEqual(player.lastConsumedSeq, 3);
    // The TIMED state prefix echoed the consumed app seq.
    assert.strictEqual(handle.lastProcessed, 3);

    client.leave();
  });

  it("does not double-apply a duplicated side-effectful input", async () => {
    const room = await colyseus.createRoom<SeqState>("sequenced_input", {});
    const client = await colyseus.connectTo(room);

    // Same seq/body delivered twice (transport redelivery shape).
    client.connection.send(rawSequencedFrame(1, 5, true));
    client.connection.send(rawSequencedFrame(1, 5, true));
    client.connection.send(rawSequencedFrame(2, 6, false));

    await sleep(300);
    const player = room.state.players.get(client.sessionId)!;
    assert.strictEqual(player.fires, 1, "the fire input applied exactly once");
    assert.strictEqual(player.lastConsumedSeq, 2);

    client.leave();
  });

  it("parks behind a gap, then applies in order when the hole fills", async () => {
    const room = await colyseus.createRoom<SeqState>("sequenced_input", {});
    const client = await colyseus.connectTo(room);

    // seq 1 applied; 3 arrives before 2 (reordered at the app layer); the
    // server must hold 3 and keep the sim on seq 1 meanwhile.
    client.connection.send(rawSequencedFrame(1, 1, false));
    client.connection.send(rawSequencedFrame(3, 30, true));
    await sleep(150);
    const player1 = room.state.players.get(client.sessionId)!;
    assert.strictEqual(player1.lastConsumedSeq, 1, "seq 3 is parked behind missing seq 2");
    assert.strictEqual(player1.x, 1);

    // Hole fills — the parked frame applies in order.
    client.connection.send(rawSequencedFrame(2, 20, false));
    await sleep(200);
    const player2 = room.state.players.get(client.sessionId)!;
    assert.strictEqual(player2.lastConsumedSeq, 3);
    assert.strictEqual(player2.x, 30);
    assert.strictEqual(player2.fires, 1);

    client.leave();
  });

  it("skips an expired gap after maxGapAgeMs instead of waiting forever", async () => {
    const room = await colyseus.createRoom<SequencedInputRoom>("sequenced_input", {});
    const client = await colyseus.connectTo(room);

    // seq 1 applied; seq 2 is never sent; seq 3 parks and must age through.
    client.connection.send(rawSequencedFrame(1, 1, false));
    client.connection.send(rawSequencedFrame(3, 30, true));
    await sleep(150);
    assert.strictEqual(
      room.state.players.get(client.sessionId)!.lastConsumedSeq, 1,
      "not expired yet",
    );

    // maxGapAgeMs = 200; the fixed-step sweep skips seq 2 and releases seq 3.
    await sleep(400);
    const player = room.state.players.get(client.sessionId)!;
    assert.strictEqual(player.lastConsumedSeq, 3, "ack jumped over the lost seq");
    assert.strictEqual(player.x, 30);
    assert.strictEqual(player.fires, 1);

    client.leave();
  });

  it("reconnect: negotiates the last ack point and replays unacked inputs in order", async () => {
    const room = await colyseus.createRoom<SeqState>("sequenced_input", {});
    const client = await colyseus.connectTo(room);

    // Fast, immediate auto-reconnect.
    client.reconnection.minUptime = 0;
    client.reconnection.minDelay = 0;
    client.reconnection.maxDelay = 0;
    client.reconnection.backoff = () => 0;

    const handle = client.input({ type: SeqInput } as any);

    // seq 1..4 sent; give the 20 Hz loop time to consume 1..2, then drop.
    for (let s = 1; s <= 4; s++) {
      (handle.data as any).seq = s;
      (handle.data as any).x = s * 10;
      (handle.data as any).fire = s === 4;
      handle.send();
    }
    await sleep(200);
    const sid = client.sessionId;
    const consumedBefore = room.state.players.get(sid)!.lastConsumedSeq;
    assert.ok(consumedBefore >= 2 && consumedBefore <= 4,
      `expected some consumption before drop, got ${consumedBefore}`);

    // Kill the transport; the SDK auto-reconnects with the reconnection token.
    client.connection.close();

    // Wait for onReconnect + the replayed (and any remaining) inputs to apply.
    await new Promise<void>((resolve) => {
      const done = () => resolve();
      client.onReconnect.once(done);
      setTimeout(done, 2000); // safety — assertion below fails if reconnect never fires
    });
    await sleep(400);

    const player = room.state.players.get(sid)!;
    // Exactly one side-effectful input (seq 4) ever applied — replay deduped.
    assert.strictEqual(player.fires, 1, "replayed inputs do not double-apply");
    assert.strictEqual(player.x, 40, "replayed stream resumes at seq 4");
    assert.strictEqual(player.lastConsumedSeq, 4);
    assert.strictEqual(handle.lastProcessed, 4);

    // Post-reconnect live sends keep sequencing on the SAME monotonic space.
    (handle.data as any).seq = 5;
    (handle.data as any).x = 50;
    (handle.data as any).fire = false;
    handle.send();
    await sleep(1200);
    assert.strictEqual(room.state.players.get(sid)!.x, 50);
    assert.strictEqual(room.state.players.get(sid)!.lastConsumedSeq, 5);

    client.leave();
  });
});
