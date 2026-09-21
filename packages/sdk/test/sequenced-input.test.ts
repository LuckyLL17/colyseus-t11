import './util';
import { describe, test } from 'vitest';
import { assert } from 'chai';

import { Protocol, ProtocolModifier, InputFlags, HandshakeSection } from '@colyseus/shared-types';
import { decode } from '@colyseus/schema';
import { InputEncoder, InputDecoder } from '@colyseus/schema/input';
import { schema, t, type SchemaType } from '@colyseus/schema';

import { InputHandleImpl, type InputHandleHost } from '../src/input/InputHandle.ts';
import { RoomInput } from '../src/input/RoomInput.ts';

const MoveInput = schema({
    x: t.number().default(0),
    fire: t.boolean().default(false),
});
type MoveInput = SchemaType<typeof MoveInput>;

interface MockConn {
    isOpen: boolean;
    reliable: Uint8Array[];
}

function mockHost(): { host: InputHandleHost; conn: MockConn } {
    const conn: MockConn = { isOpen: true, reliable: [] };
    const host: InputHandleHost = {
        connection: {
            get isOpen() { return conn.isOpen; },
            send(d: Uint8Array) { conn.reliable.push(Uint8Array.from(d)); },
            sendUnreliable() {},
        } as any,
    };
    return { host, conn };
}

function makeHandle(opts?: { sequenced?: boolean; receiveWindow?: number }) {
    const { host, conn } = mockHost();
    const instance = new MoveInput();
    const encoder = new InputEncoder(instance as any, { mode: 'reliable' });
    const handle = new InputHandleImpl(host, instance, encoder, {
        sequenced: opts?.sequenced,
        receiveWindow: opts?.receiveWindow,
    });
    return { handle, instance, conn };
}

/** Parse a SEQUENCED reliable frame the way the server does: byte 0, varint seq, body. */
function readSequencedFrame(bytes: Uint8Array): { code: number; modifiers: number; seq: number; body: Uint8Array } {
    assert.equal(bytes[0] & 0x1f, Protocol.ROOM_INPUT_RELIABLE, 'reliable opcode');
    const it = { offset: 1 };
    const seq = decode.number(bytes as Buffer, it);
    return { code: bytes[0]! & 0x1f, modifiers: bytes[0]! & 0xe0, seq, body: bytes.subarray(it.offset) };
}

describe('sequenced reliable input channel (client)', () => {
    test('legacy room: frames carry NO SEQUENCED bit and count seqs implicitly', () => {
        const { handle, instance, conn } = makeHandle(); // sequenced omitted
        assert.isFalse(handle.sequenced);

        instance.x = 1; handle.send();
        instance.x = 2; handle.send();

        for (const frame of conn.reliable) {
            assert.equal(frame[0] & ProtocolModifier.SEQUENCED, 0, 'no SEQUENCED modifier');
        }
        assert.equal(handle.sentCount, 2);
    });

    test('sequenced room: every reliable frame carries [SEQUENCED][varint seq][body]', () => {
        const { handle, instance, conn } = makeHandle({ sequenced: true, receiveWindow: 64 });
        assert.isTrue(handle.sequenced);

        instance.x = 10; instance.fire = true;
        const s1 = handle.send();
        instance.fire = false; instance.x = 20;
        const s2 = handle.send();

        assert.equal(s1, 1);
        assert.equal(s2, 2);
        assert.equal(conn.reliable.length, 2);

        const f1 = readSequencedFrame(conn.reliable[0]!);
        assert.equal(f1.modifiers & ProtocolModifier.SEQUENCED, ProtocolModifier.SEQUENCED);
        assert.equal(f1.modifiers & ProtocolModifier.TIMED, 0);
        assert.equal(f1.seq, 1);
        assert.isTrue(f1.body.length > 0, 'first send is a full snapshot');

        assert.equal(readSequencedFrame(conn.reliable[1]!).seq, 2);
    });

    test('RoomInput.applyOptions decodes SEQUENCED + window from the handshake', () => {
        const room = {} as any;
        const ri = new RoomInput(room);

        // Build INPUT_OPTIONS: flags(SEQUENCED) at section offset 0, then the
        // receiveWindow self-describing number (48 = one fixnum byte).
        const bytes = new Uint8Array([InputFlags.SEQUENCED, 48]);
        const it = { offset: 0 };

        const ack = ri.applyOptions(bytes, it);
        assert.equal(it.offset, 2, 'consumed flags + window');
        assert.equal(ack, 0, 'fresh join carries no reconnect ack');
        assert.isTrue(ri.sequenced);

        // A handle created after the handshake inherits the mode.
        const handle = ri.handle({ type: MoveInput } as any);
        assert.isTrue(handle.sequenced, 'handle created post-handshake is sequenced');
    });

    test('reconnect: adoptReconnectAck negotiates ONLY the ack point; replay re-sends the unacked set in order', () => {
        const { handle, instance, conn } = makeHandle({ sequenced: true, receiveWindow: 64 });

        // Sent 1..5, server consumed through 3 before the connection dropped.
        for (let i = 1; i <= 5; i++) { instance.x = i; handle.send(); }
        handle.ackInput(3);

        // Reset must be a no-op for a sequenced handle (the replay seqs survive).
        handle.reset();
        assert.equal(handle.sentCount, 5, 'sequenced reset does not zero the stream');
        assert.equal(handle.lastProcessed, 3);

        // Reconnect handshake negotiates the ack point (RoomInput path).
        handle.adoptReconnectAck(3);
        assert.equal(handle.lastProcessed, 3);

        conn.reliable.length = 0;
        const resent = handle.replayAfterReconnect();
        assert.equal(resent, 2, 'seqs 4 and 5 are re-sent');
        assert.equal(conn.reliable.length, 2);

        const r1 = readSequencedFrame(conn.reliable[0]!);
        const r2 = readSequencedFrame(conn.reliable[1]!);
        assert.deepEqual([r1.seq, r2.seq], [4, 5], 'replays go in seq order');
        // Replays are FULL snapshots — a fresh server decoder must read them
        // without the pre-drop deltas. Decode into a clean instance and check.
        for (const frame of [r1, r2]) {
            const target = new MoveInput();
            const dec = new InputDecoder(target);
            dec.decode(frame.body as Buffer);
            assert.isTrue((target as any).x >= 4, 'replayed snapshot decodes its own x');
        }

        // A stale/over-large ack is clamped to what actually got sent.
        handle.adoptReconnectAck(999);
        assert.equal(handle.lastProcessed, 5, 'ack never advances past sentCount');
    });

    test('replay frames carry NO TIMED stamp (re-baseline on the first live send)', () => {
        const { handle, instance, conn } = makeHandle({ sequenced: true });
        for (let i = 1; i <= 2; i++) { instance.x = i; handle.send(); }
        conn.reliable.length = 0;
        handle.adoptReconnectAck(1);
        handle.replayAfterReconnect();
        for (const frame of conn.reliable) {
            assert.equal(frame[0] & ProtocolModifier.TIMED, 0);
            assert.equal(frame[0] & ProtocolModifier.SEQUENCED, ProtocolModifier.SEQUENCED);
        }
    });

    test('non-sequenced handle ignores the sequenced reconnect APIs (legacy behavior intact)', () => {
        const { handle, instance } = makeHandle();
        for (let i = 1; i <= 3; i++) { instance.x = i; handle.send(); }

        handle.adoptReconnectAck(2); // ignored
        assert.equal(handle.lastProcessed, 0);
        assert.equal(handle.replayAfterReconnect(), 0);

        // Legacy reset still zeroes.
        handle.reset();
        assert.equal(handle.sentCount, 0);
        assert.equal(handle.lastProcessed, 0);
    });

    test('handshake section tags are unchanged for legacy parsing (tag = INPUT_OPTIONS)', () => {
        // Smoke: the constants used by both sides agree.
        assert.equal(HandshakeSection.INPUT_OPTIONS, 2);
        assert.isTrue(InputFlags.SEQUENCED > 0 && InputFlags.RECONNECT_ACK > 0);
        assert.notEqual(InputFlags.SEQUENCED, InputFlags.RECONNECT_ACK);
    });
});
