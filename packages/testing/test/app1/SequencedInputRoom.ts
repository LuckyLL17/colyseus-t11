import { Room, type Client, type StepContext, CloseCode } from "@colyseus/core";
import { MapSchema, Schema, type } from "@colyseus/schema";

/**
 * Test room for the sequenced reliable input channel:
 * - inputs are ordered/deduped server-side (`defineInput({ reliable })`),
 * - the fixed-step loop consumes ONLY policy-released inputs,
 * - `fires` counts side-effectful inputs (must match distinct seqs exactly).
 */
export class SeqInput extends Schema {
  // Echoes the client's application seq in the body for assertions; the
  // framework's ordered channel carries its OWN wire seq (SEQUENCED prefix),
  // this field is just test instrumentation.
  @type("int32") seq: number = 0;
  @type("number") x: number = 0;
  @type("boolean") fire: boolean = false;
}

export class SeqPlayer extends Schema {
  @type("number") x: number = 0;
  @type("number") fires: number = 0;
  @type("number") lastConsumedSeq: number = 0;
}

export class SeqState extends Schema {
  @type({ map: SeqPlayer }) players = new MapSchema<SeqPlayer>();
  @type("number") steps: number = 0;
}

export class SequencedInputRoom extends Room {
  maxClients = 4;
  state = new SeqState();

  input = this.defineInput(SeqInput, {
    // Ordered reliable: small window + short gap age so the test doesn't wait.
    reliable: { windowSize: 16, maxGapAgeMs: 200 },
  });

  onCreate() {
    this.setPatchRate(1000);
    // 20 Hz — the framework calls the ordered gap-age sweep before each step.
    this.setFixedTimestep((_ctx: StepContext) => {
      this.state.steps++;
      for (const [sid, player] of this.state.players) {
        const channel = this.input.get(sid);
        // The ordered buffer exposes only RELEASED inputs; parked frames are
        // invisible. Idle is off, so an empty tick (open gap) yields nothing.
        for (const frame of channel.drain()) {
          player.x = frame.x;
          if (frame.fire) { player.fires++; }
          player.lastConsumedSeq = frame.seq;
        }
      }
    }, 20);
  }

  onJoin(client: Client) {
    this.state.players.set(client.sessionId, new SeqPlayer());
  }

  async onLeave(client: Client, code: number) {
    // Keep the player row visible through the reconnect window so the test can
    // watch the frozen seat; the input subsystem collapses to the consumed seq.
    if (code === CloseCode.CONSENTED) {
      this.state.players.delete(client.sessionId);
      return;
    }
    try {
      await this.allowReconnection(client, 5);
    } catch (e) {
      this.state.players.delete(client.sessionId);
    }
  }
}
