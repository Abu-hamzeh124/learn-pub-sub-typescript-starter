import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type {
  GameState,
  PlayingState,
} from "../internal/gamelogic/gamestate.js";
import { handleMove } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";
import { MoveOutcome } from "../internal/gamelogic/move.js";
import type { ConfirmChannel } from "amqplib";
import {
  handleWar,
  WarOutcome,
  type WarResolution,
} from "../internal/gamelogic/war.js";
import type { RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  const handler = (ps: PlayingState) => {
    handlePause(gs, ps);
    return AckType.Ack;
  };
  process.stdout.write("> ");
  return handler;
}

export function handlerMove(
  gs: GameState,
  ch: ConfirmChannel,
): (move: ArmyMove) => AckType {
  const handler = (move: ArmyMove) => {
    const moveOut = handleMove(gs, move);

    if (moveOut === MoveOutcome.Safe) {
      return AckType.Ack;
    }
    if (moveOut === MoveOutcome.MakeWar) {
      const rw = {
        attacker: move.player,
        defender: gs.getPlayerSnap(),
      };
      const pub = ch.publish(
        ExchangePerilTopic,
        `${WarRecognitionsPrefix}.${move.player.username}`,
        Buffer.from(JSON.stringify(rw)),
      );
      if (!pub) {
        return AckType.NackRequeue;
      }
      return AckType.Ack;
    }
    return AckType.NackDiscard;
  };
  process.stdout.write("> ");
  return handler;
}

export function handlerWar(gs: GameState): (rw: RecognitionOfWar) => AckType {
  const handler = (rw: RecognitionOfWar) => {
    const warMsg = handleWar(gs, rw);
    if (warMsg.result === WarOutcome.NotInvolved) {
      return AckType.NackRequeue;
    }
    if (warMsg.result === WarOutcome.NoUnits) {
      return AckType.NackDiscard;
    }
    if (
      warMsg.result === WarOutcome.Draw ||
      warMsg.result === WarOutcome.OpponentWon ||
      warMsg.result === WarOutcome.YouWon
    ) {
      return AckType.Ack;
    }
    console.log(`invalid war message`);
    return AckType.NackDiscard;
  };
  process.stdout.write("> ");
  return handler;
}
