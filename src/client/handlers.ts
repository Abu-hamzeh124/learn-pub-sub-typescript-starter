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
} from "../internal/gamelogic/war.js";
import type { RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import {
  ExchangePerilTopic,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import { publishGameLog } from "./index.js";

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

export async function handlerWar(
  gs: GameState,
  ch: ConfirmChannel,
): Promise<(rw: RecognitionOfWar) => Promise<AckType>> {
  const handler = async (rw: RecognitionOfWar) => {
    const warMsg = handleWar(gs, rw);
    if (warMsg.result === WarOutcome.NotInvolved) {
      return AckType.NackRequeue;
    }
    if (warMsg.result === WarOutcome.NoUnits) {
      return AckType.NackDiscard;
    }
    if (
      warMsg.result === WarOutcome.OpponentWon ||
      warMsg.result === WarOutcome.YouWon
    ) {
      await publishGameLog(ch, rw.attacker.username, `${warMsg.winner} won a war against ${warMsg.loser}`);
      return AckType.Ack;
    }
    if (warMsg.result === WarOutcome.Draw) {
      try {
        await publishGameLog(ch, rw.attacker.username, `A war between ${warMsg.attacker} and ${warMsg.defender} resulted in draw`);
        return AckType.Ack;
      } catch (error) {
        return AckType.NackRequeue
      }
      
    }
    console.log(`invalid war message`);
    return AckType.NackDiscard;
  };
  process.stdout.write("> ");
  return handler;
}
