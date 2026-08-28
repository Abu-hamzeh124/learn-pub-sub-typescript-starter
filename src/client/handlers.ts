import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";
import { MoveOutcome } from "../internal/gamelogic/move.js"; 
export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState) => { 
        handlePause(gs, ps);
        console.log("> ");
	return AckType.Ack;
    };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => AckType{
    return (move: ArmyMove) => {
        const moveOut = handleMove(gs, move);
        console.log("> ");
	if (moveOut === MoveOutcome.Safe || moveOut === MoveOutcome.MakeWar) {
	  return AckType.Ack;
	} else {
	  return AckType.NackDiscard;
	}
    };
}
