import amqb from "amqplib";
import {
  clientWelcome,
  commandStatus,
  printClientHelp,
} from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { getInput } from "../internal/gamelogic/gamelogic.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";

async function main() {
  const connStr = "amqp://guest:guest@localhost:5672/";
  const conn = await amqb.connect(connStr);
  const ch = await conn.createConfirmChannel();
  const username = await clientWelcome();
  const queue = await declareAndBind(
    conn,
    ExchangePerilDirect,
    "pause" + "." + username,
    PauseKey,
    SimpleQueueType.Transient,
  );
  const state = new GameState(username);
  console.log("Starting Peril client...");
  while (true) {
    const input = await getInput();
    if (input.length === 0) {
      continue;
    }
    if (input[0] === "spawn") {
      try {
        commandSpawn(state, input);
      } catch (error) {
        throw error;
      }
    } else if (input[0] === "move") {
      try {
        commandMove(state, input);
        console.log("Moved successfuly");
      } catch (error) {
        throw error;
      }
    } else if (input[0] === "status") {
      await commandStatus(state);
    } else if (input[0] === "help") {
      printClientHelp();
    } else if (input[0] === "spam") {
      console.log("Spamming not allowed yet!");
    } else if (input[0] === "quit") {
      console.log("exiting");
      break;
    } else {
      console.log("Unknown command!");
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
