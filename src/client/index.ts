import amqb from "amqplib";
import {
  clientWelcome,
  commandStatus,
  printClientHelp,
} from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { getInput } from "../internal/gamelogic/gamelogic.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerPause, handlerMove, handlerWar } from "./handlers.js";
import { publishJSON } from "../internal/pubsub/publish.js";

async function main() {
  const connStr = "amqp://guest:guest@localhost:5672/";
  const conn = await amqb.connect(connStr);
  const ch = await conn.createConfirmChannel();
  const username = await clientWelcome();
  const pauseQueue = await declareAndBind(
    conn,
    ExchangePerilDirect,
    "pause" + "." + username,
    PauseKey,
    SimpleQueueType.Transient,
  );
  const state = new GameState(username);
  await subscribeJSON(
    conn, 
    ExchangePerilDirect, 
    PauseKey + '.' + username, 
    PauseKey, 
    SimpleQueueType.Transient, 
    handlerPause(state)
  );

  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `army_moves.${username}`,
    `army_moves.*`,
    SimpleQueueType.Transient,
    handlerMove(state, ch),
  );

  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `war.${username}`,
    `war.*`,
    SimpleQueueType.Durable,
    handlerWar(state),
  );  

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
        const move = commandMove(state, input);
        await publishJSON(ch, ExchangePerilTopic, `army_moves.${username}`, move);
        console.log("Moved published successfuly");
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
