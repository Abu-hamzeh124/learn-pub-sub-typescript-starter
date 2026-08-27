import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
} from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";

async function main() {
  const connStr = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(connStr);
  const ch = await conn.createConfirmChannel();
  const gameLogs = await declareAndBind(
    conn,
    ExchangePerilTopic,
    GameLogSlug,
    GameLogSlug + ".*",
    SimpleQueueType.Durable,
  );
  console.log("Connection is successful!");
  printServerHelp();
  process.on("SIGINT", async () => {
    console.log("Shutting down the connection...");
    try {
      await conn.close();
    } catch (err) {
      console.error("Error closing connection:", err);
    } finally {
      process.exit(0);
    }
  });
  while (true) {
    const input = await getInput();
    if (input.length === 0) {
      continue;
    }
    if (input[0] === "pause") {
      console.log("Sending pause message");
      await publishJSON(ch, ExchangePerilDirect, PauseKey, { isPaused: true });
    } else if (input[0] === "resume") {
      console.log("Sending resume message");
      await publishJSON(ch, ExchangePerilDirect, PauseKey, { isPaused: false });
    } else if (input[0] === "quit") {
      console.log("Quiting");
      await conn.close();
      break;
    } else {
      console.log("unknown command");
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
