import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
} from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import {
  AckType,
  SimpleQueueType,
  subscribeMsgPack,
} from "../internal/pubsub/consume.js";
import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";

async function main() {
  const connStr = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(connStr);
  const ch = await conn.createConfirmChannel();
  await subscribeMsgPack(
    conn,
    ExchangePerilTopic,
    GameLogSlug,
    `${GameLogSlug}.*`,
    SimpleQueueType.Durable,
    async (data: GameLog) => {
      await writeLog(data);
      process.stdout.write("> ");
      return AckType.Ack;
    },
  );
  console.log("Connection is successful!");
  if (!process.stdin.isTTY) {
    console.log("Non-interactive mode: skipping command input.");
    return;
  }
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
