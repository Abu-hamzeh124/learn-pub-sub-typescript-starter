import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect } from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
async function main() {
  const connStr = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(connStr);
  const ch = await conn.createConfirmChannel();
  const playingState: PlayingState = {
    isPaused: true,
  };
  console.log("Connection is successful!");
  await publishJSON(ch, ExchangePerilDirect, PauseKey, playingState);
  printServerHelp();
  while (true) {
    const input = await getInput();
    if (input.length === 0) {
      continue;
    }
    if (input[0] === "pause") {
      console.log("Sending pause message");
      await publishJSON(ch, ExchangePerilDirect, PauseKey, playingState);
    } else if (input[0] === "resume") {
      console.log("Sending resume message");
      await publishJSON(ch, ExchangePerilDirect, PauseKey, { isPaused: false });
    } else if (input[0] === "quit") {
      console.log("Quiting");
      break;
    } else {
      console.log("unknown command");
    }
  }
  process.on("SIGINT", () => {
    console.log("Shutting Down the connection.");
    conn.close();
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
