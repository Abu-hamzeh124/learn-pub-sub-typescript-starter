import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

export async function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const val = JSON.stringify(value);
  const buffer = Buffer.from(val);
  ch.publish(exchange, routingKey, buffer, { contentType: "application/json" });
}

export async function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const val = encode(value);
  const buffer = Buffer.from(val);
  ch.publish(exchange, routingKey, buffer, { contentType: "application/x-msgpack" });
}
