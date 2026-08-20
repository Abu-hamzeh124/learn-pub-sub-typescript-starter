import type { ConfirmChannel } from "amqplib";

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
