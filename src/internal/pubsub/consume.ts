import amqp from "amqplib";
import type { Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const ch = await conn.createChannel();
  await ch.assertExchange(exchange, "topic", { durable: true });
  const queue = await ch.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: queueType === SimpleQueueType.Transient,
    exclusive: queueType === SimpleQueueType.Transient,
  });
  await ch.bindQueue(queueName, exchange, key);
  return [ch, queue];
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => AckType,
): Promise<void> {
  const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);
  ch.consume(queue.queue, (message: amqp.ConsumeMessage | null) => {
    if (!message) return;
    const parsedMessage = JSON.parse(message.content.toString('utf-8'));
    const ack = handler(parsedMessage);
    if (ack === AckType.Ack) {
      ch.ack(message);
      console.log("ack")
    } else if (ack === AckType.NackRequeue) {
      ch.nack(message, false, true);
      console.log("requeue");
    } else {
      ch.nack(message, false, false);
      console.log("discard");
    }
  });
}
