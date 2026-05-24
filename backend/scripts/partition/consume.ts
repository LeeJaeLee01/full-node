import { Kafka, logLevel } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_PARTITION_TOPIC ?? 'demo-partitions';
const groupId =
  process.env.KAFKA_PARTITION_GROUP_ID ?? 'full-node-partition-consumer-group';

const kafka = new Kafka({
  clientId: 'partition-consumer',
  brokers: getBrokers(),
  logLevel: logLevel.INFO,
});

async function main() {
  const consumer = kafka.consumer({ groupId });
  const stats = new Map<number, { count: number; offsets: string[] }>();

  try {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    const maxMessages = Number(process.argv[2] ?? 9);

    console.log(
      `[partition/consume] Reading "${topic}" from beginning (stop after ${maxMessages} messages)`,
    );
    console.log('─'.repeat(70));

    let received = 0;

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const value = message.value?.toString() ?? '';
        const bucket = stats.get(partition) ?? { count: 0, offsets: [] };

        bucket.count += 1;
        bucket.offsets.push(message.offset);
        stats.set(partition, bucket);

        console.log(
          `[partition/consume] partition=${partition} offset=${message.offset} value="${value}"`,
        );

        received += 1;
        if (received >= maxMessages) {
          console.log('─'.repeat(70));
          console.log('[partition/consume] Partition / offset summary:');

          [...stats.entries()]
            .sort(([a], [b]) => a - b)
            .forEach(([partition, item]) => {
              console.log(
                `  partition ${partition}: ${item.count} message(s), offsets=[${item.offsets.join(', ')}]`,
              );
            });

          await consumer.disconnect();
          process.exit(0);
        }
      },
    });
  } catch (error) {
    await consumer.disconnect();
    throw error;
  }
}

main().catch((error: unknown) => {
  console.error('[partition/consume] failed:', error);
  process.exit(1);
});
