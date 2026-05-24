import { Kafka, logLevel } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_TOPIC ?? 'demo-messages';
const groupId = process.env.KAFKA_GROUP_ID ?? 'full-node-consumer-script';

const kafka = new Kafka({
  clientId: 'single-node-consumer',
  brokers: getBrokers(),
  logLevel: logLevel.INFO,
});

async function main() {
  const consumer = kafka.consumer({ groupId });

  try {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    console.log(
      `[single-node/consume] listening topic="${topic}" group="${groupId}" (Ctrl+C to stop)`,
    );

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value?.toString() ?? '';
        console.log(
          `[single-node/consume] topic="${topic}" partition=${partition} offset=${message.offset} value="${value}"`,
        );
      },
    });
  } catch (error) {
    await consumer.disconnect();
    throw error;
  }
}

main().catch((error: unknown) => {
  console.error('[single-node/consume] failed:', error);
  process.exit(1);
});
