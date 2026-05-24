import { Kafka, logLevel } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_TOPIC ?? 'demo-messages';
const message = process.argv[2] ?? `Hello Kafka at ${new Date().toISOString()}`;

const kafka = new Kafka({
  clientId: 'single-node-producer',
  brokers: getBrokers(),
  logLevel: logLevel.INFO,
});

async function main() {
  const producer = kafka.producer();

  try {
    await producer.connect();
    await producer.send({
      topic,
      messages: [{ value: message }],
    });
    console.log(`[single-node/produce] topic="${topic}" message="${message}"`);
  } finally {
    await producer.disconnect();
  }
}

main().catch((error: unknown) => {
  console.error('[single-node/produce] failed:', error);
  process.exit(1);
});
