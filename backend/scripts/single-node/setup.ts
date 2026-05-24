import { Kafka } from 'kafkajs';
import { getBrokers } from '../_shared/env';
import { ensureTopic } from '../_shared/setup-topic';

const topic = process.env.KAFKA_TOPIC ?? 'demo-messages';
const numPartitions = 1;

const kafka = new Kafka({
  clientId: 'single-node-setup',
  brokers: getBrokers(),
});

async function main() {
  console.log('[single-node/setup] Creating topic for basic produce/consume demo');
  await ensureTopic(kafka, topic, numPartitions);
}

main().catch((error: unknown) => {
  console.error('[single-node/setup] failed:', error);
  process.exit(1);
});
