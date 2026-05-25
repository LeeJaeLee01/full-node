import { Kafka } from 'kafkajs';
import { getBrokers } from '../_shared/env';
import { ensureTopic } from '../_shared/setup-topic';

const topic = process.env.KAFKA_KEY_TOPIC ?? 'demo-key-order';
const numPartitions = Number(process.env.KAFKA_KEY_PARTITION_COUNT ?? 3);

const kafka = new Kafka({
  clientId: 'key-order-setup',
  brokers: getBrokers(),
});

async function main() {
  console.log('[key-order/setup] Creating topic for message key & ordering demo');
  await ensureTopic(kafka, topic, numPartitions);
}

main().catch((error: unknown) => {
  console.error('[key-order/setup] failed:', error);
  process.exit(1);
});
