import { Kafka } from 'kafkajs';
import { getBrokers } from '../_shared/env';
import { ensureTopic } from '../_shared/setup-topic';

const topic = process.env.KAFKA_PARTITION_TOPIC ?? 'demo-partitions';
const numPartitions = Number(process.env.KAFKA_PARTITION_COUNT ?? 3);

const kafka = new Kafka({
  clientId: 'partition-setup',
  brokers: getBrokers(),
});

async function main() {
  console.log('[partition/setup] Creating topic for partition & offset demo');
  await ensureTopic(kafka, topic, numPartitions);
}

main().catch((error: unknown) => {
  console.error('[partition/setup] failed:', error);
  process.exit(1);
});
