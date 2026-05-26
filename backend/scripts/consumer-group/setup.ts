import { Kafka } from 'kafkajs';
import { getBrokers } from '../_shared/env';
import { ensureTopic } from '../_shared/setup-topic';

const topic = process.env.KAFKA_GROUP_TOPIC ?? 'demo-consumer-group';
const numPartitions = Number(process.env.KAFKA_GROUP_PARTITION_COUNT ?? 3);

const kafka = new Kafka({
  clientId: 'consumer-group-setup',
  brokers: getBrokers(),
});

async function main() {
  console.log('[consumer-group/setup] Creating topic for Consumer Group demo');
  await ensureTopic(kafka, topic, numPartitions);
}

main().catch((error: unknown) => {
  console.error('[consumer-group/setup] failed:', error);
  process.exit(1);
});
