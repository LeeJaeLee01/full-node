import { Kafka } from 'kafkajs';
import { getBrokers } from '../_shared/env';
import { ensureTopic } from '../_shared/setup-topic';

const topic = process.env.KAFKA_COMMIT_TOPIC ?? 'demo-manual-commit';
const numPartitions = Number(process.env.KAFKA_COMMIT_PARTITION_COUNT ?? '1');

const kafka = new Kafka({
  clientId: 'manual-commit-setup',
  brokers: getBrokers(),
});

async function main() {
  console.log(
    '[manual-commit/setup] Topic for manual commit demo (NestJS consumer)',
  );
  await ensureTopic(kafka, topic, numPartitions);
  console.log(`[manual-commit/setup] Ready: "${topic}" (${numPartitions} partition(s))`);
}

main().catch((error: unknown) => {
  console.error('[manual-commit/setup] failed:', error);
  process.exit(1);
});
