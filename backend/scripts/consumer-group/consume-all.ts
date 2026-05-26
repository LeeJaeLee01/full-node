import { Kafka, logLevel } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_GROUP_TOPIC ?? 'demo-consumer-group';
const groupId =
  process.env.KAFKA_GROUP_ID ?? 'full-node-consumer-group-demo';
const consumerCount = Number(process.argv[2] ?? 4);
const partitionCount = Number(process.env.KAFKA_GROUP_PARTITION_COUNT ?? 3);

const kafka = new Kafka({
  clientId: 'consumer-group-demo-runner',
  brokers: getBrokers(),
  logLevel: logLevel.WARN,
});

function formatAssignment(
  consumerId: number,
  assignment: Record<string, number[]> | undefined,
): string {
  const partitions = assignment?.[topic] ?? [];
  if (partitions.length === 0) {
    return `[consumer-${consumerId}] Assigned: (none) → IDLE`;
  }

  return `[consumer-${consumerId}] Assigned partition(s): [${partitions.sort((a, b) => a - b).join(', ')}]`;
}

async function startConsumer(consumerId: number): Promise<void> {
  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
  });

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    console.log(formatAssignment(consumerId, payload.memberAssignment));
  });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const value = message.value?.toString() ?? '';
      console.log(
        `[consumer-${consumerId}] partition=${partition} offset=${message.offset} value="${value}"`,
      );
    },
  });
}

async function main() {
  console.log(
    `[consumer-group/consume-all] Starting ${consumerCount} consumer(s) in group "${groupId}"`,
  );
  console.log(
    `[consumer-group/consume-all] Topic "${topic}" — expect max ${partitionCount} active with ${partitionCount} partition(s)`,
  );
  console.log('─'.repeat(70));

  await Promise.all(
    Array.from({ length: consumerCount }, (_, index) =>
      startConsumer(index + 1),
    ),
  );

}

main().catch((error: unknown) => {
  console.error('[consumer-group/consume-all] failed:', error);
  process.exit(1);
});
