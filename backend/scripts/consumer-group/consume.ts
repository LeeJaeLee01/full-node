import { Kafka, logLevel } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_GROUP_TOPIC ?? 'demo-consumer-group';
const groupId =
  process.env.KAFKA_GROUP_ID ?? 'full-node-consumer-group-demo';

const consumerId = Number(process.argv[2] ?? 1);

if (!Number.isInteger(consumerId) || consumerId < 1 || consumerId > 16) {
  console.error(
    '[consumer-group/consume] Usage: npm run kafka:group:consume -- <consumer-id 1-16>',
  );
  process.exit(1);
}

const kafka = new Kafka({
  clientId: `consumer-group-demo-${consumerId}`,
  brokers: getBrokers(),
  logLevel: logLevel.WARN,
});

function formatAssignment(
  assignment: Record<string, number[]> | undefined,
): string {
  if (!assignment) {
    return '(none)';
  }

  const partitions = assignment[topic] ?? [];
  if (partitions.length === 0) {
    return '(none — consumer is IDLE)';
  }

  return partitions.sort((a, b) => a - b).join(', ');
}

async function main() {
  const consumer = kafka.consumer({ groupId });
  let assignedPartitions: number[] = [];

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    assignedPartitions = payload.memberAssignment[topic] ?? [];
    const sorted = [...assignedPartitions].sort((a, b) => a - b);

    console.log('─'.repeat(70));
    console.log(
      `[consumer-${consumerId}] GROUP_JOIN groupId="${groupId}" memberId=${payload.memberId}`,
    );
    console.log(
      `[consumer-${consumerId}] Assigned partition(s): ${formatAssignment(payload.memberAssignment)}`,
    );

    if (sorted.length === 0) {
      console.log(
        `[consumer-${consumerId}] ⚠ IDLE: no partition assigned (consumers > partitions in group)`,
      );
    } else {
      console.log(
        `[consumer-${consumerId}] Active: will process partition(s) [${sorted.join(', ')}]`,
      );
    }
    console.log('─'.repeat(70));
  });

  try {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    console.log(
      `[consumer-${consumerId}] Joining group "${groupId}" on topic "${topic}" (Ctrl+C to stop)`,
    );

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const value = message.value?.toString() ?? '';
        console.log(
          `[consumer-${consumerId}] partition=${partition} offset=${message.offset} value="${value}"`,
        );
      },
    });
  } catch (error) {
    await consumer.disconnect();
    throw error;
  }
}

main().catch((error: unknown) => {
  console.error(`[consumer-${consumerId}] failed:`, error);
  process.exit(1);
});
