import { AssignerProtocol, Kafka } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_GROUP_TOPIC ?? 'demo-consumer-group';
const groupId =
  process.env.KAFKA_GROUP_ID ?? 'full-node-consumer-group-demo';

const kafka = new Kafka({
  clientId: 'consumer-group-describe',
  brokers: getBrokers(),
});

async function main() {
  const admin = kafka.admin();

  try {
    await admin.connect();

    const [group, metadata] = await Promise.all([
      admin.describeGroups([groupId]),
      admin.fetchTopicMetadata({ topics: [topic] }),
    ]);

    const partitionCount =
      metadata.topics[0]?.partitions.length ?? 0;
    const described = group.groups[0];

    console.log('─'.repeat(70));
    console.log(`[describe] Topic: ${topic} (${partitionCount} partition(s))`);
    console.log(`[describe] Consumer group: ${groupId}`);
    console.log(`[describe] State: ${described?.state ?? 'unknown'}`);
    console.log(`[describe] Protocol: ${described?.protocol ?? 'n/a'}`);
    console.log(`[describe] Members: ${described?.members.length ?? 0}`);
    console.log('─'.repeat(70));

    if (!described?.members.length) {
      console.log('[describe] No active members. Start consumers first.');
      return;
    }

    described.members.forEach((member, index) => {
      const decoded = member.memberAssignment
        ? AssignerProtocol.MemberAssignment.decode(member.memberAssignment)
        : { assignment: {} as Record<string, number[]> };
      const partitions: number[] = decoded.assignment[topic] ?? [];
      const idle = partitions.length === 0;

      console.log(
        `  member #${index + 1} clientId=${member.clientId} ${idle ? '→ IDLE (no partitions)' : `→ partitions [${partitions.sort((a, b) => a - b).join(', ')}]`}`,
      );
    });

    console.log('─'.repeat(70));
    console.log(
      `[describe] Rule: at most ${partitionCount} consumer(s) get work; extra members stay IDLE.`,
    );
  } finally {
    await admin.disconnect();
  }
}

main().catch((error: unknown) => {
  console.error('[consumer-group/describe] failed:', error);
  process.exit(1);
});
