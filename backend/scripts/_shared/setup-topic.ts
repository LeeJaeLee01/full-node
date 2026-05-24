import { Kafka } from 'kafkajs';

export async function ensureTopic(
  kafka: Kafka,
  topic: string,
  numPartitions: number,
  replicationFactor = 1,
): Promise<void> {
  const admin = kafka.admin();

  try {
    await admin.connect();

    const existingTopics = await admin.listTopics();
    if (existingTopics.includes(topic)) {
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      const partitionCount = metadata.topics[0]?.partitions.length ?? 0;

      console.log(
        `[setup] Topic "${topic}" already exists with ${partitionCount} partition(s)`,
      );

      if (partitionCount !== numPartitions) {
        console.warn(
          `[setup] Expected ${numPartitions} partitions. Delete topic and rerun if you need to recreate it.`,
        );
      }

      metadata.topics[0]?.partitions.forEach((partition) => {
        console.log(
          `[setup] partition=${partition.partitionId} leader=${partition.leader}`,
        );
      });
      return;
    }

    await admin.createTopics({
      topics: [{ topic, numPartitions, replicationFactor }],
    });

    console.log(
      `[setup] Created topic "${topic}" with ${numPartitions} partition(s)`,
    );
  } finally {
    await admin.disconnect();
  }
}
