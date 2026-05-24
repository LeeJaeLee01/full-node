import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_PARTITION_TOPIC ?? 'demo-partitions';
const count = Number(process.argv[2] ?? 9);

const kafka = new Kafka({
  clientId: 'partition-producer',
  brokers: getBrokers(),
  logLevel: logLevel.INFO,
});

async function main() {
  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });

  const distribution = new Map<number, { count: number; offsets: string[] }>();

  try {
    await producer.connect();
    console.log(
      `[partition/produce] Sending ${count} messages WITHOUT key to "${topic}" (round-robin)`,
    );
    console.log('─'.repeat(70));

    for (let index = 0; index < count; index += 1) {
      const value = `message-${index + 1}`;
      const result = await producer.send({
        topic,
        messages: [{ value }],
      });

      const record = result[0];
      const bucket = distribution.get(record.partition) ?? {
        count: 0,
        offsets: [],
      };

      const offset = String(record.baseOffset ?? '0');
      bucket.count += 1;
      bucket.offsets.push(offset);
      distribution.set(record.partition, bucket);

      console.log(
        `[partition/produce] #${index + 1} value="${value}" → partition=${record.partition}, offset=${offset}`,
      );
    }

    console.log('─'.repeat(70));
    console.log('[partition/produce] Distribution summary:');

    [...distribution.entries()]
      .sort(([a], [b]) => a - b)
      .forEach(([partition, stats]) => {
        console.log(
          `  partition ${partition}: ${stats.count} message(s), offsets=[${stats.offsets.join(', ')}]`,
        );
      });

    const counts = [...distribution.values()].map((item) => item.count);
    const min = Math.min(...counts);
    const max = Math.max(...counts);

    console.log('─'.repeat(70));
    if (max - min <= 1) {
      console.log('[partition/produce] Result: evenly distributed (round-robin)');
    } else {
      console.log('[partition/produce] Result: uneven distribution detected');
    }
  } finally {
    await producer.disconnect();
  }
}

main().catch((error: unknown) => {
  console.error('[partition/produce] failed:', error);
  process.exit(1);
});
