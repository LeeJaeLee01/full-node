import { Kafka, logLevel } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_KEY_TOPIC ?? 'demo-key-order';
const groupId = process.env.KAFKA_KEY_GROUP_ID ?? 'full-node-key-order-consumer-group';

const kafka = new Kafka({
  clientId: 'key-order-consumer',
  brokers: getBrokers(),
  logLevel: logLevel.INFO,
});

type ConsumedRecord = {
  key: string;
  partition: number;
  offset: string;
  value: string;
};

async function main() {
  const consumer = kafka.consumer({ groupId });
  const records: ConsumedRecord[] = [];

  try {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    const expectedCount = Number(process.argv[2] ?? 12);

    console.log(
      `[key-order/consume] Reading "${topic}" from beginning (expect ${expectedCount} messages)`,
    );
    console.log('─'.repeat(80));

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const key = message.key?.toString() ?? '(no-key)';
        const value = message.value?.toString() ?? '';

        records.push({
          key,
          partition,
          offset: message.offset,
          value,
        });

        console.log(
          `[key-order/consume] key="${key}" partition=${partition} offset=${message.offset} | ${value}`,
        );

        if (records.length >= expectedCount) {
          await consumer.disconnect();

          console.log('─'.repeat(80));
          console.log('[key-order/consume] Order check per key:\n');

          const byKey = new Map<string, ConsumedRecord[]>();
          for (const record of records) {
            const bucket = byKey.get(record.key) ?? [];
            bucket.push(record);
            byKey.set(record.key, bucket);
          }

          for (const [key, keyRecords] of [...byKey.entries()].sort()) {
            const partition = keyRecords[0].partition;
            const stable = keyRecords.every((r) => r.partition === partition);
            const offsets = keyRecords.map((r) => Number(r.offset));
            const ordered = offsets.every(
              (offset, index) => index === 0 || offset > offsets[index - 1],
            );

            console.log(
              `  key="${key}" partition=${partition} stable=${stable ? 'YES' : 'NO'} ordered=${ordered ? 'YES' : 'NO'}`,
            );
            console.log(
              `    read order: ${keyRecords.map((r) => `offset=${r.offset}`).join(' → ')}`,
            );
          }

          process.exit(0);
        }
      },
    });
  } catch (error) {
    await consumer.disconnect();
    throw error;
  }
}

main().catch((error: unknown) => {
  console.error('[key-order/consume] failed:', error);
  process.exit(1);
});
