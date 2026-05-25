import { Kafka, logLevel } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_KEY_TOPIC ?? 'demo-key-order';

type KeyEvent = {
  key: string;
  value: string;
  seq: number;
};

const events: KeyEvent[] = [
  { key: 'user-1', value: 'user-1 đặt hàng #1', seq: 1 },
  { key: 'user-2', value: 'user-2 đăng ký', seq: 2 },
  { key: 'user-1', value: 'user-1 thanh toán', seq: 3 },
  { key: 'user-3', value: 'user-3 đăng ký', seq: 4 },
  { key: 'user-2', value: 'user-2 đặt hàng #1', seq: 5 },
  { key: 'user-1', value: 'user-1 giao hàng', seq: 6 },
  { key: 'user-3', value: 'user-3 đặt hàng #1', seq: 7 },
  { key: 'user-2', value: 'user-2 thanh toán', seq: 8 },
  { key: 'user-1', value: 'user-1 hoàn tất', seq: 9 },
  { key: 'user-3', value: 'user-3 thanh toán', seq: 10 },
  { key: 'user-2', value: 'user-2 hoàn tất', seq: 11 },
  { key: 'user-3', value: 'user-3 hoàn tất', seq: 12 },
];

const kafka = new Kafka({
  clientId: 'key-order-producer',
  brokers: getBrokers(),
  logLevel: logLevel.INFO,
});

type KeyRecord = {
  partition: number;
  offset: string;
  seq: number;
  value: string;
};

async function main() {
  const producer = kafka.producer();
  const byKey = new Map<string, KeyRecord[]>();

  try {
    await producer.connect();
    console.log(
      `[key-order/produce] Sending ${events.length} events WITH key to "${topic}"`,
    );
    console.log('─'.repeat(80));

    for (const event of events) {
      const result = await producer.send({
        topic,
        messages: [{ key: event.key, value: event.value }],
      });

      const record = result[0];
      const entry: KeyRecord = {
        partition: record.partition,
        offset: String(record.baseOffset ?? '0'),
        seq: event.seq,
        value: event.value,
      };

      const bucket = byKey.get(event.key) ?? [];
      bucket.push(entry);
      byKey.set(event.key, bucket);

      console.log(
        `[key-order/produce] #${event.seq} key="${event.key}" → partition=${record.partition}, offset=${entry.offset} | ${event.value}`,
      );
    }

    console.log('─'.repeat(80));
    console.log('[key-order/produce] Verification by key:\n');

    let allKeysSamePartition = true;
    let allKeysOrdered = true;

    for (const [key, records] of [...byKey.entries()].sort()) {
      const partition = records[0].partition;
      const samePartition = records.every((r) => r.partition === partition);
      const offsets = records.map((r) => Number(r.offset));
      const ordered =
        offsets.length <= 1 ||
        offsets.every((offset, index) => index === 0 || offset > offsets[index - 1]);

      if (!samePartition) {
        allKeysSamePartition = false;
      }
      if (!ordered) {
        allKeysOrdered = false;
      }

      console.log(`  key="${key}"`);
      console.log(`    partition: ${partition} (stable: ${samePartition ? 'YES' : 'NO'})`);
      console.log(
        `    offsets:   [${records.map((r) => r.offset).join(', ')}] (ordered: ${ordered ? 'YES' : 'NO'})`,
      );
      console.log(
        `    sequence:  ${records.map((r) => `#${r.seq}`).join(' → ')}`,
      );
      console.log('');
    }

    console.log('─'.repeat(80));
    console.log(
      `[key-order/produce] Same key → same partition: ${allKeysSamePartition ? 'PASS' : 'FAIL'}`,
    );
    console.log(
      `[key-order/produce] Same key → offset tăng dần (đúng thứ tự): ${allKeysOrdered ? 'PASS' : 'FAIL'}`,
    );
  } finally {
    await producer.disconnect();
  }
}

main().catch((error: unknown) => {
  console.error('[key-order/produce] failed:', error);
  process.exit(1);
});
