import { Kafka, Partitioners } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_COMMIT_TOPIC ?? 'demo-manual-commit';

const kafka = new Kafka({
  clientId: 'manual-commit-produce',
  brokers: getBrokers(),
});

const messages = [
  { value: JSON.stringify({ action: 'create-order', orderId: 'o-1' }) },
  { value: JSON.stringify({ action: 'pay-order', orderId: 'o-1' }) },
  { value: 'FAIL' },
  { value: JSON.stringify({ action: 'ship-order', orderId: 'o-1' }) },
];

async function main() {
  const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
  });

  await producer.connect();
  console.log(`[manual-commit/produce] Sending ${messages.length} messages to "${topic}"`);
  console.log('[manual-commit/produce] Message #3 is "FAIL" — consumer should NOT commit it\n');

  for (let i = 0; i < messages.length; i++) {
    const [record] = await producer.send({
      topic,
      messages: [messages[i]],
    });
    const offset = record.baseOffset ?? record.offset ?? 'n/a';
    console.log(
      `[manual-commit/produce] #${i + 1} → partition=${record.partition}, offset=${offset} | ${messages[i].value}`,
    );
  }

  await producer.disconnect();
  console.log('\n[manual-commit/produce] Done. Start NestJS consumer: npm run start:consumer:dev');
}

main().catch((error: unknown) => {
  console.error('[manual-commit/produce] failed:', error);
  process.exit(1);
});
