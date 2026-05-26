import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { getBrokers } from '../_shared/env';

const topic = process.env.KAFKA_GROUP_TOPIC ?? 'demo-consumer-group';
const count = Number(process.argv[2] ?? 12);

const kafka = new Kafka({
  clientId: 'consumer-group-producer',
  brokers: getBrokers(),
  logLevel: logLevel.INFO,
});

async function main() {
  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });

  try {
    await producer.connect();
    console.log(
      `[consumer-group/produce] Sending ${count} messages WITHOUT key to "${topic}"`,
    );
    console.log('─'.repeat(70));

    for (let index = 0; index < count; index += 1) {
      const value = `msg-${index + 1}`;
      const result = await producer.send({
        topic,
        messages: [{ value }],
      });

      const record = result[0];
      console.log(
        `[consumer-group/produce] #${index + 1} value="${value}" → partition=${record.partition}, offset=${record.baseOffset ?? '0'}`,
      );
    }

    console.log('─'.repeat(70));
    console.log(
      '[consumer-group/produce] Done. Start 4 consumers (same group) then observe assignment.',
    );
  } finally {
    await producer.disconnect();
  }
}

main().catch((error: unknown) => {
  console.error('[consumer-group/produce] failed:', error);
  process.exit(1);
});
