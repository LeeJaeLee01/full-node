export function getBrokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
}
