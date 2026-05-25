export const KAFKA_CLIENT_ID =
  process.env.KAFKA_CLIENT_ID ?? 'full-node-backend-consumer';
export const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(
  ',',
);
export const KAFKA_TOPIC = process.env.KAFKA_TOPIC ?? 'demo-messages';
/** Topic NestJS consumer subscribe (ưu tiên KAFKA_COMMIT_TOPIC cho demo manual commit) */
export const KAFKA_CONSUMER_TOPIC =
  process.env.KAFKA_COMMIT_TOPIC ?? process.env.KAFKA_TOPIC ?? 'demo-messages';
export const KAFKA_GROUP_ID =
  process.env.KAFKA_GROUP_ID ?? 'full-node-consumer-group';
/** Dev: true = đọc từ đầu topic khi group chưa có offset */
export const KAFKA_CONSUMER_FROM_BEGINNING =
  process.env.KAFKA_CONSUMER_FROM_BEGINNING === 'true';
