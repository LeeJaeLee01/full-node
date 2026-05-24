import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaClient } from '../kafka/kafka.client';
import { KAFKA_GROUP_ID, KAFKA_TOPIC } from '../kafka/kafka.constants';

const MAX_RETRIES = 10;
const RETRY_DELAY_MS = 3000;

@Injectable()
export class KafkaConsumerService implements OnModuleInit {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private running = false;

  constructor(private readonly kafkaClient: KafkaClient) {}

  async onModuleInit(): Promise<void> {
    await this.startWithRetry();
  }

  private async startWithRetry(attempt = 1): Promise<void> {
    try {
      await this.kafkaClient.connect();

      const consumer = this.kafkaClient.getConsumer();
      await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const value = message.value?.toString() ?? '';
          this.logger.log(
            `Consumed "${topic}" [partition ${partition}, offset ${message.offset}]: ${value}`,
          );
        },
      });

      this.running = true;
      this.logger.log(
        `Subscribed to "${KAFKA_TOPIC}" with group "${KAFKA_GROUP_ID}"`,
      );
    } catch (error) {
      await this.kafkaClient.disconnect();
      this.running = false;

      const message = error instanceof Error ? error.message : String(error);

      if (attempt >= MAX_RETRIES) {
        this.logger.error(
          `Consumer failed after ${MAX_RETRIES} attempts: ${message}`,
        );
        throw error;
      }

      this.logger.warn(
        `Start attempt ${attempt}/${MAX_RETRIES} failed: ${message}. Retrying in ${RETRY_DELAY_MS}ms...`,
      );

      await this.sleep(RETRY_DELAY_MS);
      await this.startWithRetry(attempt + 1);
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
