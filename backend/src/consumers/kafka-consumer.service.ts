import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaClient } from '../kafka/kafka.client';
import {
  KAFKA_CONSUMER_FROM_BEGINNING,
  KAFKA_CONSUMER_TOPIC,
  KAFKA_GROUP_ID,
} from '../kafka/kafka.constants';
import {
  BusinessMessageError,
  MessageProcessorService,
} from './message-processor.service';

const MAX_RETRIES = 10;
const RETRY_DELAY_MS = 3000;

@Injectable()
export class KafkaConsumerService implements OnModuleInit {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private running = false;

  constructor(
    private readonly kafkaClient: KafkaClient,
    private readonly messageProcessor: MessageProcessorService,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.startWithRetry();
  }

  private async startWithRetry(attempt = 1): Promise<void> {
    try {
      await this.kafkaClient.connect();

      const consumer = this.kafkaClient.getConsumer();
      await consumer.subscribe({
        topic: KAFKA_CONSUMER_TOPIC,
        fromBeginning: KAFKA_CONSUMER_FROM_BEGINNING,
      });

      this.running = true;
      this.logger.log(
        `Manual-commit consumer on "${KAFKA_CONSUMER_TOPIC}" (group "${KAFKA_GROUP_ID}", autoCommit=false, fromBeginning=${KAFKA_CONSUMER_FROM_BEGINNING})`,
      );

      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          const value = message.value?.toString() ?? '';
          const currentOffset = message.offset;

          this.logger.log(
            `Received [${topic} p${partition} offset ${currentOffset}]: ${value}`,
          );

          try {
            await this.messageProcessor.process(value);

            const nextOffset = (BigInt(currentOffset) + 1n).toString();
            await consumer.commitOffsets([
              { topic, partition, offset: nextOffset },
            ]);

            this.logger.log(
              `Manual commit OK → next offset ${nextOffset} [p${partition}]`,
            );
          } catch (error) {
            if (error instanceof BusinessMessageError) {
              this.logger.warn(
                `Business failed — offset NOT committed [p${partition} offset ${currentOffset}]. Message will be redelivered.`,
              );
              return;
            }

            this.logger.error(
              `Unexpected error — offset NOT committed [p${partition} offset ${currentOffset}]`,
              error instanceof Error ? error.stack : String(error),
            );
            throw error;
          }
        },
      });
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
