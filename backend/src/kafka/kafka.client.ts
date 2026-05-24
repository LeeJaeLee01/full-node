import {
  Injectable,
  Logger,
  OnModuleDestroy,
} from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import {
  KAFKA_BROKERS,
  KAFKA_CLIENT_ID,
  KAFKA_GROUP_ID,
} from './kafka.constants';

@Injectable()
export class KafkaClient implements OnModuleDestroy {
  private readonly logger = new Logger(KafkaClient.name);
  private readonly kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: KAFKA_BROKERS,
  });
  private readonly consumer: Consumer = this.kafka.consumer({
    groupId: KAFKA_GROUP_ID,
  });
  private connected = false;

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    await this.consumer.connect();
    this.connected = true;
    this.logger.log(
      `Kafka connected (${KAFKA_BROKERS.join(', ')}, group=${KAFKA_GROUP_ID})`,
    );
  }

  getConsumer(): Consumer {
    return this.consumer;
  }

  async disconnect(): Promise<void> {
    if (!this.connected) {
      return;
    }

    await this.consumer.disconnect();
    this.connected = false;
    this.logger.log('Kafka disconnected');
  }

  async onModuleDestroy(): Promise<void> {
    await this.disconnect();
  }
}
