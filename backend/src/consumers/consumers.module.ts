import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { KafkaConsumerService } from './kafka-consumer.service';

@Module({
  imports: [KafkaModule],
  providers: [KafkaConsumerService],
})
export class ConsumersModule {}
