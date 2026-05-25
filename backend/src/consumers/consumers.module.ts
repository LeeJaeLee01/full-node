import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { KafkaConsumerService } from './kafka-consumer.service';
import { MessageProcessorService } from './message-processor.service';

@Module({
  imports: [KafkaModule],
  providers: [KafkaConsumerService, MessageProcessorService],
})
export class ConsumersModule {}
