import { Module } from '@nestjs/common';
import { KafkaClient } from './kafka.client';

@Module({
  providers: [KafkaClient],
  exports: [KafkaClient],
})
export class KafkaModule {}
