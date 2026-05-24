import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ConsumersModule } from './consumers.module';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(ConsumersModule, {
    logger: ['log', 'error', 'warn'],
  });

  const logger = new Logger('ConsumersService');
  logger.log('Kafka consumer service is running');

  const shutdown = async (signal: string) => {
    logger.log(`Received ${signal}, shutting down consumer...`);
    await app.close();
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

bootstrap().catch((error: unknown) => {
  console.error('Consumer service failed to start:', error);
  process.exit(1);
});
