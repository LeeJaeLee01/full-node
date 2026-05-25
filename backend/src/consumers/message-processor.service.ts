import { Injectable, Logger } from '@nestjs/common';

export class BusinessMessageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BusinessMessageError';
  }
}

@Injectable()
export class MessageProcessorService {
  private readonly logger = new Logger(MessageProcessorService.name);

  /**
   * Mô phỏng xử lý nghiệp vụ (DB, API, v.v.).
   * Message chứa "FAIL" hoặc JSON `{ "fail": true }` → ném lỗi → không commit offset.
   */
  async process(value: string): Promise<void> {
    await this.simulateWork();

    if (value === 'FAIL' || value.includes('"fail":true')) {
      throw new BusinessMessageError(
        'Simulated business logic failure (offset will NOT be committed)',
      );
    }

    try {
      const parsed = JSON.parse(value) as { fail?: boolean; action?: string };
      if (parsed.fail === true) {
        throw new BusinessMessageError(
          'Simulated business logic failure (offset will NOT be committed)',
        );
      }
      this.logger.debug(
        parsed.action
          ? `Processed action="${parsed.action}"`
          : 'Processed plain message',
      );
    } catch (error) {
      if (error instanceof BusinessMessageError) {
        throw error;
      }
      this.logger.debug('Processed plain (non-JSON) message');
    }
  }

  private simulateWork(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, 80));
  }
}
