import type { Writable } from 'node:stream';

export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

const LEVEL_RANK: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
  silent: 100,
};

export interface LoggerOptions {
  level?: LogLevel;
  stream?: Writable;
}

export class StructuredLogger {
  private readonly minRank: number;
  private readonly stream: Writable;

  constructor(options?: LoggerOptions) {
    const level = options?.level ?? 'silent';
    this.minRank = LEVEL_RANK[level];
    this.stream = options?.stream ?? process.stdout;
  }

  info(event: string, data?: Record<string, unknown>): void {
    this.write('info', event, data);
  }

  warn(event: string, data?: Record<string, unknown>): void {
    this.write('warn', event, data);
  }

  error(event: string, data?: Record<string, unknown>): void {
    this.write('error', event, data);
  }

  private write(
    level: 'debug' | 'info' | 'warn' | 'error',
    event: string,
    data?: Record<string, unknown>,
  ): void {
    if (LEVEL_RANK[level] < this.minRank) {
      return;
    }

    const entry = JSON.stringify({
      ts: new Date().toISOString(),
      level,
      event,
      ...data,
    });
    this.stream.write(`${entry}\n`);
  }
}
