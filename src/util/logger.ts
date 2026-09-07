import pino, { type Logger } from 'pino';

const level = process.env.LOG_LEVEL ?? 'info';
const pretty = process.env.LOG_PRETTY === '1' || process.env.NODE_ENV === 'development';

export const logger: Logger = pino({
  level,
  base: { pid: process.pid },
  ...(pretty
    ? {
        transport: {
          target: 'pino-pretty',
          options: { colorize: true, translateTime: 'SYS:HH:MM:ss.l', ignore: 'pid,hostname' },
        },
      }
    : {}),
});

export function childLogger(bindings: Record<string, unknown>): Logger {
  return logger.child(bindings);
}
