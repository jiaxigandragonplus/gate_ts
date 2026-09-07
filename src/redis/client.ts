import Redis, { type RedisOptions } from 'ioredis';
import { logger } from '../util/logger';

export interface RedisFactoryOptions {
  url: string;
  /** Label used in logs, e.g. "cmd" / "sub". */
  role: string;
}

/**
 * ioredis needs a dedicated connection for subscribe mode, so connections are
 * created explicitly rather than shared.
 */
export function createRedis({ url, role }: RedisFactoryOptions): Redis {
  const options: RedisOptions = {
    lazyConnect: true,
    maxRetriesPerRequest: role === 'sub' ? null : 3,
    enableReadyCheck: true,
    retryStrategy: (times) => Math.min(times * 200, 3000),
    reconnectOnError: (err) => err.message.includes('READONLY'),
  };
  const client = new Redis(url, options);
  const log = logger.child({ mod: 'redis', role });
  client.on('error', (err: Error) => log.warn({ err: err.message }, 'redis error'));
  client.on('ready', () => log.info('redis ready'));
  client.on('reconnecting', () => log.warn('redis reconnecting'));
  client.on('end', () => log.warn('redis connection closed'));
  return client;
}

export class RedisPool {
  readonly cmd: Redis;
  readonly sub: Redis;
  /** Publishing must not share the subscriber connection. */
  readonly pub: Redis;

  constructor(url: string) {
    this.cmd = createRedis({ url, role: 'cmd' });
    this.sub = createRedis({ url, role: 'sub' });
    this.pub = createRedis({ url, role: 'pub' });
  }

  async connect(): Promise<void> {
    await Promise.all([this.cmd.connect(), this.sub.connect(), this.pub.connect()]);
  }

  async close(): Promise<void> {
    await Promise.allSettled([this.cmd.quit(), this.sub.quit(), this.pub.quit()]);
  }
}
