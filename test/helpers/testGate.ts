import Redis from 'ioredis';
import { Gate } from '../../src/gate';
import type { GateConfig } from '../../src/gate/config';
import type { RouteRule } from '../../src/gate/router/routeTable';
import type { CodecName } from '../../src/framework/protocol/codec';

export const REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379';
export const JWT_SECRET = 'integration-test-secret';

export const TEST_ROUTES: RouteRule[] = [
  { prefix: 'game.', service: 'game' },
  { prefix: 'chat.', service: 'chat' },
  { prefix: 'ghost.', service: 'ghost' },
];

export interface TestGateOptions {
  gateId: string;
  wsPort: number;
  adminPort: number;
  keyPrefix: string;
  resumeWindowMs?: number;
  replayBufferSize?: number;
  crossGateResume?: boolean;
  requestTimeoutMs?: number;
  adminToken?: string;
  msgsPerSec?: number;
  codecs?: CodecName[];
  defaultCodec?: CodecName;
}

export function testConfig(o: TestGateOptions): GateConfig {
  return {
    gateId: o.gateId,
    advertiseAddr: `127.0.0.1:${o.wsPort}`,
    env: 'test',
    ws: {
      host: '127.0.0.1',
      port: o.wsPort,
      path: '/ws',
      maxPayloadBytes: 64 * 1024,
      pingIntervalMs: 60_000,
      pongTimeoutMs: 120_000,
      authTimeoutMs: 5_000,
      maxBackpressureBytes: 1024 * 1024,
      perMessageDeflate: false,
      maxConnections: 0,
      trustProxy: false,
      codecs: o.codecs ?? ['json', 'protobuf'],
      defaultCodec: o.defaultCodec ?? 'json',
    },
    session: {
      resumeWindowMs: o.resumeWindowMs ?? 5_000,
      replayBufferSize: o.replayBufferSize ?? 32,
      registryTtlMs: 30_000,
      registryRefreshMs: 60_000,
      crossGateResume: o.crossGateResume ?? true,
    },
    limits: { msgsPerSec: o.msgsPerSec ?? 200, burst: o.msgsPerSec ?? 400 },
    redis: { url: REDIS_URL, keyPrefix: o.keyPrefix },
    cluster: { heartbeatMs: 60_000, nodeTtlMs: 30_000, notifySessionEvents: true },
    backend: { requestTimeoutMs: o.requestTimeoutMs ?? 3_000, stickyTtlMs: 60_000 },
    routes: TEST_ROUTES,
    admin: {
      host: '127.0.0.1',
      port: o.adminPort,
      ...(o.adminToken === undefined ? {} : { token: o.adminToken }),
    },
    jwt: {
      secret: JWT_SECRET,
      algorithms: ['HS256'],
      uidClaim: 'sub',
      clockToleranceSec: 5,
    },
    shutdownGraceMs: 1_000,
  };
}

export async function startGate(o: TestGateOptions): Promise<Gate> {
  const gate = new Gate(testConfig(o));
  await gate.start();
  return gate;
}

export async function redisAvailable(): Promise<boolean> {
  const client = new Redis(REDIS_URL, {
    lazyConnect: true,
    maxRetriesPerRequest: 0,
    retryStrategy: () => null,
    connectTimeout: 1500,
  });
  try {
    await client.connect();
    await client.ping();
    return true;
  } catch {
    return false;
  } finally {
    client.disconnect();
  }
}

/** Remove every key this test run created. */
export async function flushPrefix(prefix: string): Promise<void> {
  const client = new Redis(REDIS_URL);
  try {
    let cursor = '0';
    do {
      const [next, keys] = await client.scan(cursor, 'MATCH', `${prefix}:*`, 'COUNT', 500);
      cursor = next;
      if (keys.length > 0) await client.del(...keys);
    } while (cursor !== '0');
  } finally {
    client.disconnect();
  }
}

/** Resolve on the next matching event, or reject after `ms`. */
export function waitFor<T = unknown>(
  emitter: { once: (ev: string, fn: (...args: any[]) => void) => unknown; off?: (ev: string, fn: (...args: any[]) => void) => unknown },
  event: string,
  ms = 5000,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for "${event}"`)), ms);
    emitter.once(event, (...args: unknown[]) => {
      clearTimeout(timer);
      resolve((args.length > 1 ? args : args[0]) as T);
    });
  });
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
