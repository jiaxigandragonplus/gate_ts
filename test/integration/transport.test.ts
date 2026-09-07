/**
 * Transport-level behaviour that only shows up when redis and nats are both
 * in play.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import jwt from 'jsonwebtoken';
import type { Gate } from '../../src/gate';
import { GateClient } from '../../src/client/gateClient';
import { ServiceNode } from '../../src/framework/serviceNode';
import { ErrorCode } from '../../src/framework/protocol/packet';
import {
  JWT_SECRET,
  REDIS_URL,
  flushPrefix,
  sleep,
  startGate,
  transportsUnderTest,
} from '../helpers/testGate';

const PREFIX = `gate-tp-${process.pid}`;
const WS_PORT = 7940;
const ADMIN_PORT = 7941;
const URL = `ws://127.0.0.1:${WS_PORT}/ws`;

const transports = transportsUnderTest();
const bothAvailable = transports.includes('redis') && transports.includes('nats');

const tokenFor = (uid: string): string =>
  jwt.sign({ sub: uid }, JWT_SECRET, { algorithm: 'HS256', expiresIn: 300 });

describe.skipIf(!bothAvailable)('cluster transport', () => {
  let gate: Gate;
  let mismatched: ServiceNode;
  let matched: ServiceNode;
  const clients: GateClient[] = [];

  beforeAll(async () => {
    await flushPrefix(PREFIX);
    gate = await startGate({
      gateId: 'tp-gate',
      wsPort: WS_PORT,
      adminPort: ADMIN_PORT,
      keyPrefix: PREFIX,
      transport: 'nats',
      requestTimeoutMs: 8_000,
    });

    // Registers and heartbeats normally, but listens on the other transport:
    // without the guard this looks perfectly healthy to the gate.
    mismatched = new ServiceNode({
      service: 'game',
      nodeId: 'game-on-redis',
      redisUrl: REDIS_URL,
      keyPrefix: PREFIX,
      transport: 'redis',
    });
    mismatched.on('game.ping', () => ({ pong: true }));
    await mismatched.start();

    matched = new ServiceNode({
      service: 'chat',
      nodeId: 'chat-on-nats',
      redisUrl: REDIS_URL,
      keyPrefix: PREFIX,
      transport: 'nats',
      subjectPrefix: PREFIX,
    });
    matched.on('chat.ping', () => ({ pong: true }));
    await matched.start();
  });

  afterAll(async () => {
    for (const c of clients) c.close();
    await Promise.allSettled([mismatched?.stop(), matched?.stop()]);
    await gate?.shutdown('test');
    await flushPrefix(PREFIX);
  });

  const client = async (uid: string): Promise<GateClient> => {
    const c = new GateClient({ url: URL, token: tokenFor(uid), autoReconnect: false });
    clients.push(c);
    await c.connect();
    return c;
  };

  it('fails fast instead of timing out when a service is on another transport', async () => {
    const c = await client('mismatch');
    const started = Date.now();

    // The request timeout for this gate is 8s; a correct diagnosis must come
    // back far sooner than that, and must say "unavailable" not "timeout".
    await expect(c.request('game.ping')).rejects.toMatchObject({
      code: ErrorCode.ServiceUnavailable,
    });
    expect(Date.now() - started).toBeLessThan(2_000);
  });

  it('still routes to services on the matching transport', async () => {
    const c = await client('matched');
    expect(await c.request('chat.ping')).toEqual({ pong: true });
  });

  it('reports the transport and its counters for ops', async () => {
    const c = await client('counters');
    await c.request('chat.ping');
    await sleep(100);

    const stats = (await (await fetch(`http://127.0.0.1:${ADMIN_PORT}/stats`)).json()) as {
      transport: string;
      transportCounters: Record<string, number> | null;
    };
    expect(stats.transport).toBe('nats');
    expect(stats.transportCounters).toMatchObject({ connected: 1 });
    // slow_consumers is the metric to alert on; it must exist and be quiet.
    expect(stats.transportCounters?.['slow_consumers']).toBe(0);
    expect(stats.transportCounters?.['out_msgs']).toBeGreaterThan(0);

    const metrics = await (await fetch(`http://127.0.0.1:${ADMIN_PORT}/metrics`)).text();
    expect(metrics).toContain('gate_transport_slow_consumers');
    expect(metrics).toContain('gate_transport_reconnects');
  });
});
