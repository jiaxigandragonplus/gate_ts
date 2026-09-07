/**
 * Cluster behaviour, exercised against a real redis with two gate processes
 * running in-process. Skipped automatically when redis is unreachable.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import jwt from 'jsonwebtoken';
import type { Gate } from '../../src/gate';
import { GateClient, RequestError } from '../../src/sdk/gateClient';
import { ServiceNode } from '../../src/sdk/serviceNode';
import { CloseCode, ErrorCode, KICK_REASON } from '../../src/protocol/packet';
import {
  JWT_SECRET,
  REDIS_URL,
  flushPrefix,
  sleep,
  startGate,
  waitFor,
} from '../helpers/testGate';

const PREFIX = `gate-test-${process.pid}`;
const GATE_A_PORT = 7910;
const GATE_B_PORT = 7911;
const ADMIN_A_PORT = 7920;
const ADMIN_B_PORT = 7921;
const ADMIN_TOKEN = 'test-admin-token';
const URL_A = `ws://127.0.0.1:${GATE_A_PORT}/ws`;
const URL_B = `ws://127.0.0.1:${GATE_B_PORT}/ws`;

// Probed once in test/helpers/globalSetup.ts.
const available = process.env.GATE_TEST_REDIS !== '0';

const tokenFor = (uid: string, ttl = 300): string =>
  jwt.sign({ sub: uid }, JWT_SECRET, { algorithm: 'HS256', expiresIn: ttl });

describe.skipIf(!available)('gate cluster', () => {
  let gateA: Gate;
  let gateB: Gate;
  let game: ServiceNode;
  let chat: ServiceNode;
  const clients: GateClient[] = [];

  const client = (uid: string, url = URL_A, opts: Record<string, unknown> = {}): GateClient => {
    const c = new GateClient({ url, token: tokenFor(uid), autoReconnect: false, ...opts });
    clients.push(c);
    return c;
  };

  beforeAll(async () => {
    await flushPrefix(PREFIX);

    gateA = await startGate({
      gateId: 'gate-a',
      wsPort: GATE_A_PORT,
      adminPort: ADMIN_A_PORT,
      keyPrefix: PREFIX,
      adminToken: ADMIN_TOKEN,
      resumeWindowMs: 4_000,
      replayBufferSize: 8,
      requestTimeoutMs: 1_500,
    });
    gateB = await startGate({
      gateId: 'gate-b',
      wsPort: GATE_B_PORT,
      adminPort: ADMIN_B_PORT,
      keyPrefix: PREFIX,
      adminToken: ADMIN_TOKEN,
      resumeWindowMs: 4_000,
      replayBufferSize: 8,
      requestTimeoutMs: 1_500,
    });

    game = new ServiceNode({ service: 'game', nodeId: 'game-1', redisUrl: REDIS_URL, keyPrefix: PREFIX });
    game
      .on('game.echo', (ctx) => ({ echo: ctx.payload, uid: ctx.uid, servedBy: game.nodeId }))
      .on('game.count', (() => {
        // Counts how many times the service actually executed a command, so
        // tests can prove a resent packet is not applied twice.
        const counters = new Map<string, number>();
        return (ctx: { uid: string }) => {
          const n = (counters.get(ctx.uid) ?? 0) + 1;
          counters.set(ctx.uid, n);
          return { count: n };
        };
      })())
      .on('game.slow', async () => {
        await sleep(5_000);
        return { late: true };
      })
      .on('game.boom', () => {
        throw new Error('handler exploded');
      });
    await game.start();

    chat = new ServiceNode({ service: 'chat', nodeId: 'chat-1', redisUrl: REDIS_URL, keyPrefix: PREFIX });
    chat.on('chat.send', (ctx) => ({ sent: true, from: ctx.uid, payload: ctx.payload }));
    await chat.start();
  });

  afterAll(async () => {
    for (const c of clients) c.close();
    await Promise.allSettled([game?.stop(), chat?.stop()]);
    await Promise.allSettled([gateA?.shutdown('test'), gateB?.shutdown('test')]);
    await flushPrefix(PREFIX);
  });

  // ------------------------------------------------------------- auth -----

  it('authenticates with a valid jwt and reports session identity', async () => {
    const c = client('auth-ok');
    await c.connect();
    expect(c.currentUid).toBe('auth-ok');
    const who = await c.request<{ uid: string; gate: string }>('gate.whoami');
    expect(who).toMatchObject({ uid: 'auth-ok', gate: 'gate-a' });
  });

  it('closes the socket on an invalid jwt', async () => {
    const c = new GateClient({ url: URL_A, token: 'not-a-jwt', autoReconnect: false });
    clients.push(c);
    const closed = waitFor<[number, string]>(c, 'close');
    await expect(c.connect()).rejects.toThrow();
    const [code] = await closed;
    expect(code).toBe(CloseCode.AuthFailed);
  });

  it('closes the socket on an expired jwt', async () => {
    const c = new GateClient({
      url: URL_A,
      token: jwt.sign({ sub: 'stale' }, JWT_SECRET, { algorithm: 'HS256', expiresIn: -60 }),
      autoReconnect: false,
    });
    clients.push(c);
    const closed = waitFor<[number, string]>(c, 'close');
    await expect(c.connect()).rejects.toThrow();
    expect((await closed)[0]).toBe(CloseCode.AuthFailed);
  });

  // ---------------------------------------------------------- routing -----

  it('routes each command to the service that owns it', async () => {
    const c = client('router');
    await c.connect();
    expect(await c.request('game.echo', { v: 1 })).toMatchObject({ servedBy: 'game-1' });
    expect(await c.request('chat.send', { text: 'hi' })).toMatchObject({ sent: true, from: 'router' });
  });

  it('answers gate-internal commands without a backend', async () => {
    const c = client('internal');
    await c.connect();
    const res = await c.request<{ ts: number }>('gate.ping');
    expect(res.ts).toBeGreaterThan(0);
  });

  it('reports an unroutable command instead of hanging', async () => {
    const c = client('no-route');
    await c.connect();
    await expect(c.request('mail.list')).rejects.toMatchObject({ code: ErrorCode.RouteNotFound });
  });

  it('reports a service with no live nodes as unavailable', async () => {
    const c = client('no-service');
    await c.connect();
    await expect(c.request('ghost.poke')).rejects.toMatchObject({
      code: ErrorCode.ServiceUnavailable,
    });
  });

  it('surfaces a service-side exception as an error response', async () => {
    const c = client('boom');
    await c.connect();
    await expect(c.request('game.boom')).rejects.toMatchObject({ code: ErrorCode.Internal });
  });

  it('times out an upstream request that never gets answered', async () => {
    const c = client('slow');
    await c.connect();
    await expect(c.request('game.slow', {}, 4_000)).rejects.toMatchObject({
      code: ErrorCode.ServiceTimeout,
    });
  });

  // ------------------------------------------------- duplicate login ------

  it('kicks the existing session when the same account logs in on another gate', async () => {
    const uid = 'dup-cross';
    const first = client(uid, URL_A);
    await first.connect();
    await first.request('gate.whoami');

    const kicked = waitFor<{ reason: string }>(first, 'kick');
    const closed = waitFor<[number, string]>(first, 'close');

    const second = client(uid, URL_B);
    await second.connect();

    expect((await kicked).reason).toBe(KICK_REASON.DuplicateLogin);
    expect((await closed)[0]).toBe(CloseCode.KickedDuplicateLogin);

    // The newcomer keeps working, and owns the account in redis.
    expect(await second.request<{ gate: string }>('gate.whoami')).toMatchObject({ gate: 'gate-b' });
    expect(await game.ownerOf(uid)).toMatchObject({ gate: 'gate-b' });
  });

  it('kicks the existing session when the same account logs in on the same gate', async () => {
    const uid = 'dup-local';
    const first = client(uid, URL_A);
    await first.connect();
    const kicked = waitFor<{ reason: string }>(first, 'kick');

    const second = client(uid, URL_A);
    await second.connect();

    expect((await kicked).reason).toBe(KICK_REASON.DuplicateLogin);
    expect(await second.request('gate.whoami')).toMatchObject({ uid });
  });

  // --------------------------------------------------------- reconnect ----

  it('replays missed pushes after a reconnect to the same gate', async () => {
    const uid = 'resume-replay';
    // Reconnect is driven by hand here so the pushes are guaranteed to happen
    // while the session has no socket.
    const c = client(uid, URL_A);
    await c.connect();

    const pushes: Array<{ cmd: string; payload: unknown }> = [];
    c.on('push', (cmd: string, payload: unknown) => pushes.push({ cmd, payload }));

    c.simulateNetworkDrop();
    await sleep(100);
    expect(await game.pushToUid(uid, 'game.offline1', { n: 1 })).toBe(true);
    expect(await game.pushToUid(uid, 'game.offline2', { n: 2 })).toBe(true);
    await sleep(100);
    expect(pushes).toEqual([]);

    const ready = waitFor<{ resumed: boolean; resync: boolean; replayed: number }>(c, 'ready');
    await c.connect();
    const info = await ready;
    expect(info).toMatchObject({ resumed: true, resync: false, replayed: 2 });

    await sleep(150);
    expect(pushes).toEqual([
      { cmd: 'game.offline1', payload: { n: 1 } },
      { cmd: 'game.offline2', payload: { n: 2 } },
    ]);
  });

  it('does not execute a request twice when it is resent after a drop', async () => {
    const uid = 'resume-dedup';
    const c = client(uid, URL_A, { autoReconnect: true });
    await c.connect();

    expect(await c.request<{ count: number }>('game.count')).toEqual({ count: 1 });

    // Fire a request and kill the socket before the reply can land: the gate
    // has already forwarded it, so the resent copy must be ignored and the
    // buffered reply replayed instead.
    const inflight = c.request<{ count: number }>('game.count');
    c.simulateNetworkDrop();
    expect(await inflight).toEqual({ count: 2 });

    expect(await c.request<{ count: number }>('game.count')).toEqual({ count: 3 });
  });

  it('resumes on a different gate, telling the client to resync', async () => {
    const uid = 'resume-migrate';
    const c = client(uid, URL_A, { autoReconnect: true });
    await c.connect();
    expect(await c.request<{ gate: string }>('gate.whoami')).toMatchObject({ gate: 'gate-a' });

    // Simulate the load balancer sending the reconnect to the other gate.
    c.setUrl(URL_B);
    const ready = waitFor<{ resumed: boolean; resync: boolean }>(c, 'ready');
    c.simulateNetworkDrop();

    expect(await ready).toMatchObject({ resumed: true, resync: true });
    expect(await c.request<{ gate: string }>('gate.whoami')).toMatchObject({ gate: 'gate-b' });
    // Ownership followed the session.
    expect(await game.ownerOf(uid)).toMatchObject({ gate: 'gate-b' });
  });

  it('refuses a resume with the wrong token', async () => {
    const uid = 'resume-forged';
    const c = client(uid, URL_A);
    await c.connect();
    const sid = c.sessionId as string;

    const forged = new GateClient({ url: URL_A, token: tokenFor('someone-else'), autoReconnect: false });
    clients.push(forged);
    // Reach into the client to replay a stolen session id with a bad secret.
    (forged as unknown as { sid: string; resumeToken: string }).sid = sid;
    (forged as unknown as { sid: string; resumeToken: string }).resumeToken = 'forged-secret';

    const closed = waitFor<[number, string]>(forged, 'close');
    await expect(forged.connect()).rejects.toThrow();
    expect((await closed)[0]).toBe(CloseCode.ResumeFailed);

    // The real session is untouched.
    expect(await c.request('gate.whoami')).toMatchObject({ uid });
  });

  it('drops the session once the resume window closes', async () => {
    const uid = 'resume-expired';
    const c = client(uid, URL_A);
    await c.connect();
    const sid = c.sessionId as string;
    c.simulateNetworkDrop();

    // resumeWindowMs is 4s for these gates.
    await sleep(4_600);
    expect(await game.ownerOf(uid)).toBeNull();

    const late = new GateClient({ url: URL_A, token: tokenFor(uid), autoReconnect: false });
    clients.push(late);
    (late as unknown as { sid: string; resumeToken: string }).sid = sid;
    (late as unknown as { sid: string; resumeToken: string }).resumeToken = 'whatever';
    const closed = waitFor<[number, string]>(late, 'close');
    await expect(late.connect()).rejects.toThrow();
    expect((await closed)[0]).toBe(CloseCode.ResumeFailed);
  });

  // ------------------------------------------------------ service push ----

  it('delivers a targeted push to whichever gate owns the session', async () => {
    const onA = client('push-a', URL_A);
    const onB = client('push-b', URL_B);
    await Promise.all([onA.connect(), onB.connect()]);

    const gotA = waitFor<[string, unknown]>(onA, 'push');
    const gotB = waitFor<[string, unknown]>(onB, 'push');
    expect(await chat.pushToUid('push-a', 'chat.dm', { text: 'for a' })).toBe(true);
    expect(await chat.pushToUid('push-b', 'chat.dm', { text: 'for b' })).toBe(true);

    expect(await gotA).toEqual(['chat.dm', { text: 'for a' }]);
    expect(await gotB).toEqual(['chat.dm', { text: 'for b' }]);
  });

  it('reports a push to an offline account as undelivered', async () => {
    expect(await chat.pushToUid('nobody-here', 'chat.dm', {})).toBe(false);
  });

  it('broadcasts to sessions on every gate', async () => {
    const onA = client('bcast-a', URL_A);
    const onB = client('bcast-b', URL_B);
    await Promise.all([onA.connect(), onB.connect()]);

    const gotA = waitFor<[string, unknown]>(onA, 'push');
    const gotB = waitFor<[string, unknown]>(onB, 'push');
    await chat.broadcast('chat.notice', { text: 'server restarting' });

    expect(await gotA).toEqual(['chat.notice', { text: 'server restarting' }]);
    expect(await gotB).toEqual(['chat.notice', { text: 'server restarting' }]);
  });

  it('multicasts to a list of accounts across gates', async () => {
    const onA = client('multi-a', URL_A);
    const onB = client('multi-b', URL_B);
    const other = client('multi-c', URL_A);
    await Promise.all([onA.connect(), onB.connect(), other.connect()]);

    const received: string[] = [];
    other.on('push', () => received.push('multi-c'));
    const gotA = waitFor<[string, unknown]>(onA, 'push');
    const gotB = waitFor<[string, unknown]>(onB, 'push');

    await chat.multicast(['multi-a', 'multi-b'], 'chat.guild', { text: 'raid time' });
    expect(await gotA).toEqual(['chat.guild', { text: 'raid time' }]);
    expect(await gotB).toEqual(['chat.guild', { text: 'raid time' }]);
    expect(received).toEqual([]);
  });

  it('lets a service kick a player on another gate', async () => {
    const uid = 'service-kick';
    const c = client(uid, URL_B);
    await c.connect();
    const kicked = waitFor<{ reason: string }>(c, 'kick');
    expect(await game.kick(uid, 'admin', 'cheating')).toBe(true);
    expect((await kicked).reason).toBe('admin');
    await sleep(100);
    expect(await game.ownerOf(uid)).toBeNull();
  });

  // ------------------------------------------------------------- admin ----

  it('kicks a player through the admin api of any gate', async () => {
    const uid = 'admin-kick';
    const c = client(uid, URL_A);
    await c.connect();
    const kicked = waitFor<{ reason: string }>(c, 'kick');

    // Ask gate B to kick a session that lives on gate A.
    const res = await fetch(`http://127.0.0.1:${ADMIN_B_PORT}/admin/kick`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${ADMIN_TOKEN}` },
      body: JSON.stringify({ uid, reason: 'admin' }),
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ kicked: true });
    expect((await kicked).reason).toBe('admin');
  });

  it('rejects admin calls without the token', async () => {
    const res = await fetch(`http://127.0.0.1:${ADMIN_A_PORT}/admin/kick`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ uid: 'whoever' }),
    });
    expect(res.status).toBe(401);
  });

  it('lets a client resume elsewhere when its gate shuts down', async () => {
    // A rolling restart must not log players out: the gate leaves the resume
    // ticket in redis and the client re-establishes on another node.
    const gateC = await startGate({
      gateId: 'gate-c',
      wsPort: 7912,
      adminPort: 7922,
      keyPrefix: PREFIX,
      resumeWindowMs: 4_000,
    });

    const uid = 'rolling-restart';
    const c = client(uid, 'ws://127.0.0.1:7912/ws', { autoReconnect: true });
    await c.connect();
    expect(await c.request<{ gate: string }>('gate.whoami')).toMatchObject({ gate: 'gate-c' });

    const kicked = waitFor<{ reason: string; resumable?: boolean }>(c, 'kick');
    const ready = waitFor<{ resumed: boolean; resync: boolean }>(c, 'ready', 10_000);
    c.setUrl(URL_A);
    await gateC.shutdown('rolling restart');

    expect(await kicked).toMatchObject({ reason: KICK_REASON.Shutdown, resumable: true });
    expect(await ready).toMatchObject({ resumed: true, resync: true });
    expect(await c.request<{ gate: string }>('gate.whoami')).toMatchObject({ gate: 'gate-a' });
  });

  it('exposes health and metrics without a token', async () => {
    const health = await fetch(`http://127.0.0.1:${ADMIN_A_PORT}/healthz`);
    expect(health.status).toBe(200);

    const ready = await fetch(`http://127.0.0.1:${ADMIN_A_PORT}/readyz`);
    expect(ready.status).toBe(200);

    const metrics = await fetch(`http://127.0.0.1:${ADMIN_A_PORT}/metrics`);
    const body = await metrics.text();
    expect(body).toContain('gate_sessions_online{gate="gate-a"}');
    expect(body).toContain('gate_auth_ok_total');

    const stats = await (await fetch(`http://127.0.0.1:${ADMIN_A_PORT}/stats`)).json();
    expect(stats).toMatchObject({ gate: 'gate-a' });
  });

  it('actually moves its counters', async () => {
    const c = client('counters');
    await c.connect();
    await c.request('game.echo', { v: 1 });

    const counters = (await (await fetch(`http://127.0.0.1:${ADMIN_A_PORT}/stats`)).json()) as {
      counters: Record<string, number>;
    };
    // Transport counters live in the net layer and were easy to leave at zero.
    expect(counters.counters.connections_accepted_total).toBeGreaterThan(0);
    expect(counters.counters.connections_closed_total).toBeGreaterThan(0);
    expect(counters.counters.packets_in_total).toBeGreaterThan(0);
    expect(counters.counters.packets_out_total).toBeGreaterThan(0);
    expect(counters.counters.bytes_in_total).toBeGreaterThan(0);
    expect(counters.counters.bytes_out_total).toBeGreaterThan(0);
    expect(counters.counters.protocol_errors_total).toBe(0);
    // Gate-level counters.
    expect(counters.counters.auth_ok_total).toBeGreaterThan(0);
    expect(counters.counters.auth_failed_total).toBeGreaterThan(0);
    expect(counters.counters.upstream_sent_total).toBeGreaterThan(0);
    expect(counters.counters.downstream_responses_total).toBeGreaterThan(0);
    expect(counters.counters.route_misses_total).toBeGreaterThan(0);
    expect(counters.counters.kicks_duplicate_login_total).toBeGreaterThan(0);
    expect(counters.counters.upstream_latency_count).toBeGreaterThan(0);
  });
});
