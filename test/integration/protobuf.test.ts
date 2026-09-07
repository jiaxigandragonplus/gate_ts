/**
 * Protobuf codec end to end: a real gate, a real service, real redis, and
 * clients on both codecs at the same time.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import jwt from 'jsonwebtoken';
import protobuf from 'protobufjs';
import WebSocket from 'ws';
import type { Gate } from '../../src/gate';
import { GateClient } from '../../src/client/gateClient';
import { ServiceNode } from '../../src/framework/serviceNode';
import { SUBPROTOCOLS } from '../../src/framework/protocol/codecs';
import { protobufCodec } from '../../src/framework/protocol/protobufCodec';
import { PacketType } from '../../src/framework/protocol/packet';
import {
  JWT_SECRET,
  REDIS_URL,
  flushPrefix,
  sleep,
  startGate,
  transportsUnderTest,
  waitFor,
} from '../helpers/testGate';

const PREFIX = `gate-pbtest-${process.pid}`;
const WS_PORT = 7930;
const ADMIN_PORT = 7931;
const JSON_ONLY_PORT = 7932;
const JSON_ONLY_ADMIN = 7933;
const URL = `ws://127.0.0.1:${WS_PORT}/ws`;
const JSON_ONLY_URL = `ws://127.0.0.1:${JSON_ONLY_PORT}/ws`;

const available = process.env.GATE_TEST_REDIS !== '0';
// Codec behaviour is transport-independent, so this suite pins one: the
// production default (nats) when it is up, redis otherwise. Transport
// equivalence itself is covered by cluster.test.ts, which runs on both.
const TRANSPORT = transportsUnderTest().at(-1) ?? 'redis';
const tokenFor = (uid: string): string =>
  jwt.sign({ sub: uid }, JWT_SECRET, { algorithm: 'HS256', expiresIn: 300 });

/**
 * A game's own message schema - the thing the gate must never need to know
 * about. Used here to prove the payload survives byte-for-byte.
 */
const gameRoot = protobuf.Root.fromJSON({
  nested: {
    demo: {
      nested: {
        Move: {
          fields: {
            dx: { type: 'sint32', id: 1 },
            dy: { type: 'sint32', id: 2 },
            label: { type: 'string', id: 3 },
          },
        },
        Position: {
          fields: {
            x: { type: 'sint32', id: 1 },
            y: { type: 'sint32', id: 2 },
            servedBy: { type: 'string', id: 3 },
          },
        },
      },
    },
  },
});
const Move = gameRoot.lookupType('demo.Move');
const Position = gameRoot.lookupType('demo.Position');

describe.skipIf(!available)('protobuf codec over the wire', () => {
  let gate: Gate;
  let jsonOnlyGate: Gate;
  let game: ServiceNode;
  const clients: GateClient[] = [];

  const client = (uid: string, opts: Record<string, unknown> = {}): GateClient => {
    const c = new GateClient({
      url: URL,
      token: tokenFor(uid),
      autoReconnect: false,
      ...opts,
    });
    clients.push(c);
    return c;
  };

  beforeAll(async () => {
    await flushPrefix(PREFIX);

    gate = await startGate({
      gateId: 'pb-gate',
      wsPort: WS_PORT,
      adminPort: ADMIN_PORT,
      keyPrefix: PREFIX,
      resumeWindowMs: 4_000,
      transport: TRANSPORT,
    });
    // A gate that serves JSON only, to check that a protobuf client is
    // refused rather than quietly handed JSON.
    jsonOnlyGate = await startGate({
      gateId: 'json-only-gate',
      wsPort: JSON_ONLY_PORT,
      adminPort: JSON_ONLY_ADMIN,
      keyPrefix: PREFIX,
      codecs: ['json'],
      defaultCodec: 'json',
      transport: TRANSPORT,
    });

    game = new ServiceNode({
      service: 'game',
      nodeId: 'pb-game-1',
      redisUrl: REDIS_URL,
      keyPrefix: PREFIX,
      transport: TRANSPORT,
      subjectPrefix: PREFIX,
    });
    game
      // Binary in, binary out: the service owns the schema, the gate does not.
      .on('game.move', (ctx) => {
        if (!ctx.payloadBytes) {
          return { echoedJson: ctx.payload, sawBytes: false, servedBy: game.nodeId };
        }
        const move = Move.decode(ctx.payloadBytes) as unknown as { dx: number; dy: number; label: string };
        return Buffer.from(
          Position.encode({ x: move.dx * 2, y: move.dy * 2, servedBy: game.nodeId }).finish(),
        );
      })
      .on('game.shape', (ctx) => ({
        sawBytes: ctx.payloadBytes !== undefined,
        byteLength: ctx.payloadBytes?.length ?? 0,
        json: ctx.payload,
      }))
      .on('game.ping', () => ({ pong: true }));
    await game.start();
  });

  afterAll(async () => {
    for (const c of clients) c.close();
    await game?.stop();
    await Promise.allSettled([gate?.shutdown('test'), jsonOnlyGate?.shutdown('test')]);
    await flushPrefix(PREFIX);
  });

  // ------------------------------------------------------- negotiation ----

  it('negotiates protobuf through the websocket subprotocol', async () => {
    const c = client('pb-auth', { codec: 'protobuf' });
    await c.connect();
    expect(c.codecName).toBe('protobuf');
    expect(c.currentUid).toBe('pb-auth');
    expect(await c.request('gate.whoami')).toMatchObject({ uid: 'pb-auth', gate: 'pb-gate' });
  });

  it('serves json and protobuf clients side by side on one gate', async () => {
    const asJson = client('side-json', { codec: 'json' });
    const asPb = client('side-pb', { codec: 'protobuf' });
    await Promise.all([asJson.connect(), asPb.connect()]);
    expect(asJson.codecName).toBe('json');
    expect(asPb.codecName).toBe('protobuf');
    expect(await asJson.request('game.ping')).toEqual({ pong: true });
    expect(await asPb.request('game.ping')).toEqual({ pong: true });
  });

  it('refuses a protobuf client on a json-only gate', async () => {
    // RFC 6455: a client that requested a subprotocol must fail when the
    // server selects none - so it never silently mis-parses JSON as protobuf.
    const c = new GateClient({
      url: JSON_ONLY_URL,
      token: tokenFor('rejected'),
      autoReconnect: false,
      codec: 'protobuf',
    });
    clients.push(c);
    await expect(c.connect()).rejects.toThrow('Server sent no subprotocol');
  });

  it('rejects the handshake when ?codec asks for something disabled', async () => {
    // No subprotocol here, so only the query parameter can express the
    // choice - and the gate must refuse rather than serve JSON.
    const ws = new WebSocket(`${JSON_ONLY_URL}?codec=pb`);
    const outcome = new Promise<void>((resolve, reject) => {
      ws.on('open', () => resolve());
      ws.on('error', reject);
    });
    await expect(outcome).rejects.toThrow(/400/);
    ws.terminate();
  });

  it('negotiates protobuf from ?codec= when no subprotocol is offered', async () => {
    // Some engines cannot set Sec-WebSocket-Protocol; the query parameter is
    // the escape hatch. The gate must not echo a subprotocol here, since the
    // client never offered one.
    const ws = new WebSocket(`${URL}?codec=pb`);
    await new Promise<void>((resolve, reject) => {
      ws.on('open', () => resolve());
      ws.on('error', reject);
    });
    expect(ws.protocol).toBe('');

    // Speak protobuf and check the gate understood it.
    const frames: Buffer[] = [];
    ws.on('message', (data: WebSocket.RawData) => frames.push(data as Buffer));
    ws.send(protobufCodec.encodeClient({ t: PacketType.Auth, token: tokenFor('query-pb') }));
    await sleep(300);

    expect(frames).toHaveLength(1);
    const ack = protobufCodec.decodeServer(frames[0] as Buffer);
    expect(ack).toMatchObject({ t: PacketType.AuthAck, uid: 'query-pb' });
    ws.close();
  });

  it('does not echo a subprotocol the client never offered', async () => {
    const ws = new WebSocket(`${URL}?codec=pb`, ['some.other.protocol']);
    const outcome = new Promise<void>((resolve, reject) => {
      ws.on('open', () => resolve());
      ws.on('error', reject);
    });
    // "no subprotocol" (not "an invalid subprotocol"): the gate stayed silent
    // instead of echoing gate.pb.v1, which the client never offered. RFC 6455
    // then requires the client to fail - not to assume protobuf was agreed.
    await expect(outcome).rejects.toThrow('Server sent no subprotocol');
    ws.terminate();
  });

  it('advertises its subprotocol name', () => {
    expect(SUBPROTOCOLS.protobuf).toBe('gate.pb.v1');
  });

  // ---------------------------------------------------------- payloads ----

  it('passes an opaque protobuf payload through untouched, both ways', async () => {
    const c = client('pb-payload', { codec: 'protobuf' });
    await c.connect();

    const move = Buffer.from(Move.encode({ dx: 7, dy: -3, label: 'north' }).finish());
    const reply = await c.request<Buffer>('game.move', move);

    // The gate moved bytes; the service and client agree on the schema.
    expect(Buffer.isBuffer(reply)).toBe(true);
    const pos = Position.decode(reply) as unknown as { x: number; y: number; servedBy: string };
    expect({ x: pos.x, y: pos.y, servedBy: pos.servedBy }).toEqual({
      x: 14,
      y: -6,
      servedBy: 'pb-game-1',
    });
  });

  it('lets a protobuf client send a json payload when it wants to', async () => {
    const c = client('pb-jsonpayload', { codec: 'protobuf' });
    await c.connect();
    expect(await c.request('game.shape', { hello: 'world' })).toEqual({
      sawBytes: false,
      byteLength: 0,
      json: { hello: 'world' },
    });
  });

  it('delivers a json service payload to a protobuf client', async () => {
    const c = client('pb-jsonreply', { codec: 'protobuf' });
    await c.connect();
    // game.ping answers with a plain object; the pb codec flags it as JSON.
    expect(await c.request('game.ping')).toEqual({ pong: true });
  });

  it('delivers binary bytes to a json client as base64-flagged payload', async () => {
    const c = client('json-binary', { codec: 'json' });
    await c.connect();
    const move = Buffer.from(Move.encode({ dx: 1, dy: 1, label: 'x' }).finish());
    const reply = await c.request<Buffer>('game.move', move);
    expect(Buffer.isBuffer(reply)).toBe(true);
    const pos = Position.decode(reply) as unknown as { x: number; y: number };
    expect({ x: pos.x, y: pos.y }).toEqual({ x: 2, y: 2 });
  });

  it('pushes to both codecs from one broadcast', async () => {
    const asJson = client('bc-json', { codec: 'json' });
    const asPb = client('bc-pb', { codec: 'protobuf' });
    await Promise.all([asJson.connect(), asPb.connect()]);

    const gotJson = waitFor<[string, unknown]>(asJson, 'push');
    const gotPb = waitFor<[string, unknown]>(asPb, 'push');
    await game.broadcast('sys.notice', { text: 'both of you' });

    expect(await gotJson).toEqual(['sys.notice', { text: 'both of you' }]);
    expect(await gotPb).toEqual(['sys.notice', { text: 'both of you' }]);
  });

  it('pushes opaque bytes to a protobuf client', async () => {
    const c = client('pb-push', { codec: 'protobuf' });
    await c.connect();
    const got = waitFor<[string, unknown]>(c, 'push');
    const bytes = Buffer.from(Position.encode({ x: 5, y: 6, servedBy: 'srv' }).finish());
    expect(await game.pushToUid('pb-push', 'game.state', bytes)).toBe(true);

    const [cmd, payload] = await got;
    expect(cmd).toBe('game.state');
    expect(Buffer.isBuffer(payload)).toBe(true);
    const pos = Position.decode(payload as Buffer) as unknown as { x: number; y: number };
    expect({ x: pos.x, y: pos.y }).toEqual({ x: 5, y: 6 });
  });

  // ---------------------------------------------------------- reconnect ---

  it('resumes a protobuf session and replays over the same codec', async () => {
    const uid = 'pb-resume';
    // Reconnect is driven by hand so the push is guaranteed to land while
    // the session has no socket.
    const c = client(uid, { codec: 'protobuf' });
    await c.connect();

    const pushes: Array<[string, unknown]> = [];
    c.on('push', (cmd: string, payload: unknown) => pushes.push([cmd, payload]));

    c.simulateNetworkDrop();
    await sleep(100);
    const bytes = Buffer.from(Position.encode({ x: 1, y: 2, servedBy: 's' }).finish());
    expect(await game.pushToUid(uid, 'game.offline', bytes)).toBe(true);
    await sleep(100);
    expect(pushes).toEqual([]);

    const ready = waitFor<{ resumed: boolean; replayed: number }>(c, 'ready');
    await c.connect();
    expect(await ready).toMatchObject({ resumed: true, replayed: 1 });

    await sleep(150);
    expect(pushes).toHaveLength(1);
    expect(pushes[0]?.[0]).toBe('game.offline');
    expect(Buffer.isBuffer(pushes[0]?.[1])).toBe(true);
    expect(pushes[0]?.[1]).toEqual(bytes);
  });

  it('kicks a protobuf session on duplicate login', async () => {
    const uid = 'pb-dup';
    const first = client(uid, { codec: 'protobuf' });
    await first.connect();
    const kicked = waitFor<{ reason: string }>(first, 'kick');

    const second = client(uid, { codec: 'json' });
    await second.connect();

    expect((await kicked).reason).toBe('duplicate_login');
    expect(await second.request('gate.whoami')).toMatchObject({ uid });
  });

  // -------------------------------------------------------------- size ----

  it('sends measurably fewer bytes than json for the same traffic', async () => {
    const move = Buffer.from(Move.encode({ dx: 3, dy: 4, label: 'compare' }).finish());

    const measure = async (codec: 'json' | 'protobuf'): Promise<number> => {
      const before = await counters();
      const c = client(`size-${codec}`, { codec });
      await c.connect();
      for (let i = 0; i < 20; i++) await c.request('game.move', move);
      c.close();
      await sleep(100);
      const after = await counters();
      return (after['bytes_out_total'] ?? 0) - (before['bytes_out_total'] ?? 0);
    };

    const jsonBytes = await measure('json');
    const pbBytes = await measure('protobuf');
    expect(pbBytes).toBeLessThan(jsonBytes);
  });

  async function counters(): Promise<Record<string, number>> {
    const res = (await (await fetch(`http://127.0.0.1:${ADMIN_PORT}/stats`)).json()) as {
      counters: Record<string, number>;
    };
    return res.counters;
  }
});
