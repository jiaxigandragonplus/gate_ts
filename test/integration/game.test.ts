/**
 * The game node end to end: a real gate, a real game node with real systems,
 * a real client, and real redis. Business flows, error paths, per-player
 * concurrency and persistence.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import jwt from 'jsonwebtoken';
import type { Gate } from '../../src/gate';
import { GateClient } from '../../src/client/gateClient';
import { GameNode } from '../../src/game/gameNode';
import { GameSystem, type SystemHandler } from '../../src/game/system';
import { MemoryPlayerStore } from '../../src/game/store';
import { ErrorCode } from '../../src/framework/protocol/packet';
import {
  JWT_SECRET,
  flushPrefix,
  sleep,
  startGate,
  testGameConfig,
  transportsUnderTest,
  waitFor,
} from '../helpers/testGate';

const PREFIX = `gate-game-${process.pid}`;
const WS_PORT = 7950;
const ADMIN_PORT = 7951;
const GAME_ADMIN_PORT = 7952;
const URL = `ws://127.0.0.1:${WS_PORT}/ws`;

const TRANSPORT = transportsUnderTest().at(-1);
const tokenFor = (uid: string): string =>
  jwt.sign({ sub: uid }, JWT_SECRET, { algorithm: 'HS256', expiresIn: 300 });

/**
 * Test-only system with an await between reading and writing its state.
 *
 * The shipped systems mutate synchronously, so they would look correct even
 * without the mailbox. This one only gives the right answer if messages for
 * one player really are serialised.
 */
class RaceSystem extends GameSystem<{ n: number }> {
  readonly name = 'race';

  override createState(): { n: number } {
    return { n: 0 };
  }

  override handlers(): Record<string, SystemHandler<{ n: number }>> {
    return {
      bump: async (ctx) => {
        const seen = ctx.state.n;
        await sleep(5);
        ctx.state.n = seen + 1;
        this.touch(ctx.player);
        return { n: ctx.state.n };
      },
      get: (ctx) => ({ n: ctx.state.n }),
    };
  }
}

describe.skipIf(!TRANSPORT)('game node', () => {
  let gate: Gate;
  let game: GameNode;
  const store = new MemoryPlayerStore();
  const clients: GateClient[] = [];

  const client = async (uid: string): Promise<GateClient> => {
    const c = new GateClient({ url: URL, token: tokenFor(uid), autoReconnect: false });
    clients.push(c);
    await c.connect();
    return c;
  };

  beforeAll(async () => {
    await flushPrefix(PREFIX);
    gate = await startGate({
      gateId: 'game-gate',
      wsPort: WS_PORT,
      adminPort: ADMIN_PORT,
      keyPrefix: PREFIX,
      transport: TRANSPORT,
      // Short resume window so the offline event (and the unload it triggers)
      // arrives inside a test.
      resumeWindowMs: 600,
      requestTimeoutMs: 5_000,
    });

    const { ProfileSystem } = await import('../../src/game/systems/profile');
    const { BagSystem } = await import('../../src/game/systems/bag');
    const { QuestSystem } = await import('../../src/game/systems/quest');

    game = new GameNode(
      testGameConfig({
        nodeId: 'game-1',
        keyPrefix: PREFIX,
        adminPort: GAME_ADMIN_PORT,
        transport: TRANSPORT,
        unloadDelayMs: 300,
      }),
      { store },
    ).use(new ProfileSystem(), new BagSystem(), new QuestSystem(), new RaceSystem());
    await game.start();
  });

  afterAll(async () => {
    for (const c of clients) c.close();
    await game?.shutdown('test');
    await gate?.shutdown('test');
    await flushPrefix(PREFIX);
  });

  // ------------------------------------------------------------ systems ---

  it('routes <service>.<system>.<action> to the owning system', async () => {
    const c = await client('router');
    const profile = await c.request<{ level: number; nickname: string }>('game.profile.get');
    expect(profile).toMatchObject({ level: 1 });
    // Filled in by the system's onPlayerOnline hook.
    expect(profile.nickname).toMatch(/^player_/);

    expect(await c.request('game.bag.list')).toEqual({ slots: [], capacity: 30 });
    expect(await c.request('game.quest.list')).toMatchObject({ active: [], completed: [] });
  });

  it('reports an unknown system or action as a route miss', async () => {
    const c = await client('missing');
    await expect(c.request('game.nope.thing')).rejects.toMatchObject({
      code: ErrorCode.RouteNotFound,
    });
    await expect(c.request('game.bag.nope')).rejects.toMatchObject({
      code: ErrorCode.RouteNotFound,
    });
  });

  it('surfaces system validation as a client error', async () => {
    const c = await client('validate');
    await expect(c.request('game.profile.rename', { name: 'x' })).rejects.toMatchObject({
      code: ErrorCode.BadRequest,
    });
    expect(await c.request('game.profile.rename', { name: 'Valid Name' })).toEqual({
      nickname: 'Valid Name',
    });
  });

  it('lets one system act on another through its public api', async () => {
    const c = await client('cross');
    await c.request('game.quest.accept', { id: 1 });
    // 300 exp = two level-ups on the demo curve.
    expect(await c.request('game.profile.addExp', { amount: 300 })).toMatchObject({
      level: 3,
      levelsGained: 2,
    });

    // quest advanced purely by reacting to profile's event: profile knows
    // nothing about quests.
    const quests = await c.request<{ active: Array<{ id: number; progress: number }> }>(
      'game.quest.list',
    );
    expect(quests.active[0]).toMatchObject({ id: 1, progress: 2, target: 2 });

    // claiming grants items through the bag system's api
    expect(await c.request('game.quest.claim', { id: 1 })).toMatchObject({
      claimed: 1,
      reward: [{ id: 2001, count: 1 }],
    });
    expect(await c.request('game.bag.list')).toMatchObject({
      slots: [{ id: 2001, count: 1 }],
    });
  });

  it('pushes to the client from inside a system', async () => {
    const c = await client('pusher');
    const pushes: Array<[string, unknown]> = [];
    c.on('push', (cmd: string, payload: unknown) => pushes.push([cmd, payload]));

    await c.request('game.quest.accept', { id: 1 });
    await c.request('game.profile.addExp', { amount: 300 });
    await sleep(150);

    expect(pushes.map(([cmd]) => cmd)).toEqual(
      expect.arrayContaining(['game.profile.levelUp', 'game.quest.finished']),
    );
  });

  it('rejects using an item the player does not have', async () => {
    const c = await client('empty-bag');
    await expect(c.request('game.bag.use', { id: 1001 })).rejects.toMatchObject({
      code: ErrorCode.BadRequest,
    });
  });

  // -------------------------------------------------------- concurrency ---

  it('serialises concurrent requests for one player, end to end', async () => {
    const c = await client('racer');
    // Ten concurrent read-await-write handlers. Without the per-player
    // mailbox most of them read the same value and the counter lands well
    // below 10.
    const results = await Promise.all(
      Array.from({ length: 10 }, () => c.request<{ n: number }>('game.race.bump')),
    );
    expect(await c.request('game.race.get')).toEqual({ n: 10 });
    expect(results.map((r) => r.n).sort((a, b) => a - b)).toEqual([
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    ]);
  });

  it('keeps different players independent', async () => {
    const [a, b] = await Promise.all([client('indep-a'), client('indep-b')]);
    await Promise.all([
      a.request('game.race.bump'),
      a.request('game.race.bump'),
      b.request('game.race.bump'),
    ]);
    expect(await a.request('game.race.get')).toEqual({ n: 2 });
    expect(await b.request('game.race.get')).toEqual({ n: 1 });
  });

  // -------------------------------------------------------- persistence ---

  it('saves on unload and reloads the same state', async () => {
    const uid = 'persistent';
    const first = await client(uid);
    await first.request('game.profile.rename', { name: 'Remembered' });
    await first.request('game.profile.addExp', { amount: 150 });
    first.close();

    // gate resume window (600ms) then the node's unload delay (300ms).
    await sleep(1_600);
    const saved = await store.load(uid);
    expect(saved).not.toBeNull();
    expect(saved?.systems['profile']).toMatchObject({ nickname: 'Remembered', level: 2 });

    const second = await client(uid);
    expect(await second.request('game.profile.get')).toMatchObject({
      nickname: 'Remembered',
      level: 2,
    });
  });

  it('exposes player and system state for ops', async () => {
    const c = await client('ops');
    await c.request('game.profile.get');
    const stats = (await (
      await fetch(`http://127.0.0.1:${GAME_ADMIN_PORT}/stats`)
    ).json()) as {
      node: string;
      players: { loaded: number; online: number };
      systems: Array<{ system: string; actions: string[] }>;
    };
    expect(stats.node).toBe('game-1');
    expect(stats.players.loaded).toBeGreaterThan(0);
    expect(stats.systems.map((s) => s.system).sort()).toEqual(['bag', 'profile', 'quest', 'race']);

    const metrics = await (await fetch(`http://127.0.0.1:${GAME_ADMIN_PORT}/metrics`)).text();
    expect(metrics).toContain('gate_players_online');
    expect(metrics).toContain('gate_systems');
  });

  it('rejects a player who floods their own mailbox', async () => {
    // mailboxLimit is 64 in the test config; 200 concurrent slow requests
    // must shed load rather than queue without bound.
    const c = await client('flooder');
    const results = await Promise.allSettled(
      Array.from({ length: 200 }, () => c.request('game.race.bump')),
    );
    const rejected = results.filter(
      (r) => r.status === 'rejected' && (r.reason as { code?: number }).code === ErrorCode.RateLimited,
    );
    expect(rejected.length).toBeGreaterThan(0);
    // and the player still works afterwards
    expect(await c.request('game.race.get')).toMatchObject({ n: expect.any(Number) });
  });
});
