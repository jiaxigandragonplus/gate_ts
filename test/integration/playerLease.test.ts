/**
 * Player ownership: two game nodes must never serve the same player, or each
 * would save its own copy of that player's inventory.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import Redis from 'ioredis';
import { PlayerBusyElsewhereError, PlayerManager } from '../../src/game/playerManager';
import { MemoryPlayerStore } from '../../src/game/store';
import { SystemRegistry, GameSystem } from '../../src/game/system';
import { GameEvents } from '../../src/game/events';
import { NodeRegistry } from '../../src/framework/redis/nodeRegistry';
import { Keys } from '../../src/framework/redis/keys';
import { logger } from '../../src/framework/util/logger';
import { REDIS_URL, flushPrefix } from '../helpers/testGate';

const PREFIX = `gate-lease-${process.pid}`;
const available = process.env.GATE_TEST_REDIS !== '0';

class Dummy extends GameSystem<{ v: number }> {
  readonly name = 'dummy';
  override createState(): { v: number } {
    return { v: 0 };
  }
}

describe.skipIf(!available)('player ownership lease', () => {
  let redis: Redis;
  let nodes: NodeRegistry;
  const store = new MemoryPlayerStore();
  const log = logger.child({ mod: 'lease-test' });

  const manager = (nodeId: string): PlayerManager => {
    const registry = new SystemRegistry();
    registry.add(new Dummy());
    return new PlayerManager(
      redis,
      store,
      registry,
      new GameEvents(log),
      nodes,
      log,
      {
        nodeId,
        service: 'game',
        keyPrefix: PREFIX,
        unloadDelayMs: 60_000,
        mailboxLimit: 16,
        slowHandlerMs: 10_000,
        leaseTtlMs: 2_000,
      },
      () => undefined,
    );
  };

  beforeAll(async () => {
    await flushPrefix(PREFIX);
    redis = new Redis(REDIS_URL);
    nodes = new NodeRegistry(redis, new Keys(PREFIX), 30_000);
  });

  afterAll(async () => {
    redis?.disconnect();
    await flushPrefix(PREFIX);
  });

  it('lets one node load a player', async () => {
    const a = manager('node-a');
    const player = await a.getOrLoad('p1');
    expect(player.uid).toBe('p1');
    expect(a.size).toBe(1);
    await a.unload('p1');
  });

  it('refuses a second live node, rather than forking the player', async () => {
    // node-a is registered and heartbeating, so it is demonstrably alive.
    await nodes.heartbeatService('game', {
      id: 'node-a',
      addr: 'a:1',
      load: 0,
      ts: Date.now(),
    });
    const a = manager('node-a');
    await a.getOrLoad('p2');

    const b = manager('node-b');
    await expect(b.getOrLoad('p2')).rejects.toThrow(PlayerBusyElsewhereError);
    expect(b.size).toBe(0);

    await a.unload('p2');
    // Once released, the other node can take over cleanly.
    const player = await b.getOrLoad('p2');
    expect(player.uid).toBe('p2');
    await b.unload('p2');
  });

  it('takes over from a node that is gone from the registry', async () => {
    // A dead node's lease would otherwise lock the player out until its TTL.
    await redis.set(`${PREFIX}:game:game:owner:p3`, 'node-dead', 'PX', 60_000);
    const b = manager('node-b');
    const player = await b.getOrLoad('p3');
    expect(player.uid).toBe('p3');
    expect(await redis.get(`${PREFIX}:game:game:owner:p3`)).toBe('node-b');
    await b.unload('p3');
  });

  it('shares one load between concurrent callers', async () => {
    const a = manager('node-a');
    const [x, y, z] = await Promise.all([
      a.getOrLoad('p4'),
      a.getOrLoad('p4'),
      a.getOrLoad('p4'),
    ]);
    expect(x).toBe(y);
    expect(y).toBe(z);
    expect(a.size).toBe(1);
    await a.unload('p4');
  });

  it('drops a player whose lease was lost instead of overwriting the new owner', async () => {
    const a = manager('node-a');
    const player = await a.getOrLoad('p5');
    player.setState('dummy', { v: 42 });

    // Simulate another node winning the lease.
    await redis.set(`${PREFIX}:game:game:owner:p5`, 'node-other', 'PX', 60_000);
    await a.refreshLeases();

    expect(a.size).toBe(0);
    // The stolen player was NOT saved over the new owner's state.
    expect(await store.load('p5')).toBeNull();
  });

  it('releases the lease on unload so another node can take the player', async () => {
    const a = manager('node-a');
    await a.getOrLoad('p6');
    expect(await redis.get(`${PREFIX}:game:game:owner:p6`)).toBe('node-a');
    await a.unload('p6');
    expect(await redis.get(`${PREFIX}:game:game:owner:p6`)).toBeNull();
  });
});
