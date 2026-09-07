import type Redis from 'ioredis';
import type { Logger } from 'pino';
import { Player } from './player';
import type { PlayerStore, PlayerRecord } from './store';
import { emptyRecord } from './store';
import type { SomeSystem, SystemContext, SystemRegistry } from './system';
import type { GameEvents } from './events';
import type { NodeRegistry } from '../framework/redis/nodeRegistry';

export class PlayerBusyElsewhereError extends Error {
  constructor(
    readonly uid: string,
    readonly owner: string,
  ) {
    super(`player ${uid} is still owned by live node ${owner}`);
    this.name = 'PlayerBusyElsewhereError';
  }
}

const ACQUIRE_LUA = `
local cur = redis.call('GET', KEYS[1])
if not cur then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 'acquired'
end
if cur == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 'acquired'
end
return cur
`;

const RELEASE_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`;

const REFRESH_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`;

interface LeaseRedis extends Redis {
  gameAcquirePlayer(k: string, a1: string, a2: string): Promise<string>;
  gameReleasePlayer(k: string, a1: string): Promise<number>;
  gameRefreshPlayer(k: string, a1: string, a2: string): Promise<number>;
}

export interface PlayerManagerOptions {
  nodeId: string;
  service: string;
  keyPrefix: string;
  unloadDelayMs: number;
  mailboxLimit: number;
  slowHandlerMs: number;
  leaseTtlMs: number;
}

/**
 * Owns the in-memory player set on this node: loading, per-system state
 * slices, save policy, and the ownership lease that keeps two nodes from
 * serving the same player.
 */
export class PlayerManager {
  private readonly redis: LeaseRedis;
  private readonly players = new Map<string, Player>();
  private readonly loading = new Map<string, Promise<Player>>();
  private readonly unloadTimers = new Map<string, NodeJS.Timeout>();
  private draining = false;

  constructor(
    redis: Redis,
    private readonly store: PlayerStore,
    private readonly systems: SystemRegistry,
    private readonly events: GameEvents,
    private readonly nodes: NodeRegistry,
    private readonly log: Logger,
    private readonly opts: PlayerManagerOptions,
    private readonly pushFn: (player: Player, cmd: string, payload?: unknown) => void,
  ) {
    this.redis = redis as LeaseRedis;
    redis.defineCommand('gameAcquirePlayer', { numberOfKeys: 1, lua: ACQUIRE_LUA });
    redis.defineCommand('gameReleasePlayer', { numberOfKeys: 1, lua: RELEASE_LUA });
    redis.defineCommand('gameRefreshPlayer', { numberOfKeys: 1, lua: REFRESH_LUA });
  }

  get size(): number {
    return this.players.size;
  }

  get onlineCount(): number {
    let n = 0;
    for (const p of this.players.values()) if (p.online) n += 1;
    return n;
  }

  get dirtyCount(): number {
    let n = 0;
    for (const p of this.players.values()) if (p.dirty) n += 1;
    return n;
  }

  all(): Player[] {
    return [...this.players.values()];
  }

  get(uid: string): Player | undefined {
    return this.players.get(uid);
  }

  private leaseKey(uid: string): string {
    return `${this.opts.keyPrefix}:game:${this.opts.service}:owner:${uid}`;
  }

  // -------------------------------------------------------------- load ----

  /**
   * Get a loaded player, loading them if needed. Concurrent callers share one
   * load, so a burst of requests for a fresh login does not hit the store
   * repeatedly.
   */
  async getOrLoad(uid: string): Promise<Player> {
    const existing = this.players.get(uid);
    if (existing && !existing.isClosed) {
      this.cancelUnload(uid);
      return existing;
    }
    const inFlight = this.loading.get(uid);
    if (inFlight) return inFlight;

    const promise = this.load(uid).finally(() => this.loading.delete(uid));
    this.loading.set(uid, promise);
    return promise;
  }

  private async load(uid: string): Promise<Player> {
    if (this.draining) throw new Error('node is shutting down');
    await this.acquireLease(uid);

    const record = (await this.store.load(uid)) ?? emptyRecord(uid);
    const player = new Player({
      uid,
      record,
      mailboxLimit: this.opts.mailboxLimit,
      slowHandlerMs: this.opts.slowHandlerMs,
      push: this.pushFn,
      onSlowHandler: (info) => this.log.warn(info, 'handler held a player mailbox'),
    });

    // Give every system its slice: fresh for a new player, migrated for one
    // persisted by an older build.
    for (const system of this.systems.all()) {
      if (player.hasState(system.name)) {
        const migrated = system.migrate?.(player.state(system.name));
        if (migrated !== undefined) player.setState(system.name, migrated);
      } else {
        player.setState(system.name, system.createState());
      }
    }

    this.players.set(uid, player);
    this.log.info({ uid, isNew: record.version === 0, systems: this.systems.names().length }, 'player loaded');
    return player;
  }

  /**
   * Claim this player for this node.
   *
   * On conflict the lease is only taken over when the current owner is gone
   * from the node registry. Stealing from a *live* node would fork the
   * player's state - two nodes each saving their own copy of someone's
   * inventory - so that case fails loudly instead.
   */
  private async acquireLease(uid: string): Promise<void> {
    const result = await this.redis.gameAcquirePlayer(
      this.leaseKey(uid),
      this.opts.nodeId,
      String(this.opts.leaseTtlMs),
    );
    if (result === 'acquired') return;

    const live = await this.nodes.listServiceNodes(this.opts.service);
    if (live.some((n) => n.id === result)) {
      this.log.error({ uid, owner: result }, 'refusing to load player owned by another live node');
      throw new PlayerBusyElsewhereError(uid, result);
    }

    // Previous owner is not in the registry any more: its lease is stale.
    this.log.warn({ uid, deadOwner: result }, 'taking over player from a dead node');
    await this.redis.set(this.leaseKey(uid), this.opts.nodeId, 'PX', this.opts.leaseTtlMs);
  }

  // ------------------------------------------------------------ session ---

  async onOnline(uid: string, gate: string, sid: string): Promise<void> {
    const player = await this.getOrLoad(uid);
    player.bindSession(gate, sid);
    this.cancelUnload(uid);
    await this.runLifecycle(player, 'onPlayerOnline');
    await this.events.emit('player.online', { player });
  }

  async onOffline(uid: string): Promise<void> {
    const player = this.players.get(uid);
    if (!player) return;
    player.markOffline();
    await this.runLifecycle(player, 'onPlayerOffline');
    await this.events.emit('player.offline', { player });
    await this.save(player);

    // Keep state warm for the gate's resume window: a reconnecting player
    // should not pay a reload.
    this.cancelUnload(uid);
    const timer = setTimeout(() => {
      this.unloadTimers.delete(uid);
      void this.unload(uid).catch((err: Error) =>
        this.log.error({ err: err.message, uid }, 'unload failed'),
      );
    }, this.opts.unloadDelayMs);
    timer.unref();
    this.unloadTimers.set(uid, timer);
  }

  private cancelUnload(uid: string): void {
    const timer = this.unloadTimers.get(uid);
    if (timer) {
      clearTimeout(timer);
      this.unloadTimers.delete(uid);
    }
  }

  private async runLifecycle(
    player: Player,
    hook: 'onPlayerOnline' | 'onPlayerOffline' | 'onTick',
  ): Promise<void> {
    for (const system of this.systems.all()) {
      const fn = system[hook];
      if (!fn) continue;
      try {
        await player.enqueue(`${system.name}.${hook}`, () =>
          fn.call(system, this.contextFor(player, system, `${hook}`)),
        );
      } catch (err) {
        this.log.error(
          { err: (err as Error).message, uid: player.uid, system: system.name, hook },
          'system lifecycle hook failed',
        );
      }
    }
  }

  /** Build the per-message context handed to a system. */
  contextFor(player: Player, system: SomeSystem, cmd: string): SystemContext<never> {
    return {
      player,
      state: player.state(system.name),
      systems: this.systems,
      events: this.events,
      log: this.log.child({ uid: player.uid, system: system.name }),
      cmd,
    };
  }

  // --------------------------------------------------------------- save ---

  async save(player: Player): Promise<void> {
    if (!player.dirty) return;
    const record = player.toRecord();
    await this.store.save(record);
    player.onSaved(record);
  }

  /** Periodic flush of everyone dirty. */
  async saveDirty(): Promise<number> {
    const dirty = this.all().filter((p) => p.dirty);
    if (dirty.length === 0) return 0;

    const records: PlayerRecord[] = dirty.map((p) => p.toRecord());
    try {
      if (this.store.saveMany) await this.store.saveMany(records);
      else for (const record of records) await this.store.save(record);
      dirty.forEach((p, i) => p.onSaved(records[i] as PlayerRecord));
      return dirty.length;
    } catch (err) {
      // Leave them dirty so the next pass retries.
      this.log.error({ err: (err as Error).message, count: dirty.length }, 'batch save failed');
      return 0;
    }
  }

  async refreshLeases(): Promise<void> {
    for (const player of this.all()) {
      const ok = await this.redis
        .gameRefreshPlayer(this.leaseKey(player.uid), this.opts.nodeId, String(this.opts.leaseTtlMs))
        .catch(() => 0);
      if (ok !== 1) {
        // Someone else owns this player now; stop serving them locally rather
        // than writing over the new owner's state.
        this.log.error({ uid: player.uid }, 'lost player lease, unloading without saving');
        await this.unload(player.uid, { save: false });
      }
    }
  }

  // ------------------------------------------------------------- unload ---

  async unload(uid: string, opts: { save?: boolean } = {}): Promise<void> {
    const player = this.players.get(uid);
    if (!player) return;
    this.cancelUnload(uid);

    await player.quiesce();
    player.markClosed();
    this.players.delete(uid);

    if (opts.save !== false) {
      await this.save(player).catch((err: Error) =>
        this.log.error({ err: err.message, uid }, 'save on unload failed'),
      );
      await this.redis.gameReleasePlayer(this.leaseKey(uid), this.opts.nodeId).catch(() => 0);
    }
    this.log.info({ uid }, 'player unloaded');
  }

  /** Save and release everyone; used on shutdown. */
  async drain(): Promise<void> {
    this.draining = true;
    for (const timer of this.unloadTimers.values()) clearTimeout(timer);
    this.unloadTimers.clear();
    const uids = [...this.players.keys()];
    for (const uid of uids) {
      await this.unload(uid).catch((err: Error) =>
        this.log.error({ err: err.message, uid }, 'drain unload failed'),
      );
    }
    this.log.info({ count: uids.length }, 'all players saved and released');
  }

  /** Run each system's tick hook for every loaded player. */
  async tick(): Promise<void> {
    const tickers = this.systems.all().filter((s) => s.onTick);
    if (tickers.length === 0) return;
    for (const player of this.all()) {
      for (const system of tickers) {
        try {
          await player.enqueue(`${system.name}.tick`, () =>
            system.onTick?.(this.contextFor(player, system, 'tick')),
          );
        } catch (err) {
          this.log.error(
            { err: (err as Error).message, uid: player.uid, system: system.name },
            'system tick failed',
          );
        }
      }
    }
  }
}
