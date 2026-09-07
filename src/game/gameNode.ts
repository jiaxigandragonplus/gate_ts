import { ServiceNode, ServiceError } from '../framework/serviceNode';
import { ErrorCode } from '../framework/protocol/packet';
import { RedisPool } from '../framework/redis/client';
import { Keys } from '../framework/redis/keys';
import { NodeRegistry } from '../framework/redis/nodeRegistry';
import { Metrics, AdminServer } from '../framework/admin';
import { logger } from '../framework/util/logger';
import type { GameConfig } from './config';
import { GameEvents } from './events';
import { MailboxFullError, type Player } from './player';
import { PlayerBusyElsewhereError, PlayerManager } from './playerManager';
import { MemoryPlayerStore, RedisPlayerStore, type PlayerStore } from './store';
import { SystemRegistry, type SomeSystem } from './system';

/**
 * The game node: the process that actually holds business logic.
 *
 * It is a thin shell on purpose. Everything specific to a game lives in
 * systems (see ./systems), and everything about being a cluster member -
 * transport, discovery, heartbeats, request/response plumbing - comes from
 * ../framework via ServiceNode.
 *
 * The gate pins each uid to one game node, which is what makes in-memory
 * player state correct; a per-player mailbox then serialises that player's
 * messages so handlers can await freely.
 */
export class GameNode {
  private readonly log;
  private readonly metrics = new Metrics();
  private readonly pool: RedisPool;
  private readonly keys: Keys;
  private readonly nodes: NodeRegistry;
  private readonly service: ServiceNode;
  private readonly registry = new SystemRegistry();
  private readonly events: GameEvents;
  private readonly players: PlayerManager;
  private readonly store: PlayerStore;
  private readonly admin: AdminServer;

  /** Counters specific to a game node; the generic ones live in Metrics. */
  private readonly own = { mailboxRejected: 0, playerLoadRefused: 0, systemErrors: 0 };

  private saveTimer: NodeJS.Timeout | null = null;
  private leaseTimer: NodeJS.Timeout | null = null;
  private tickTimer: NodeJS.Timeout | null = null;
  private started = false;
  private shuttingDown = false;

  /**
   * `deps.store` is the seam for persistence: pass your own PlayerStore to
   * put players in MySQL / Mongo / whatever you already operate. The shipped
   * redis store is for development and small scale.
   */
  constructor(
    private readonly cfg: GameConfig,
    deps: { store?: PlayerStore } = {},
  ) {
    this.log = logger.child({ mod: 'game', node: cfg.nodeId });
    this.pool = new RedisPool(cfg.redis.url);
    this.keys = new Keys(cfg.redis.keyPrefix);
    this.nodes = new NodeRegistry(this.pool.cmd, this.keys, cfg.cluster.nodeTtlMs);
    this.events = new GameEvents(this.log);

    this.store =
      deps.store ??
      (cfg.store.kind === 'memory'
        ? new MemoryPlayerStore()
        : new RedisPlayerStore(this.pool.cmd, cfg.redis.keyPrefix));

    this.service = new ServiceNode({
      service: cfg.service,
      nodeId: cfg.nodeId,
      redisUrl: cfg.redis.url,
      keyPrefix: cfg.redis.keyPrefix,
      heartbeatMs: cfg.cluster.heartbeatMs,
      nodeTtlMs: cfg.cluster.nodeTtlMs,
      transport: cfg.cluster.transport,
      subjectPrefix: cfg.nats.subjectPrefix,
      // Load reported to gates is the number of players actually resident.
      load: () => this.players?.size ?? 0,
    });

    this.players = new PlayerManager(
      this.pool.cmd,
      this.store,
      this.registry,
      this.events,
      this.nodes,
      this.log,
      {
        nodeId: cfg.nodeId,
        service: cfg.service,
        keyPrefix: cfg.redis.keyPrefix,
        unloadDelayMs: cfg.player.unloadDelayMs,
        mailboxLimit: cfg.player.mailboxLimit,
        slowHandlerMs: cfg.player.slowHandlerMs,
        leaseTtlMs: cfg.player.leaseTtlMs,
      },
      (player, cmd, payload) => {
        this.metrics.downstreamPushes += 1;
        void this.service.pushToSession(player.gate, player.sid, cmd, payload);
      },
    );

    this.admin = new AdminServer(
      { host: cfg.admin.host, port: cfg.admin.port, gateId: cfg.nodeId },
      this.metrics,
      {
        stats: () => this.stats(),
        ready: () => this.started && !this.shuttingDown,
        kick: async (target, reason) =>
          target.uid ? this.service.kick(target.uid, reason) : false,
        push: (uid, cmd, payload) => this.service.pushToUid(uid, cmd, payload),
        drain: () => void this.shutdown('admin drain'),
      },
    );

    this.metrics.registerGauge('players_loaded', () => this.players.size);
    this.metrics.registerGauge('players_online', () => this.players.onlineCount);
    this.metrics.registerGauge('players_dirty', () => this.players.dirtyCount);
    this.metrics.registerGauge('systems', () => this.registry.names().length);
    this.metrics.registerGauge('mailbox_rejected_total', () => this.own.mailboxRejected);
    this.metrics.registerGauge('player_load_refused_total', () => this.own.playerLoadRefused);
    this.metrics.registerGauge('system_errors_total', () => this.own.systemErrors);
  }

  /** Register a system. Call before start(). */
  use(...systems: SomeSystem[]): this {
    if (this.started) throw new Error('systems must be registered before start()');
    for (const system of systems) this.registry.add(system);
    return this;
  }

  // ---------------------------------------------------------- lifecycle ---

  async start(): Promise<void> {
    if (this.registry.all().length === 0) {
      throw new Error('no systems registered: a game node with no systems does nothing');
    }
    await this.pool.connect();

    // Systems see each other here, so init() is where cross-system event
    // subscriptions belong.
    for (const system of this.registry.all()) {
      const shared = {
        systems: this.registry,
        events: this.events,
        log: this.log.child({ system: system.name }),
        nodeId: this.cfg.nodeId,
        pushToUid: (uid: string, cmd: string, payload?: unknown) =>
          this.service.pushToUid(uid, cmd, payload),
        kick: (uid: string, reason?: string, message?: string) =>
          this.service.kick(uid, reason, message),
      };
      system.attach(shared);
      await system.init?.(shared);
    }

    // One catch-all route: the gate sends the whole `<service>.` prefix here
    // and never looks inside, so adding a system needs no gate change.
    this.service.on(`${this.cfg.service}.`, (ctx) => this.dispatch(ctx.uid, ctx.cmd, ctx.payload));
    this.service.onSession(async (ctx) => {
      try {
        if (ctx.event === 'online' || ctx.event === 'resumed') {
          await this.players.onOnline(ctx.uid, ctx.gate, ctx.sid);
        } else if (ctx.event === 'offline') {
          await this.players.onOffline(ctx.uid);
        }
      } catch (err) {
        this.log.error(
          { err: (err as Error).message, uid: ctx.uid, ev: ctx.event },
          'session event failed',
        );
      }
    });

    await this.service.start();
    await this.admin.listen();
    this.startTimers();
    this.started = true;

    this.log.info(
      {
        service: this.cfg.service,
        transport: this.cfg.cluster.transport,
        store: this.cfg.store.kind,
        systems: this.registry.describe(),
      },
      'game node started',
    );
  }

  private startTimers(): void {
    this.saveTimer = setInterval(() => {
      void this.players
        .saveDirty()
        .then((n) => {
          if (n > 0) this.log.debug({ saved: n }, 'flushed dirty players');
        })
        .catch((err: Error) => this.log.error({ err: err.message }, 'save pass failed'));
    }, this.cfg.player.saveIntervalMs);
    this.saveTimer.unref();

    // Refresh at a third of the TTL so a hiccup does not drop a lease.
    this.leaseTimer = setInterval(() => {
      void this.players
        .refreshLeases()
        .catch((err: Error) => this.log.error({ err: err.message }, 'lease refresh failed'));
    }, Math.max(1_000, Math.floor(this.cfg.player.leaseTtlMs / 3)));
    this.leaseTimer.unref();

    if (this.cfg.tickIntervalMs > 0) {
      this.tickTimer = setInterval(() => {
        void this.players
          .tick()
          .catch((err: Error) => this.log.error({ err: err.message }, 'tick failed'));
      }, this.cfg.tickIntervalMs);
      this.tickTimer.unref();
    }
  }

  async shutdown(reason: string): Promise<void> {
    if (this.shuttingDown) return;
    this.shuttingDown = true;
    this.log.info({ reason, players: this.players.size }, 'shutting down');

    for (const timer of [this.saveTimer, this.leaseTimer, this.tickTimer]) {
      if (timer) clearInterval(timer);
    }
    // Deregister first so gates stop routing here, then persist everyone.
    await this.service.stop().catch(() => undefined);
    await this.players.drain();
    await this.store.close?.().catch(() => undefined);
    await this.admin.close();
    await this.pool.close();
    this.log.info('shutdown complete');
  }

  // ------------------------------------------------------------ routing ---

  /**
   * Route one client message to the system that owns it.
   *
   * `game.bag.use` -> system "bag", action "use". The handler runs inside the
   * player's mailbox, so it has exclusive access to that player's state.
   */
  private async dispatch(uid: string, cmd: string, payload: unknown): Promise<unknown> {
    const key = cmd.startsWith(`${this.cfg.service}.`)
      ? cmd.slice(this.cfg.service.length + 1)
      : cmd;

    const route = this.registry.route(key);
    if (!route) {
      this.metrics.routeMisses += 1;
      throw new ServiceError(`no handler for ${cmd}`, ErrorCode.RouteNotFound);
    }

    let player: Player;
    try {
      player = await this.players.getOrLoad(uid);
    } catch (err) {
      if (err instanceof PlayerBusyElsewhereError) {
        // Another live node owns this player: tell the client to retry rather
        // than serving a second copy of their state.
        this.own.playerLoadRefused += 1;
        throw new ServiceError('player is being served elsewhere', ErrorCode.ServiceUnavailable);
      }
      throw err;
    }

    const startedAt = Date.now();
    try {
      const result = await player.enqueue(key, () =>
        route.handler(this.players.contextFor(player, route.system, cmd), payload),
      );
      this.metrics.observeUpstreamLatency(Date.now() - startedAt);
      this.metrics.downstreamResponses += 1;
      return result;
    } catch (err) {
      if (err instanceof MailboxFullError) {
        // The player is flooding: shed load instead of growing the queue.
        this.own.mailboxRejected += 1;
        throw new ServiceError('too many pending requests', ErrorCode.RateLimited);
      }
      if (!(err instanceof ServiceError)) this.own.systemErrors += 1;
      throw err;
    }
  }

  private stats(): unknown {
    return {
      node: this.cfg.nodeId,
      service: this.cfg.service,
      transport: this.cfg.cluster.transport,
      store: this.cfg.store.kind,
      draining: this.shuttingDown,
      players: {
        loaded: this.players.size,
        online: this.players.onlineCount,
        dirty: this.players.dirtyCount,
        deepestMailbox: Math.max(0, ...this.players.all().map((p) => p.peakQueueDepth)),
      },
      systems: this.registry.describe(),
      counters: { ...this.metrics.snapshot(), ...this.own },
    };
  }
}
