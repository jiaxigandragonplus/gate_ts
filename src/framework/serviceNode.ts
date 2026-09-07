import { hostname } from 'node:os';
import { RedisPool } from './redis/client';
import { Keys } from './redis/keys';
import { NodeRegistry } from './redis/nodeRegistry';
import { ErrorCode } from './protocol/packet';
import { createServiceBus, type ServiceBus, type TransportKind } from './transport';
import { normalizeNodeId } from './nats/subjects';
import type {
  DownstreamMessage,
  UpRequest,
  UpNotify,
  UpSessionEvent,
  UpstreamMessage,
  UpstreamMeta,
} from './protocol/internal';
import { fromInternal, isBytes, toInternal } from './protocol/payload';
import { shortId } from './util/id';
import { logger } from './util/logger';

export interface ServiceNodeOptions {
  /** Service name clients route to, e.g. "game" or "chat". */
  service: string;
  nodeId?: string;
  redisUrl?: string;
  keyPrefix?: string;
  heartbeatMs?: number;
  nodeTtlMs?: number;
  /** Reported to gates for load-aware routing; defaults to session count. */
  load?: () => number;
  /**
   * Server-to-server transport. Defaults to CLUSTER_TRANSPORT, then 'nats'.
   * Redis is still used for cluster state either way.
   */
  transport?: TransportKind;
  natsServers?: string[];
  subjectPrefix?: string;
}

export interface RequestContext {
  uid: string;
  sid: string;
  /** Gate that owns the session; replies go back through it automatically. */
  gate: string;
  cmd: string;
  /** Decoded JSON payload. Undefined when the client sent opaque bytes. */
  payload: unknown;
  /**
   * Raw payload bytes, set when the client speaks the protobuf codec (or any
   * codec that carries binary payloads). The gate never parses these - decode
   * them with your own schema.
   */
  payloadBytes?: Buffer;
  meta?: UpstreamMeta;
  ts: number;
}

export interface SessionContext {
  event: 'online' | 'offline' | 'suspended' | 'resumed';
  uid: string;
  sid: string;
  gate: string;
  reason?: string;
  meta?: UpstreamMeta;
}

/**
 * Return a value to send it back as the response payload. A `Buffer` is sent
 * as opaque bytes (what a protobuf client wants); anything else is sent as
 * JSON.
 */
export type RequestHandler = (ctx: RequestContext) => Promise<unknown> | unknown;
export type SessionHandler = (ctx: SessionContext) => Promise<void> | void;

/** Throw from a handler to send a structured error back to the client. */
export class ServiceError extends Error {
  constructor(
    message: string,
    readonly code: ErrorCode | number = ErrorCode.BadRequest,
  ) {
    super(message);
    this.name = 'ServiceError';
  }
}

/**
 * Backend-side counterpart of the gate.
 *
 * A game / chat / whatever service creates one of these, registers command
 * handlers, and gets client traffic delivered to it - plus `push`,
 * `broadcast` and `kick` to talk back. It handles registration, heartbeats
 * and reply routing, so a service never needs to know a gate's address.
 */
export class ServiceNode {
  readonly service: string;
  readonly nodeId: string;
  /** Sessions this node currently knows about; the default load metric. */
  readonly liveSessions = new Set<string>();

  private readonly log;
  private readonly pool: RedisPool;
  private readonly keys: Keys;
  private readonly registry: NodeRegistry;
  private readonly bus: ServiceBus;
  private readonly handlers = new Map<string, RequestHandler>();
  private readonly prefixHandlers: Array<{ prefix: string; handler: RequestHandler }> = [];
  private readonly opts: {
    service: string;
    redisUrl: string;
    keyPrefix: string;
    heartbeatMs: number;
    nodeTtlMs: number;
    load?: () => number;
  };
  private sessionHandler: SessionHandler | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private started = false;

  constructor(options: ServiceNodeOptions) {
    this.service = normalizeNodeId(options.service);
    // Normalized once: the same id is used as a redis field, a NATS subject
    // token and a log label, and hostnames routinely contain dots.
    this.nodeId = normalizeNodeId(
      options.nodeId ?? `${options.service}-${hostname()}-${shortId(4)}`,
    );
    this.opts = {
      service: options.service,
      redisUrl: options.redisUrl ?? process.env.REDIS_URL ?? 'redis://127.0.0.1:6379',
      keyPrefix: options.keyPrefix ?? process.env.REDIS_KEY_PREFIX ?? 'gate',
      heartbeatMs: options.heartbeatMs ?? 5_000,
      nodeTtlMs: options.nodeTtlMs ?? 15_000,
      ...(options.load ? { load: options.load } : {}),
    };
    this.log = logger.child({ mod: 'service', service: this.service, node: this.nodeId });
    // Redis stays the state layer (session ownership, node registry) whatever
    // transport carries the messages.
    this.pool = new RedisPool(this.opts.redisUrl);
    this.keys = new Keys(this.opts.keyPrefix);
    this.registry = new NodeRegistry(this.pool.cmd, this.keys, this.opts.nodeTtlMs);

    const transport =
      options.transport ?? ((process.env.CLUSTER_TRANSPORT as TransportKind | undefined) || 'nats');
    if (transport !== 'redis' && transport !== 'nats') {
      throw new Error(`unknown transport "${transport}" (expected "redis" or "nats")`);
    }
    this.bus = createServiceBus(
      {
        kind: transport,
        redis: { pool: this.pool, keys: this.keys },
        nats: {
          subjectPrefix:
            options.subjectPrefix ?? process.env.NATS_SUBJECT_PREFIX ?? 'gate',
          options: {
            servers:
              options.natsServers ??
              (process.env.NATS_SERVERS ?? 'nats://127.0.0.1:4222')
                .split(',')
                .map((v) => v.trim())
                .filter(Boolean),
            name: this.nodeId,
          },
        },
      },
      this.service,
      this.nodeId,
    );
  }

  /** Register a handler. `cmd` ending in `.` or `*` matches by prefix. */
  on(cmd: string, handler: RequestHandler): this {
    if (cmd.endsWith('*') || cmd.endsWith('.')) {
      this.prefixHandlers.push({ prefix: cmd.replace(/\*$/, ''), handler });
      this.prefixHandlers.sort((a, b) => b.prefix.length - a.prefix.length);
    } else {
      this.handlers.set(cmd, handler);
    }
    return this;
  }

  onSession(handler: SessionHandler): this {
    this.sessionHandler = handler;
    return this;
  }

  async start(): Promise<void> {
    if (this.started) throw new Error('service node already started');
    this.started = true;
    await this.pool.connect();
    await this.bus.start((msg: UpstreamMessage) => {
      void this.dispatch(msg);
    });

    await this.heartbeat();
    this.heartbeatTimer = setInterval(() => {
      void this.heartbeat();
    }, this.opts.heartbeatMs);
    this.heartbeatTimer.unref();

    this.log.info(
      { transport: this.bus.kind, commands: [...this.handlers.keys()] },
      'service node ready',
    );
  }

  private async heartbeat(): Promise<void> {
    try {
      await this.registry.heartbeatService(this.service, {
        id: this.nodeId,
        addr: `${hostname()}:${process.pid}`,
        load: this.opts.load ? this.opts.load() : this.liveSessions.size,
        ts: Date.now(),
        // Advertised so a gate on a different transport can say so out loud
        // instead of timing out on every request.
        meta: { transport: this.bus.kind },
      });
    } catch (err) {
      this.log.warn({ err: (err as Error).message }, 'heartbeat failed');
    }
  }

  private async dispatch(msg: UpRequest | UpNotify | UpSessionEvent): Promise<void> {
    if (msg.k === 'session') {
      if (msg.ev === 'online' || msg.ev === 'resumed') this.liveSessions.add(msg.sid);
      if (msg.ev === 'offline') this.liveSessions.delete(msg.sid);
      if (!this.sessionHandler) return;
      try {
        await this.sessionHandler({
          event: msg.ev,
          uid: msg.uid,
          sid: msg.sid,
          gate: msg.gate,
          ...(msg.reason === undefined ? {} : { reason: msg.reason }),
          ...(msg.meta === undefined ? {} : { meta: msg.meta }),
        });
      } catch (err) {
        this.log.error({ err: (err as Error).message, ev: msg.ev }, 'session handler threw');
      }
      return;
    }

    const handler = this.resolve(msg.cmd);
    const payload = fromInternal(msg);
    const binary = isBytes(payload);
    const ctx: RequestContext = {
      uid: msg.uid,
      sid: msg.sid,
      gate: msg.gate,
      cmd: msg.cmd,
      payload: binary ? undefined : payload,
      ts: msg.ts,
      ...(binary ? { payloadBytes: payload } : {}),
      ...(msg.meta === undefined ? {} : { meta: msg.meta }),
    };

    if (!handler) {
      this.log.warn({ cmd: msg.cmd }, 'no handler for command');
      if (msg.k === 'req') {
        await this.reply(msg, undefined, ErrorCode.RouteNotFound, `unhandled command ${msg.cmd}`);
      }
      return;
    }

    try {
      const result = await handler(ctx);
      if (msg.k === 'req') await this.reply(msg, result);
    } catch (err) {
      const code = err instanceof ServiceError ? err.code : ErrorCode.Internal;
      const message = err instanceof ServiceError ? err.message : 'internal service error';
      if (!(err instanceof ServiceError)) {
        this.log.error({ err: (err as Error).message, cmd: msg.cmd }, 'handler threw');
      }
      if (msg.k === 'req') await this.reply(msg, undefined, code, message);
    }
  }

  private resolve(cmd: string): RequestHandler | undefined {
    const exact = this.handlers.get(cmd);
    if (exact) return exact;
    for (const { prefix, handler } of this.prefixHandlers) {
      if (cmd.startsWith(prefix)) return handler;
    }
    return undefined;
  }

  private async reply(
    msg: UpRequest,
    payload: unknown,
    error?: ErrorCode | number,
    message?: string,
  ): Promise<void> {
    await this.publishToGate(msg.gate, {
      k: 'resp',
      sid: msg.sid,
      id: msg.id,
      ...toInternal(payload),
      ...(error === undefined ? {} : { e: error as ErrorCode }),
      ...(message === undefined ? {} : { m: message }),
    });
  }

  // ------------------------------------------------------------ outbound ---

  /** Push to a specific session (fastest: no lookup needed). */
  async pushToSession(gate: string, sid: string, cmd: string, payload?: unknown): Promise<void> {
    await this.publishToGate(gate, { k: 'push', sid, cmd, ...toInternal(payload) });
  }

  /** Push to a uid wherever it is connected. */
  async pushToUid(uid: string, cmd: string, payload?: unknown): Promise<boolean> {
    const owner = await this.ownerOf(uid);
    if (!owner) return false;
    await this.publishToGate(owner.gate, { k: 'push', uid, cmd, ...toInternal(payload) });
    return true;
  }

  /** Fan out to many uids; each gate filters to the sessions it owns. */
  async multicast(uids: string[], cmd: string, payload?: unknown): Promise<void> {
    await this.publishToAllGates({ k: 'multicast', uids, cmd, ...toInternal(payload) });
  }

  async broadcast(cmd: string, payload?: unknown): Promise<void> {
    await this.publishToAllGates({ k: 'broadcast', cmd, ...toInternal(payload) });
  }

  /** Force a client offline (ban, anti-cheat, ...). */
  async kick(uid: string, reason = 'admin', message?: string): Promise<boolean> {
    const owner = await this.ownerOf(uid);
    if (!owner) return false;
    await this.publishToGate(owner.gate, {
      k: 'kick',
      uid,
      sid: owner.sid,
      reason,
      ...(message === undefined ? {} : { m: message }),
    });
    return true;
  }

  /** Where is this account connected right now? */
  async ownerOf(uid: string): Promise<{ gate: string; sid: string } | null> {
    const raw = await this.pool.cmd.get(this.keys.session(uid));
    if (!raw) return null;
    try {
      const owner = JSON.parse(raw) as { gate: string; sid: string };
      return owner.gate && owner.sid ? owner : null;
    } catch {
      return null;
    }
  }

  private async publishToGate(gate: string, msg: DownstreamMessage): Promise<void> {
    await this.bus.publishToGate(gate, msg);
  }

  private async publishToAllGates(msg: DownstreamMessage): Promise<void> {
    await this.bus.publishToAllGates(msg);
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    await this.registry.unregisterService(this.service, this.nodeId).catch(() => undefined);
    await this.bus.stop().catch(() => undefined);
    await this.pool.close();
    this.log.info('service node stopped');
  }
}
