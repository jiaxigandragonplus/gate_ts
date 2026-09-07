import type Redis from 'ioredis';
import type { ClusterBus } from '../../framework/redis/bus';
import type { Keys } from '../../framework/redis/keys';
import { NodeRegistry, pickNode, type NodeInfo } from '../../framework/redis/nodeRegistry';
import type { UpstreamMessage } from '../../framework/protocol/internal';
import { logger } from '../../framework/util/logger';

export class ServiceUnavailableError extends Error {
  constructor(readonly service: string) {
    super(`no live node for service "${service}"`);
    this.name = 'ServiceUnavailableError';
  }
}

interface PendingRequest {
  sid: string;
  id: number;
  cmd: string;
  service: string;
  timer: NodeJS.Timeout;
  startedAt: number;
}

export interface BackendClientOptions {
  requestTimeoutMs: number;
  stickyTtlMs: number;
  /** How long a service's node list may be cached locally (ms). */
  nodeCacheMs?: number;
}

type TimeoutHandler = (info: { sid: string; id: number; cmd: string; service: string }) => void;

/**
 * Sends client traffic upstream to game / chat / ... services and tracks
 * outstanding requests so a dead backend surfaces as a timeout rather than a
 * hung client.
 *
 * Sticky routing: a uid is pinned to one node per service (redis binding +
 * rendezvous hashing as the fallback), so stateful services keep their
 * in-memory player state across requests and across gates.
 */
export class BackendClient {
  private readonly log = logger.child({ mod: 'backend' });
  private readonly pending = new Map<string, PendingRequest>();
  private readonly nodeCache = new Map<string, { nodes: NodeInfo[]; at: number }>();
  private readonly nodeCacheMs: number;
  private onTimeout: TimeoutHandler = () => undefined;

  constructor(
    private readonly bus: ClusterBus,
    private readonly registry: NodeRegistry,
    private readonly redis: Redis,
    private readonly keys: Keys,
    private readonly opts: BackendClientOptions,
  ) {
    this.nodeCacheMs = opts.nodeCacheMs ?? 1000;
  }

  setTimeoutHandler(handler: TimeoutHandler): void {
    this.onTimeout = handler;
  }

  /**
   * Publish one upstream message. Returns the node it was delivered to.
   * A stale sticky binding (node gone, or nobody listening) is dropped and
   * retried once against a freshly picked node.
   */
  async send(
    service: string,
    routingKey: string,
    msg: UpstreamMessage,
    sticky: boolean,
  ): Promise<string> {
    const node = await this.resolveNode(service, routingKey, sticky);
    const received = await this.bus.publishToServiceNode(service, node, msg);
    if (received > 0) return node;

    // Nobody was listening on that channel: the node died between our
    // registry read and the publish. Invalidate and retry once.
    this.log.warn({ service, node }, 'no subscriber on service channel, re-routing');
    await this.invalidate(service, routingKey, sticky);
    const retryNode = await this.resolveNode(service, routingKey, sticky);
    const retried = await this.bus.publishToServiceNode(service, retryNode, msg);
    if (retried === 0) throw new ServiceUnavailableError(service);
    return retryNode;
  }

  /** Track a request so it can be failed with a timeout if no reply arrives. */
  track(sid: string, id: number, cmd: string, service: string, timeoutMs?: number): void {
    const key = pendingKey(sid, id);
    const existing = this.pending.get(key);
    if (existing) clearTimeout(existing.timer);

    const ms = timeoutMs ?? this.opts.requestTimeoutMs;
    const timer = setTimeout(() => {
      this.pending.delete(key);
      this.log.warn({ sid, id, cmd, service, ms }, 'upstream request timed out');
      this.onTimeout({ sid, id, cmd, service });
    }, ms);
    timer.unref();

    this.pending.set(key, { sid, id, cmd, service, timer, startedAt: Date.now() });
  }

  /** Clear a tracked request. Returns latency in ms, or null if unknown/late. */
  settle(sid: string, id: number): number | null {
    const key = pendingKey(sid, id);
    const p = this.pending.get(key);
    if (!p) return null;
    clearTimeout(p.timer);
    this.pending.delete(key);
    return Date.now() - p.startedAt;
  }

  /** Cancel everything outstanding for a session (called on disconnect). */
  cancelSession(sid: string): void {
    for (const [key, p] of this.pending) {
      if (p.sid === sid) {
        clearTimeout(p.timer);
        this.pending.delete(key);
      }
    }
  }

  get pendingCount(): number {
    return this.pending.size;
  }

  /** Release a uid's sticky bindings so its next request re-picks a node. */
  async clearBindings(uid: string, services: string[]): Promise<void> {
    if (services.length === 0) return;
    await this.redis
      .del(...services.map((s) => this.keys.backendBinding(uid, s)))
      .catch((err: Error) => this.log.debug({ err: err.message }, 'binding cleanup failed'));
  }

  private async resolveNode(service: string, routingKey: string, sticky: boolean): Promise<string> {
    const nodes = await this.liveNodes(service);
    if (nodes.length === 0) throw new ServiceUnavailableError(service);

    if (!sticky) {
      const picked = nodes[Math.floor(Math.random() * nodes.length)] as NodeInfo;
      return picked.id;
    }

    const bindKey = this.keys.backendBinding(routingKey, service);
    const bound = await this.redis.get(bindKey);
    if (bound && nodes.some((n) => n.id === bound)) {
      // Sliding TTL: an active player keeps their pinning.
      this.redis.pexpire(bindKey, this.opts.stickyTtlMs).catch(() => undefined);
      return bound;
    }

    const chosen = pickNode(nodes, routingKey);
    if (!chosen) throw new ServiceUnavailableError(service);
    // NX so concurrent requests for the same uid agree on one node.
    const set = await this.redis.set(bindKey, chosen.id, 'PX', this.opts.stickyTtlMs, 'NX');
    if (set === null) {
      const winner = await this.redis.get(bindKey);
      if (winner && nodes.some((n) => n.id === winner)) return winner;
    }
    return chosen.id;
  }

  private async invalidate(service: string, routingKey: string, sticky: boolean): Promise<void> {
    this.nodeCache.delete(service);
    if (sticky) {
      await this.redis.del(this.keys.backendBinding(routingKey, service)).catch(() => undefined);
    }
  }

  private async liveNodes(service: string): Promise<NodeInfo[]> {
    const cached = this.nodeCache.get(service);
    if (cached && Date.now() - cached.at < this.nodeCacheMs) return cached.nodes;
    const nodes = await this.registry.listServiceNodes(service);
    this.nodeCache.set(service, { nodes, at: Date.now() });
    return nodes;
  }
}

function pendingKey(sid: string, id: number): string {
  return `${sid}:${id}`;
}
