import { createHash } from 'node:crypto';
import type Redis from 'ioredis';
import type { Keys } from './keys';
import { logger } from '../util/logger';

export interface NodeInfo {
  id: string;
  addr: string;
  /** Live connection count (gates) or load hint (services). */
  load: number;
  /** Last heartbeat, ms since epoch. */
  ts: number;
  meta?: Record<string, unknown>;
}

/**
 * Membership for both gate nodes and backend service nodes.
 *
 * Liveness is heartbeat-based rather than TTL-based because everything lives
 * in one hash per group; stale entries are filtered on read and pruned
 * opportunistically.
 */
export class NodeRegistry {
  private readonly log = logger.child({ mod: 'node-registry' });

  constructor(
    private readonly redis: Redis,
    private readonly keys: Keys,
    private readonly nodeTtlMs: number,
  ) {}

  async heartbeatGate(info: NodeInfo): Promise<void> {
    await this.redis.hset(this.keys.gateNodes(), info.id, JSON.stringify({ ...info, ts: Date.now() }));
  }

  async unregisterGate(id: string): Promise<void> {
    await this.redis.hdel(this.keys.gateNodes(), id);
  }

  async listGates(): Promise<NodeInfo[]> {
    return this.list(this.keys.gateNodes());
  }

  async heartbeatService(service: string, info: NodeInfo): Promise<void> {
    await this.redis.hset(
      this.keys.serviceNodes(service),
      info.id,
      JSON.stringify({ ...info, ts: Date.now() }),
    );
  }

  async unregisterService(service: string, id: string): Promise<void> {
    await this.redis.hdel(this.keys.serviceNodes(service), id);
  }

  async listServiceNodes(service: string): Promise<NodeInfo[]> {
    return this.list(this.keys.serviceNodes(service));
  }

  private async list(key: string): Promise<NodeInfo[]> {
    const all = await this.redis.hgetall(key);
    const alive: NodeInfo[] = [];
    const dead: string[] = [];
    const cutoff = Date.now() - this.nodeTtlMs;
    for (const [id, raw] of Object.entries(all)) {
      try {
        const info = JSON.parse(raw) as NodeInfo;
        if (info.ts >= cutoff) alive.push(info);
        else dead.push(id);
      } catch {
        dead.push(id);
      }
    }
    if (dead.length > 0) {
      this.redis
        .hdel(key, ...dead)
        .catch((err: Error) => this.log.debug({ err: err.message }, 'prune failed'));
    }
    return alive;
  }
}

/**
 * Rendezvous (highest-random-weight) hashing: a uid keeps mapping to the same
 * service node as long as that node is alive, and only 1/N of accounts move
 * when the node set changes.
 */
export function pickNode(nodes: NodeInfo[], routingKey: string): NodeInfo | null {
  let best: NodeInfo | null = null;
  let bestScore = '';
  for (const node of nodes) {
    const score = createHash('sha1').update(`${routingKey}|${node.id}`).digest('hex');
    if (best === null || score > bestScore) {
      best = node;
      bestScore = score;
    }
  }
  return best;
}
