import { RedisClusterBus, RedisServiceBus } from '../redis/bus';
import type { RedisPool } from '../redis/client';
import type { Keys } from '../redis/keys';
import { NatsClient, NatsClusterBus, NatsServiceBus, Subjects } from '../nats';
import type { NatsOptions } from '../nats/connection';
import type { ClusterBus, ServiceBus, TransportKind } from './types';

export * from './types';

/**
 * What a node needs to build either transport.
 *
 * Redis is listed unconditionally because it stays the state layer either
 * way - session ownership and the node registry live there. NATS only ever
 * carries messages; mixing the two roles is what makes a cluster hard to
 * reason about.
 */
export interface TransportConfig {
  kind: TransportKind;
  redis: { pool: RedisPool; keys: Keys };
  nats: { options: NatsOptions; subjectPrefix: string };
}

export function createClusterBus(cfg: TransportConfig, gateId: string): ClusterBus {
  if (cfg.kind === 'nats') {
    return new NatsClusterBus(
      new NatsClient(cfg.nats.options),
      new Subjects(cfg.nats.subjectPrefix),
      gateId,
    );
  }
  return new RedisClusterBus(cfg.redis.pool, cfg.redis.keys, gateId);
}

export function createServiceBus(
  cfg: TransportConfig,
  service: string,
  nodeId: string,
): ServiceBus {
  if (cfg.kind === 'nats') {
    return new NatsServiceBus(
      new NatsClient(cfg.nats.options),
      new Subjects(cfg.nats.subjectPrefix),
      service,
      nodeId,
    );
  }
  return new RedisServiceBus(cfg.redis.pool, cfg.redis.keys, service, nodeId);
}
