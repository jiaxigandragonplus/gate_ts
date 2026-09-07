import { hostname } from 'node:os';
import * as dotenv from 'dotenv';
import { normalizeNodeId } from '../framework/nats/subjects';
import type { TransportKind } from '../framework/transport/types';

dotenv.config();

function str(key: string, def: string): string {
  const v = process.env[key];
  return v === undefined || v === '' ? def : v;
}

function optStr(key: string): string | undefined {
  const v = process.env[key];
  return v === undefined || v === '' ? undefined : v;
}

function int(key: string, def: number): number {
  const v = process.env[key];
  if (v === undefined || v === '') return def;
  const n = Number.parseInt(v, 10);
  if (Number.isNaN(n)) throw new Error(`env ${key} must be an integer, got "${v}"`);
  return n;
}

function bool(key: string, def: boolean): boolean {
  const v = process.env[key];
  if (v === undefined || v === '') return def;
  return v === '1' || v.toLowerCase() === 'true';
}

export type StoreKind = 'redis' | 'memory';

export interface GameConfig {
  /** Service name clients route to; must match the gate's route table. */
  service: string;
  nodeId: string;

  redis: { url: string; keyPrefix: string };
  cluster: { transport: TransportKind; heartbeatMs: number; nodeTtlMs: number };
  nats: { servers: string[]; subjectPrefix: string };

  player: {
    /**
     * How long a player stays in memory after going offline. Should be >= the
     * gate's resume window, so a reconnecting player finds their state warm
     * instead of paying a reload.
     */
    unloadDelayMs: number;
    /** Periodic flush of dirty players. */
    saveIntervalMs: number;
    /** Max queued messages per player before new ones are rejected. */
    mailboxLimit: number;
    /** Warn when one message holds a player's mailbox longer than this. */
    slowHandlerMs: number;
    /** Ownership lease TTL; refreshed while the player is loaded. */
    leaseTtlMs: number;
  };

  /** Interval for system tick hooks; 0 disables ticking. */
  tickIntervalMs: number;

  store: { kind: StoreKind };
  admin: { host: string; port: number };
  shutdownGraceMs: number;
}

export function loadGameConfig(): GameConfig {
  const service = str('GAME_SERVICE', 'game');
  const transport = str('CLUSTER_TRANSPORT', 'nats').trim().toLowerCase();
  if (transport !== 'redis' && transport !== 'nats') {
    throw new Error(`CLUSTER_TRANSPORT must be "redis" or "nats", got "${transport}"`);
  }
  const storeKind = str('GAME_STORE', 'redis').trim().toLowerCase();
  if (storeKind !== 'redis' && storeKind !== 'memory') {
    throw new Error(`GAME_STORE must be "redis" or "memory", got "${storeKind}"`);
  }

  return {
    service: normalizeNodeId(service),
    // Normalized once: the same id is a redis field, a NATS subject token,
    // a log field and a metric label.
    nodeId: normalizeNodeId(str('GAME_NODE_ID', `${service}-${hostname()}-${process.pid}`)),

    redis: {
      url: str('REDIS_URL', 'redis://127.0.0.1:6379'),
      keyPrefix: str('REDIS_KEY_PREFIX', 'gate'),
    },
    cluster: {
      transport,
      heartbeatMs: int('CLUSTER_HEARTBEAT_MS', 5_000),
      nodeTtlMs: int('CLUSTER_NODE_TTL_MS', 15_000),
    },
    nats: {
      servers: str('NATS_SERVERS', 'nats://127.0.0.1:4222')
        .split(',')
        .map((v) => v.trim())
        .filter(Boolean),
      subjectPrefix: str('NATS_SUBJECT_PREFIX', 'gate'),
    },

    player: {
      unloadDelayMs: int('GAME_UNLOAD_DELAY_MS', 90_000),
      saveIntervalMs: int('GAME_SAVE_INTERVAL_MS', 30_000),
      mailboxLimit: int('GAME_MAILBOX_LIMIT', 64),
      slowHandlerMs: int('GAME_SLOW_HANDLER_MS', 200),
      leaseTtlMs: int('GAME_LEASE_TTL_MS', 60_000),
    },

    tickIntervalMs: int('GAME_TICK_INTERVAL_MS', 1_000),
    store: { kind: storeKind },
    admin: {
      host: str('GAME_ADMIN_HOST', '127.0.0.1'),
      port: int('GAME_ADMIN_PORT', 9000),
    },
    shutdownGraceMs: int('SHUTDOWN_GRACE_MS', 10_000),
  };
}

export const gameEnvFlags = { verboseHandlers: bool('GAME_LOG_HANDLERS', false) };
export const optionalEnv = { storeKeyPrefix: optStr('GAME_STORE_PREFIX') };
