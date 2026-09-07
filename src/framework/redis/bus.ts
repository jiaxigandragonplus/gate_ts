import type { RedisPool } from './client';
import type { Keys } from './keys';
import type { DownstreamMessage, UpstreamMessage } from '../protocol/internal';
import { isDownstreamMessage } from '../protocol/internal';
import type {
  ClusterBus,
  Delivery,
  DownstreamHandler,
  ServiceBus,
  UpstreamHandler,
} from '../transport/types';
import { logger } from '../util/logger';

/**
 * Redis pub/sub transport for cluster traffic.
 *
 * Each gate subscribes to exactly two channels: its own inbox (responses,
 * targeted pushes, takeover kicks) and the cluster-wide broadcast channel.
 * Upstream traffic is published to a specific service node's channel, which
 * keeps request fan-out at zero.
 */
export class RedisClusterBus implements ClusterBus {
  readonly kind = 'redis' as const;
  private readonly log = logger.child({ mod: 'bus', transport: 'redis' });
  private handler: DownstreamHandler | null = null;
  private started = false;

  constructor(
    private readonly pool: RedisPool,
    private readonly keys: Keys,
    private readonly gateId: string,
  ) {}

  async start(handler: DownstreamHandler): Promise<void> {
    if (this.started) throw new Error('bus already started');
    this.handler = handler;
    this.started = true;

    this.pool.sub.on('message', (channel: string, payload: string) => {
      this.dispatch(channel, payload);
    });

    const channels = [this.keys.nodeChannel(this.gateId), this.keys.allNodesChannel()];
    await this.pool.sub.subscribe(...channels);
    this.log.info({ channels }, 'subscribed to cluster channels');
  }

  private dispatch(channel: string, payload: string): void {
    if (!this.handler) return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload);
    } catch {
      this.log.warn({ channel }, 'dropping non-JSON cluster message');
      return;
    }
    if (!isDownstreamMessage(parsed)) {
      this.log.warn({ channel, kind: (parsed as { k?: unknown })?.k }, 'dropping unknown cluster message');
      return;
    }
    try {
      this.handler(parsed);
    } catch (err) {
      this.log.error({ err: (err as Error).message, kind: parsed.k }, 'cluster handler threw');
    }
  }

  async publishToGate(gateId: string, msg: DownstreamMessage): Promise<void> {
    await this.pool.pub.publish(this.keys.nodeChannel(gateId), JSON.stringify(msg));
  }

  /** Reaches every gate, including this one. */
  async publishToAllGates(msg: DownstreamMessage): Promise<void> {
    await this.pool.pub.publish(this.keys.allNodesChannel(), JSON.stringify(msg));
  }

  async publishToServiceNode(
    service: string,
    nodeId: string,
    msg: UpstreamMessage,
  ): Promise<Delivery> {
    // Redis pub/sub reports how many subscribers got it, which lets a dead
    // node be detected before the request times out.
    const received = await this.pool.pub.publish(
      this.keys.serviceChannel(service, nodeId),
      JSON.stringify(msg),
    );
    return received > 0 ? 'delivered' : 'no-subscriber';
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    this.handler = null;
    await this.pool.sub.unsubscribe().catch(() => undefined);
  }
}

/**
 * Redis transport, backend service side. Mirror image of the cluster bus:
 * consumes one service node's channel, publishes towards gates.
 */
export class RedisServiceBus implements ServiceBus {
  readonly kind = 'redis' as const;
  private readonly log = logger.child({ mod: 'bus', transport: 'redis' });
  private started = false;

  constructor(
    private readonly pool: RedisPool,
    private readonly keys: Keys,
    private readonly service: string,
    private readonly nodeId: string,
  ) {}

  async start(handler: UpstreamHandler): Promise<void> {
    if (this.started) throw new Error('bus already started');
    this.started = true;

    this.pool.sub.on('message', (_channel: string, payload: string) => {
      let parsed: UpstreamMessage;
      try {
        parsed = JSON.parse(payload) as UpstreamMessage;
      } catch {
        this.log.warn('dropping non-JSON upstream message');
        return;
      }
      try {
        handler(parsed);
      } catch (err) {
        this.log.error({ err: (err as Error).message }, 'upstream handler threw');
      }
    });

    const channel = this.keys.serviceChannel(this.service, this.nodeId);
    await this.pool.sub.subscribe(channel);
    this.log.info({ channel }, 'service node subscribed');
  }

  async publishToGate(gateId: string, msg: DownstreamMessage): Promise<void> {
    await this.pool.pub.publish(this.keys.nodeChannel(gateId), JSON.stringify(msg));
  }

  async publishToAllGates(msg: DownstreamMessage): Promise<void> {
    await this.pool.pub.publish(this.keys.allNodesChannel(), JSON.stringify(msg));
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    await this.pool.sub.unsubscribe().catch(() => undefined);
  }
}
