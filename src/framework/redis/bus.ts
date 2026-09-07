import type { RedisPool } from './client';
import type { Keys } from './keys';
import type { DownstreamMessage, UpstreamMessage } from '../protocol/internal';
import { isDownstreamMessage } from '../protocol/internal';
import { logger } from '../util/logger';

export type DownstreamHandler = (msg: DownstreamMessage) => void;

/**
 * Redis pub/sub transport for cluster traffic.
 *
 * Each gate subscribes to exactly two channels: its own inbox (responses,
 * targeted pushes, takeover kicks) and the cluster-wide broadcast channel.
 * Upstream traffic is published to a specific service node's channel, which
 * keeps request fan-out at zero.
 */
export class ClusterBus {
  private readonly log = logger.child({ mod: 'bus' });
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

  /** Returns the number of subscribers that received the message. */
  async publishToServiceNode(
    service: string,
    nodeId: string,
    msg: UpstreamMessage,
  ): Promise<number> {
    return this.pool.pub.publish(this.keys.serviceChannel(service, nodeId), JSON.stringify(msg));
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    this.handler = null;
    await this.pool.sub.unsubscribe().catch(() => undefined);
  }
}
