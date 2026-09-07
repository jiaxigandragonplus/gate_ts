import type { Subscription } from '@nats-io/nats-core';
import type { NatsClient } from './connection';
import type { Subjects } from './subjects';
import {
  isDownstreamMessage,
  type DownstreamMessage,
  type UpstreamMessage,
} from '../protocol/internal';
import type {
  ClusterBus,
  Delivery,
  DownstreamHandler,
  ServiceBus,
  UpstreamHandler,
} from '../transport/types';
import { logger } from '../util/logger';

/**
 * NATS transport, gate side.
 *
 * Two subscriptions per gate: its own inbox and the cluster-wide subject.
 * Upstream traffic is published straight to one service node's subject, so
 * fan-out stays at zero.
 */
export class NatsClusterBus implements ClusterBus {
  readonly kind = 'nats' as const;
  private readonly log = logger.child({ mod: 'bus', transport: 'nats' });
  private subs: Subscription[] = [];
  private started = false;

  constructor(
    private readonly client: NatsClient,
    private readonly subjects: Subjects,
    private readonly gateId: string,
  ) {}

  async start(handler: DownstreamHandler): Promise<void> {
    if (this.started) throw new Error('bus already started');
    this.started = true;
    await this.client.connect();

    const dispatch = (msg: unknown): void => {
      // Same hardening as the redis transport: anything that is not a known
      // message shape is dropped rather than handed to the gate.
      if (!isDownstreamMessage(msg)) {
        this.log.warn({ kind: (msg as { k?: unknown })?.k }, 'dropping unknown cluster message');
        return;
      }
      handler(msg);
    };

    const own = this.subjects.node(this.gateId);
    const all = this.subjects.allNodes();
    // No queue group here: a queue group would hand each broadcast to exactly
    // one gate, which is the opposite of what a broadcast means.
    this.subs.push(this.client.subscribe<unknown>(own, dispatch));
    this.subs.push(this.client.subscribe<unknown>(all, dispatch));
    this.log.info({ subjects: [own, all] }, 'subscribed to cluster subjects');
  }

  async publishToGate(gateId: string, msg: DownstreamMessage): Promise<void> {
    this.client.publish(this.subjects.node(gateId), msg);
  }

  async publishToAllGates(msg: DownstreamMessage): Promise<void> {
    this.client.publish(this.subjects.allNodes(), msg);
  }

  async publishToServiceNode(
    service: string,
    nodeId: string,
    msg: UpstreamMessage,
  ): Promise<Delivery> {
    this.client.publish(this.subjects.serviceNode(service, nodeId), msg);
    // Core NATS publish is fire-and-forget: liveness comes from the node
    // registry heartbeat and, failing that, the request timeout.
    return 'unknown';
  }

  counters(): Record<string, number> {
    const s = this.client.stats();
    return {
      connected: s.connected ? 1 : 0,
      reconnects: s.reconnects,
      disconnects: s.disconnects,
      // The metric to alert on: a slow consumer means dropped messages.
      slow_consumers: s.slowConsumers,
      errors: s.errors,
      in_msgs: s.inMsgs,
      out_msgs: s.outMsgs,
      in_bytes: s.inBytes,
      out_bytes: s.outBytes,
    };
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    for (const sub of this.subs) {
      await sub.drain().catch(() => undefined);
    }
    this.subs = [];
    await this.client.close();
  }
}

/**
 * NATS transport, backend service side. Mirror image of the above: consumes
 * one node's inbox, publishes towards gates.
 */
export class NatsServiceBus implements ServiceBus {
  readonly kind = 'nats' as const;
  private readonly log = logger.child({ mod: 'bus', transport: 'nats' });
  private sub: Subscription | null = null;
  private started = false;

  constructor(
    private readonly client: NatsClient,
    private readonly subjects: Subjects,
    private readonly service: string,
    private readonly nodeId: string,
  ) {}

  async start(handler: UpstreamHandler): Promise<void> {
    if (this.started) throw new Error('bus already started');
    this.started = true;
    await this.client.connect();

    const subject = this.subjects.serviceNode(this.service, this.nodeId);
    this.sub = this.client.subscribe<UpstreamMessage>(subject, handler);
    this.log.info({ subject }, 'service node subscribed');
  }

  async publishToGate(gateId: string, msg: DownstreamMessage): Promise<void> {
    this.client.publish(this.subjects.node(gateId), msg);
  }

  async publishToAllGates(msg: DownstreamMessage): Promise<void> {
    this.client.publish(this.subjects.allNodes(), msg);
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    await this.sub?.drain().catch(() => undefined);
    this.sub = null;
    await this.client.close();
  }
}
