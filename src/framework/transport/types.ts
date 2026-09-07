import type { DownstreamMessage, UpstreamMessage } from '../protocol/internal';

export type TransportKind = 'redis' | 'nats';

/**
 * What a publish could establish about delivery.
 *
 *  - `delivered`      at least one subscriber received it
 *  - `no-subscriber`  nobody was listening; the caller should re-route
 *  - `unknown`        the transport cannot tell (plain fire-and-forget)
 *
 * Redis pub/sub reports a subscriber count, so it answers precisely. Core
 * NATS `publish` is fire-and-forget and answers `unknown`; callers fall back
 * to the request timeout there. (Switching the upstream path to NATS
 * request/reply would report `no-subscriber` instantly - see docs.)
 */
export type Delivery = 'delivered' | 'no-subscriber' | 'unknown';

export type DownstreamHandler = (msg: DownstreamMessage) => void;
export type UpstreamHandler = (msg: UpstreamMessage) => void;

/** Both sides can send messages towards gates. */
export interface GatePublisher {
  publishToGate(gateId: string, msg: DownstreamMessage): Promise<void>;
  /** Reaches every gate, including the caller if it is one. */
  publishToAllGates(msg: DownstreamMessage): Promise<void>;
}

/**
 * The gate's view of the cluster: it consumes downstream traffic addressed to
 * itself and sends upstream traffic to service nodes.
 */
export interface ClusterBus extends GatePublisher {
  readonly kind: TransportKind;
  /** Subscribe this node's inbox and the cluster-wide channel. */
  start(handler: DownstreamHandler): Promise<void>;
  publishToServiceNode(
    service: string,
    nodeId: string,
    msg: UpstreamMessage,
  ): Promise<Delivery>;
  stop(): Promise<void>;
  /**
   * Transport-level counters for /metrics. Optional because there is nothing
   * useful to report for redis pub/sub beyond what the redis server exposes
   * itself; for NATS this is where slow consumers and reconnects surface.
   */
  counters?(): Record<string, number>;
}

/**
 * A backend service's view: it consumes upstream traffic addressed to itself
 * and answers (or pushes) towards gates.
 */
export interface ServiceBus extends GatePublisher {
  readonly kind: TransportKind;
  start(handler: UpstreamHandler): Promise<void>;
  stop(): Promise<void>;
}
