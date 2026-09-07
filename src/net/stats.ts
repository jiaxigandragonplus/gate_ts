/**
 * Transport-level counters, shared by the WebSocket acceptor and every
 * connection it creates. Kept separate from `Metrics` so the net layer does
 * not need to know about the metrics registry - the gate exposes these as
 * gauges at startup.
 */
export interface TransportStats {
  accepted: number;
  closed: number;
  /** Upgrades refused (wrong path, draining, connection limit). */
  rejected: number;
  packetsRecv: number;
  packetsSent: number;
  bytesRecv: number;
  bytesSent: number;
  rateLimited: number;
  protocolErrors: number;
  backpressureDrops: number;
}

export function createTransportStats(): TransportStats {
  return {
    accepted: 0,
    closed: 0,
    rejected: 0,
    packetsRecv: 0,
    packetsSent: 0,
    bytesRecv: 0,
    bytesSent: 0,
    rateLimited: 0,
    protocolErrors: 0,
    backpressureDrops: 0,
  };
}
