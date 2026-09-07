import type { WebSocket } from 'ws';
import { CloseCode, ErrorCode, PacketType, type ServerPacket } from '../../framework/protocol/packet';
import type { Codec } from '../../framework/protocol/codec';
import { TokenBucket } from '../../framework/util/rateLimiter';
import { nextConnId } from '../../framework/util/id';
import type { Session } from '../session/session';
import type { TransportStats } from './stats';

export type ConnectionState = 'unauthenticated' | 'bound' | 'closing' | 'closed';

export interface ConnectionOptions {
  maxBackpressureBytes: number;
  msgsPerSec: number;
  burst: number;
  /** Shared with the acceptor so process-wide totals stay in one place. */
  stats: TransportStats;
  /** Wire format negotiated for this connection (json / protobuf). */
  codec: Codec;
}

export interface ConnectionMeta {
  ip: string;
  userAgent?: string;
  /** Value of the `token` query parameter, when auth is done at handshake time. */
  handshakeToken?: string;
}

/**
 * One live WebSocket. Owns transport concerns only - framing, keepalive,
 * backpressure and rate limiting. It knows nothing about routing, and holds a
 * reference to its `Session` once authenticated.
 */
export class Connection {
  readonly id = nextConnId();
  readonly connectedAt = Date.now();
  readonly ip: string;
  readonly userAgent?: string;
  readonly handshakeToken?: string;

  state: ConnectionState = 'unauthenticated';
  session: Session | null = null;
  /** Last time anything (frame or pong) was received; drives idle timeout. */
  lastRecvAt = Date.now();
  bytesSent = 0;
  bytesRecv = 0;
  packetsSent = 0;
  packetsRecv = 0;

  private readonly bucket: TokenBucket;

  constructor(
    private readonly ws: WebSocket,
    meta: ConnectionMeta,
    private readonly opts: ConnectionOptions,
  ) {
    this.ip = meta.ip;
    if (meta.userAgent !== undefined) this.userAgent = meta.userAgent;
    if (meta.handshakeToken !== undefined) this.handshakeToken = meta.handshakeToken;
    this.bucket = new TokenBucket(opts.burst, opts.msgsPerSec);
  }

  /** Wire format this connection speaks. */
  get codec(): Codec {
    return this.opts.codec;
  }

  get closed(): boolean {
    return this.state === 'closed' || this.state === 'closing';
  }

  get bufferedBytes(): number {
    return this.ws.bufferedAmount;
  }

  /** Charge one inbound packet against the rate limit. */
  allowInbound(): boolean {
    return this.bucket.tryConsume(1);
  }

  markRecv(bytes: number): void {
    this.lastRecvAt = Date.now();
    this.bytesRecv += bytes;
    this.packetsRecv += 1;
    this.opts.stats.packetsRecv += 1;
    this.opts.stats.bytesRecv += bytes;
  }

  /**
   * Serialize and enqueue a packet. Returns false when the packet was
   * dropped, which only happens if the socket is gone or so far behind that
   * we chose to kill it instead of buffering without bound.
   */
  send(packet: ServerPacket): boolean {
    if (this.closed || this.ws.readyState !== 1) return false;

    if (this.ws.bufferedAmount > this.opts.maxBackpressureBytes) {
      // The client cannot keep up. Dropping the connection is the only way to
      // stop unbounded memory growth; a resume will re-sync it.
      this.opts.stats.backpressureDrops += 1;
      this.close(CloseCode.RateLimited, 'backpressure');
      return false;
    }

    const frame = this.opts.codec.encode(packet);
    // ws picks the frame type from the argument: Buffer -> binary, string -> text.
    this.ws.send(frame);
    const size = typeof frame === 'string' ? Buffer.byteLength(frame) : frame.length;
    this.bytesSent += size;
    this.packetsSent += 1;
    this.opts.stats.packetsSent += 1;
    this.opts.stats.bytesSent += size;
    return true;
  }

  sendError(code: ErrorCode, message: string, requestId?: number): void {
    this.send({
      t: PacketType.Error,
      e: code,
      m: message,
      ...(requestId === undefined ? {} : { id: requestId }),
    });
  }

  ping(): void {
    if (this.ws.readyState === 1) this.ws.ping();
  }

  close(code: CloseCode, reason = ''): void {
    if (this.closed) return;
    this.state = 'closing';
    try {
      this.ws.close(code, reason.slice(0, 120));
    } catch {
      this.terminate();
    }
    // A client that never sends the close handshake back must not linger.
    const t = setTimeout(() => this.terminate(), 3000);
    t.unref();
  }

  /** Abort immediately, no close handshake. */
  terminate(): void {
    this.state = 'closed';
    try {
      this.ws.terminate();
    } catch {
      /* already gone */
    }
  }

  markClosed(): void {
    this.state = 'closed';
  }
}
