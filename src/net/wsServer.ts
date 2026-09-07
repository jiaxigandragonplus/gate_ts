import { createServer, type IncomingMessage, type Server } from 'node:http';
import type { Duplex } from 'node:stream';
import { WebSocketServer, type WebSocket } from 'ws';
import { Connection, type ConnectionMeta } from './connection';
import { createTransportStats, type TransportStats } from './stats';
import { CloseCode, ErrorCode } from '../protocol/packet';
import { DecodeError, type Codec, type CodecName } from '../protocol/codec';
import { negotiateCodec } from '../protocol/codecs';
import type { ClientPacket } from '../protocol/packet';
import { logger } from '../util/logger';

export interface WsServerOptions {
  host: string;
  port: number;
  path: string;
  maxPayloadBytes: number;
  pingIntervalMs: number;
  pongTimeoutMs: number;
  authTimeoutMs: number;
  maxBackpressureBytes: number;
  perMessageDeflate: boolean;
  maxConnections: number;
  msgsPerSec: number;
  burst: number;
  /** Trust X-Forwarded-For (only enable behind your own load balancer). */
  trustProxy: boolean;
  /** Wire formats this gate serves. */
  codecs: readonly CodecName[];
  /** Used when the client expresses no preference. */
  defaultCodec: CodecName;
}

export interface WsServerHandlers {
  onPacket: (conn: Connection, packet: ClientPacket) => void;
  onClose: (conn: Connection, code: number, reason: string) => void;
  /** Return false to reject the upgrade (e.g. while draining). */
  onUpgrade?: (req: IncomingMessage) => boolean;
}

/**
 * WebSocket acceptor. Deals with the socket only: handshake filtering,
 * decoding, keepalive and the unauthenticated-connection timeout. Everything
 * above that is the gate's business.
 */
export class WsServer {
  private readonly log = logger.child({ mod: 'ws' });
  private readonly http: Server;
  private readonly wss: WebSocketServer;
  private readonly connections = new Set<Connection>();
  readonly stats: TransportStats = createTransportStats();
  private keepaliveTimer: NodeJS.Timeout | null = null;
  private accepting = true;
  /**
   * Codec chosen during `handleUpgrade`, read back when `handleProtocols` has
   * to answer with a subprotocol and again when the Connection is built.
   */
  private readonly negotiated = new WeakMap<IncomingMessage, { codec: Codec; subprotocol?: string }>();

  constructor(
    private readonly opts: WsServerOptions,
    private readonly handlers: WsServerHandlers,
  ) {
    this.http = createServer((req, res) => {
      // The admin/metrics API lives on its own port; this one only upgrades.
      res.writeHead(426, { 'content-type': 'text/plain' });
      res.end('this endpoint speaks WebSocket only\n');
    });
    this.wss = new WebSocketServer({
      noServer: true,
      maxPayload: opts.maxPayloadBytes,
      perMessageDeflate: opts.perMessageDeflate,
      clientTracking: false,
      // Echo back the subprotocol for the codec we already picked, so the
      // client can be certain which wire format is in use. Only ever echo one
      // the client actually offered - answering with an unrequested
      // subprotocol is a protocol violation, and can happen when the codec
      // came from ?codec= while the client offered something unrelated.
      handleProtocols: (protocols, req) => {
        const selected = this.negotiated.get(req)?.subprotocol;
        return selected !== undefined && protocols.has(selected) ? selected : false;
      },
    });
    this.http.on('upgrade', (req, socket, head) => this.handleUpgrade(req, socket, head));
    this.http.on('clientError', (_err, socket) => socket.destroy());
  }

  get connectionCount(): number {
    return this.connections.size;
  }

  async listen(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.http.once('error', reject);
      this.http.listen(this.opts.port, this.opts.host, () => {
        this.http.removeListener('error', reject);
        resolve();
      });
    });
    this.startKeepalive();
    this.log.info(
      { host: this.opts.host, port: this.opts.port, path: this.opts.path },
      'websocket server listening',
    );
  }

  private handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void {
    const url = new URL(req.url ?? '/', 'http://localhost');
    if (url.pathname !== this.opts.path) {
      return this.rejectUpgrade(socket, 404, 'Not Found');
    }
    if (!this.accepting) {
      return this.rejectUpgrade(socket, 503, 'Gate Draining');
    }
    if (this.opts.maxConnections > 0 && this.connections.size >= this.opts.maxConnections) {
      this.log.warn({ count: this.connections.size }, 'connection limit reached, rejecting upgrade');
      return this.rejectUpgrade(socket, 503, 'Server Full');
    }
    if (this.handlers.onUpgrade && !this.handlers.onUpgrade(req)) {
      return this.rejectUpgrade(socket, 403, 'Forbidden');
    }

    const codec = negotiateCodec({
      offered: subprotocols(req),
      query: url.searchParams.get('codec'),
      allowed: this.opts.codecs,
      fallback: this.opts.defaultCodec,
    });
    if (!codec.ok) {
      this.log.warn({ ip: req.socket.remoteAddress, reason: codec.reason }, 'codec negotiation failed');
      return this.rejectUpgrade(socket, 400, 'Unsupported Codec');
    }
    this.negotiated.set(req, {
      codec: codec.codec,
      ...(codec.subprotocol === undefined ? {} : { subprotocol: codec.subprotocol }),
    });

    const meta: ConnectionMeta = {
      ip: clientIp(req, this.opts.trustProxy),
      ...(req.headers['user-agent'] ? { userAgent: String(req.headers['user-agent']) } : {}),
      ...(url.searchParams.get('token') ? { handshakeToken: url.searchParams.get('token') as string } : {}),
    };

    this.wss.handleUpgrade(req, socket, head, (ws) => this.register(ws, meta, codec.codec));
  }

  private rejectUpgrade(socket: Duplex, status: number, message: string): void {
    this.stats.rejected += 1;
    socket.write(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\n\r\n`);
    socket.destroy();
  }

  private register(ws: WebSocket, meta: ConnectionMeta, codec: Codec): void {
    const conn = new Connection(ws, meta, {
      maxBackpressureBytes: this.opts.maxBackpressureBytes,
      msgsPerSec: this.opts.msgsPerSec,
      burst: this.opts.burst,
      stats: this.stats,
      codec,
    });
    this.connections.add(conn);
    this.stats.accepted += 1;

    // An unauthenticated socket is a liability: close it if it does not
    // present a token quickly.
    const authTimer = setTimeout(() => {
      if (conn.state === 'unauthenticated') {
        this.log.debug({ ip: conn.ip }, 'closing socket: auth timeout');
        conn.close(CloseCode.AuthTimeout, 'auth timeout');
      }
    }, this.opts.authTimeoutMs);
    authTimer.unref();

    ws.on('message', (data: Buffer | ArrayBuffer | Buffer[]) => {
      const raw = normalize(data);
      conn.markRecv(raw.length);

      if (!conn.allowInbound()) {
        this.stats.rateLimited += 1;
        this.log.warn({ ip: conn.ip, uid: conn.session?.uid }, 'rate limit exceeded');
        conn.sendError(ErrorCode.RateLimited, 'too many messages');
        conn.close(CloseCode.RateLimited, 'rate limited');
        return;
      }

      let packet: ClientPacket;
      try {
        // The codec is fixed per connection, so frame type does not matter:
        // a JSON client may use binary frames and vice versa.
        packet = conn.codec.decode(raw);
      } catch (err) {
        this.stats.protocolErrors += 1;
        const message = err instanceof DecodeError ? err.message : 'malformed packet';
        this.log.debug({ ip: conn.ip, err: message }, 'protocol error');
        conn.sendError(ErrorCode.BadRequest, message);
        conn.close(CloseCode.ProtocolError, 'protocol error');
        return;
      }

      try {
        this.handlers.onPacket(conn, packet);
      } catch (err) {
        this.log.error({ err: (err as Error).message, t: packet.t }, 'packet handler threw');
        conn.sendError(ErrorCode.Internal, 'internal error');
      }
    });

    ws.on('pong', () => {
      conn.lastRecvAt = Date.now();
    });

    ws.on('error', (err: Error) => {
      this.log.debug({ ip: conn.ip, err: err.message }, 'socket error');
    });

    ws.on('close', (code: number, reasonBuf: Buffer) => {
      clearTimeout(authTimer);
      conn.markClosed();
      this.connections.delete(conn);
      this.stats.closed += 1;
      this.handlers.onClose(conn, code, reasonBuf.toString('utf8'));
    });
  }

  /**
   * One timer for every connection: send pings, and drop sockets that have
   * been silent for longer than the pong timeout (dead TCP connections do not
   * always produce a close event).
   */
  private startKeepalive(): void {
    this.keepaliveTimer = setInterval(() => {
      const deadline = Date.now() - this.opts.pongTimeoutMs;
      for (const conn of this.connections) {
        if (conn.lastRecvAt < deadline) {
          this.log.debug({ ip: conn.ip, uid: conn.session?.uid }, 'idle timeout');
          conn.close(CloseCode.IdleTimeout, 'idle timeout');
          continue;
        }
        conn.ping();
      }
    }, this.opts.pingIntervalMs);
    this.keepaliveTimer.unref();
  }

  /** Stop accepting new sockets but keep serving the existing ones. */
  stopAccepting(): void {
    this.accepting = false;
  }

  async close(): Promise<void> {
    this.accepting = false;
    if (this.keepaliveTimer) {
      clearInterval(this.keepaliveTimer);
      this.keepaliveTimer = null;
    }
    for (const conn of this.connections) conn.terminate();
    this.connections.clear();
    await new Promise<void>((resolve) => this.wss.close(() => resolve()));
    await new Promise<void>((resolve) => this.http.close(() => resolve()));
  }
}

function normalize(data: Buffer | ArrayBuffer | Buffer[]): Buffer {
  if (Buffer.isBuffer(data)) return data;
  if (Array.isArray(data)) return Buffer.concat(data);
  return Buffer.from(data);
}

/** `Sec-WebSocket-Protocol` values, in the client's preference order. */
function subprotocols(req: IncomingMessage): string[] {
  const header = req.headers['sec-websocket-protocol'];
  if (!header) return [];
  const raw = Array.isArray(header) ? header.join(',') : header;
  return raw.split(',').map((s) => s.trim()).filter(Boolean);
}

function clientIp(req: IncomingMessage, trustProxy: boolean): string {
  if (trustProxy) {
    const fwd = req.headers['x-forwarded-for'];
    const first = Array.isArray(fwd) ? fwd[0] : fwd;
    if (first) {
      const ip = first.split(',')[0]?.trim();
      if (ip) return ip;
    }
  }
  return req.socket.remoteAddress ?? 'unknown';
}
