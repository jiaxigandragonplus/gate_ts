import { readFileSync } from 'node:fs';
import { connect } from '@nats-io/transport-node';
// nats.js v3 splits the driver from the protocol core: `connect` is
// transport-specific, everything else lives in nats-core.
import {
  credsAuthenticator,
  type NatsConnection,
  type Subscription,
} from '@nats-io/nats-core';
import { logger } from '../util/logger';

export interface NatsOptions {
  /** Server list, e.g. ["nats://a:4222", "nats://b:4222"]. */
  servers: string[];
  /** Shows up in the server's connz output; make it the node id. */
  name: string;
  token?: string;
  user?: string;
  pass?: string;
  /** Path to a .creds file (NATS accounts / NGS). */
  credsFile?: string;
  tls?: boolean;
  /** -1 keeps retrying forever, which is what a server node wants. */
  maxReconnectAttempts?: number;
  reconnectWaitMs?: number;
  pingIntervalMs?: number;
  /** Default timeout for request/reply. */
  requestTimeoutMs?: number;
}

export interface NatsStats {
  connected: boolean;
  server: string;
  reconnects: number;
  disconnects: number;
  slowConsumers: number;
  errors: number;
  inMsgs: number;
  outMsgs: number;
  inBytes: number;
  outBytes: number;
}

/**
 * Thin wrapper over the NATS client.
 *
 * Exists so the rest of the codebase never imports the driver: it owns
 * connection options, the status stream (reconnects, slow consumers - the two
 * things that actually page you), JSON encoding of internal messages, and an
 * orderly drain on shutdown.
 */
export class NatsClient {
  private readonly log = logger.child({ mod: 'nats' });
  private nc: NatsConnection | null = null;
  private reconnects = 0;
  private disconnects = 0;
  private slowConsumers = 0;
  private errors = 0;
  private closing = false;

  constructor(private readonly opts: NatsOptions) {
    if (opts.servers.length === 0) throw new Error('NatsClient needs at least one server');
  }

  get connected(): boolean {
    return this.nc !== null && !this.nc.isClosed();
  }

  get connection(): NatsConnection {
    if (!this.nc) throw new Error('NATS client is not connected; call connect() first');
    return this.nc;
  }

  async connect(): Promise<void> {
    if (this.nc) return;
    const nc = await connect({
      servers: this.opts.servers,
      name: this.opts.name,
      // A server node should keep trying indefinitely rather than exit.
      maxReconnectAttempts: this.opts.maxReconnectAttempts ?? -1,
      reconnectTimeWait: this.opts.reconnectWaitMs ?? 250,
      reconnectJitter: 100,
      pingInterval: this.opts.pingIntervalMs ?? 20_000,
      // Buffer publishes while reconnecting instead of throwing at callers.
      reconnect: true,
      ...(this.opts.tls ? { tls: {} } : {}),
      ...(this.opts.token ? { token: this.opts.token } : {}),
      ...(this.opts.user ? { user: this.opts.user, pass: this.opts.pass } : {}),
      ...(this.opts.credsFile
        ? { authenticator: credsAuthenticator(readFileSync(this.opts.credsFile)) }
        : {}),
    });

    this.nc = nc;
    this.log.info({ server: nc.getServer(), name: this.opts.name }, 'connected to nats');
    void this.watchStatus(nc);
    void nc.closed().then((err: void | Error) => {
      if (err instanceof Error && !this.closing) {
        this.log.error({ err: err.message }, 'nats connection closed with error');
      }
    });
  }

  /**
   * The status stream is the only place reconnects and slow consumers are
   * reported, so it must be consumed - an unconsumed iterator hides exactly
   * the problems you want alerts on.
   */
  private async watchStatus(nc: NatsConnection): Promise<void> {
    for await (const status of nc.status()) {
      switch (status.type) {
        case 'disconnect':
          this.disconnects += 1;
          this.log.warn({ server: status.server }, 'nats disconnected');
          break;
        case 'reconnect':
          this.reconnects += 1;
          this.log.info({ server: status.server }, 'nats reconnected');
          break;
        case 'reconnecting':
          this.log.warn('nats reconnecting');
          break;
        case 'slowConsumer':
          this.slowConsumers += 1;
          this.log.error(
            { subject: (status as { subject?: string }).subject },
            'nats slow consumer: messages are being dropped for this subscription',
          );
          break;
        case 'error':
          this.errors += 1;
          this.log.error({ status: JSON.stringify(status) }, 'nats protocol error');
          break;
        case 'ldm':
          // Lame duck mode: this server is draining, the client will move.
          this.log.warn({ server: status.server }, 'nats server entering lame duck mode');
          break;
        default:
          this.log.debug({ type: status.type }, 'nats status');
      }
    }
  }

  publish(subject: string, payload: unknown): void {
    this.connection.publish(subject, encode(payload));
  }

  /**
   * Request/reply with a deadline. Rejects with a "no responders" error when
   * nothing is listening on the subject, which is a precise and immediate
   * signal that the target node is gone.
   */
  async request<T>(subject: string, payload: unknown, timeoutMs?: number): Promise<T> {
    const reply = await this.connection.request(subject, encode(payload), {
      timeout: timeoutMs ?? this.opts.requestTimeoutMs ?? 5_000,
    });
    return decode<T>(reply.data);
  }

  /**
   * Subscribe and pump messages into `onMessage`. Errors thrown by the
   * handler are logged, never allowed to kill the subscription loop.
   */
  subscribe<T>(
    subject: string,
    onMessage: (msg: T) => void,
    opts: { queue?: string } = {},
  ): Subscription {
    const sub = this.connection.subscribe(subject, {
      ...(opts.queue === undefined ? {} : { queue: opts.queue }),
    });
    void (async () => {
      for await (const msg of sub) {
        let decoded: T;
        try {
          decoded = decode<T>(msg.data);
        } catch {
          this.log.warn({ subject: msg.subject }, 'dropping undecodable nats message');
          continue;
        }
        try {
          onMessage(decoded);
        } catch (err) {
          this.log.error({ err: (err as Error).message, subject: msg.subject }, 'nats handler threw');
        }
      }
    })();
    return sub;
  }

  /** Wait until everything published so far has reached the server. */
  async flush(): Promise<void> {
    if (this.nc) await this.nc.flush();
  }

  stats(): NatsStats {
    const s = this.nc?.stats();
    return {
      connected: this.connected,
      server: this.nc?.getServer() ?? '',
      reconnects: this.reconnects,
      disconnects: this.disconnects,
      slowConsumers: this.slowConsumers,
      errors: this.errors,
      inMsgs: s?.inMsgs ?? 0,
      outMsgs: s?.outMsgs ?? 0,
      inBytes: s?.inBytes ?? 0,
      outBytes: s?.outBytes ?? 0,
    };
  }

  /**
   * Drain rather than close: pending messages are delivered and buffered
   * publishes are flushed first, so a rolling restart does not drop the
   * in-flight replies a client is waiting for.
   */
  async close(): Promise<void> {
    if (!this.nc || this.closing) return;
    this.closing = true;
    try {
      await this.nc.drain();
    } catch (err) {
      this.log.warn({ err: (err as Error).message }, 'nats drain failed, closing hard');
      await this.nc.close().catch(() => undefined);
    }
    this.nc = null;
    this.log.info('nats connection closed');
  }
}

/** Internal cluster messages travel as JSON, same as on the redis transport. */
function encode(payload: unknown): Uint8Array {
  return Buffer.from(JSON.stringify(payload), 'utf8');
}

function decode<T>(data: Uint8Array): T {
  return JSON.parse(Buffer.from(data).toString('utf8')) as T;
}
