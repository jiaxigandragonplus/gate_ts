import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import type { Metrics } from './metrics';
import { logger } from '../../framework/util/logger';

export interface AdminHandlers {
  /** Aggregate view of this node, returned by GET /stats. */
  stats: () => unknown;
  /** True once redis and the cluster registry are usable. */
  ready: () => boolean;
  /** Cluster-wide kick, returns whether a session was found. */
  kick: (target: { uid?: string; sid?: string }, reason: string) => Promise<boolean>;
  /** Push a message to a uid, for smoke tests and ops tooling. */
  push?: (uid: string, cmd: string, payload: unknown) => Promise<boolean>;
  /** Begin graceful shutdown. */
  drain?: () => void;
}

export interface AdminServerOptions {
  host: string;
  port: number;
  gateId: string;
  /** When set, mutating endpoints require `Authorization: Bearer <token>`. */
  token?: string;
}

/**
 * Ops surface: health probes for the load balancer, Prometheus metrics, and a
 * couple of operator actions (kick / drain). Bind this to an internal
 * interface - it is not meant to be reachable from the internet.
 */
export class AdminServer {
  private readonly log = logger.child({ mod: 'admin' });
  private readonly server: Server;

  constructor(
    private readonly opts: AdminServerOptions,
    private readonly metrics: Metrics,
    private readonly handlers: AdminHandlers,
  ) {
    this.server = createServer((req, res) => {
      void this.route(req, res).catch((err: Error) => {
        this.log.error({ err: err.message }, 'admin request failed');
        json(res, 500, { error: 'internal error' });
      });
    });
  }

  private async route(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const path = url.pathname;

    if (req.method === 'GET' && (path === '/healthz' || path === '/')) {
      return json(res, 200, { ok: true, gate: this.opts.gateId });
    }

    if (req.method === 'GET' && path === '/readyz') {
      const ready = this.handlers.ready();
      return json(res, ready ? 200 : 503, { ready });
    }

    if (req.method === 'GET' && path === '/metrics') {
      res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' });
      res.end(this.metrics.render({ gate: this.opts.gateId }));
      return;
    }

    if (req.method === 'GET' && path === '/stats') {
      return json(res, 200, this.handlers.stats());
    }

    // ---- mutating endpoints ----
    if (!this.authorized(req)) {
      return json(res, 401, { error: 'unauthorized' });
    }

    if (req.method === 'POST' && path === '/admin/kick') {
      const body = await readJson(req);
      const uid = str(body['uid']) ?? url.searchParams.get('uid') ?? undefined;
      const sid = str(body['sid']) ?? url.searchParams.get('sid') ?? undefined;
      if (!uid && !sid) return json(res, 400, { error: 'uid or sid required' });
      const reason = str(body['reason']) ?? 'admin';
      const found = await this.handlers.kick({ uid, sid }, reason);
      return json(res, 200, { kicked: found });
    }

    if (req.method === 'POST' && path === '/admin/push' && this.handlers.push) {
      const body = await readJson(req);
      const uid = str(body['uid']);
      const cmd = str(body['cmd']);
      if (!uid || !cmd) return json(res, 400, { error: 'uid and cmd required' });
      const delivered = await this.handlers.push(uid, cmd, body['d']);
      return json(res, 200, { delivered });
    }

    if (req.method === 'POST' && path === '/admin/drain' && this.handlers.drain) {
      this.handlers.drain();
      return json(res, 202, { draining: true });
    }

    return json(res, 404, { error: 'not found' });
  }

  private authorized(req: IncomingMessage): boolean {
    if (!this.opts.token) return true;
    const header = req.headers.authorization ?? '';
    const provided = header.startsWith('Bearer ') ? header.slice(7) : '';
    const a = Buffer.from(provided);
    const b = Buffer.from(this.opts.token);
    return a.length === b.length && timingSafeEqual(a, b);
  }

  async listen(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.server.once('error', reject);
      this.server.listen(this.opts.port, this.opts.host, () => {
        this.server.removeListener('error', reject);
        resolve();
      });
    });
    this.log.info({ host: this.opts.host, port: this.opts.port }, 'admin server listening');
  }

  async close(): Promise<void> {
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

function json(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) });
  res.end(payload);
}

function str(v: unknown): string | undefined {
  return typeof v === 'string' && v.length > 0 ? v : undefined;
}

async function readJson(req: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += (chunk as Buffer).length;
    if (size > 64 * 1024) throw new Error('request body too large');
    chunks.push(chunk as Buffer);
  }
  if (chunks.length === 0) return {};
  try {
    const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return typeof parsed === 'object' && parsed !== null ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}
