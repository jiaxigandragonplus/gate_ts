import { readFileSync, existsSync } from 'node:fs';
import { hostname } from 'node:os';
import { resolve } from 'node:path';
import * as dotenv from 'dotenv';
import type { RouteRule } from './router/routeTable';
import type { CodecName } from '../framework/protocol/codec';
import { parseCodecList, parseCodecName } from '../framework/protocol/codecs';

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
  return v === '1' || v.toLowerCase() === 'true' || v.toLowerCase() === 'yes';
}

export interface JwtConfig {
  /** HS* shared secret. Mutually exclusive with publicKey. */
  secret?: string;
  /** Asymmetric public key, PEM (RS256, ES256...), from JWT_PUBLIC_KEY_FILE or JWT_PUBLIC_KEY. */
  publicKey?: string;
  algorithms: string[];
  issuer?: string;
  audience?: string;
  /** Claim holding the user id. */
  uidClaim: string;
  /** Allowed clock skew in seconds. */
  clockToleranceSec: number;
}

export interface GateConfig {
  gateId: string;
  /** Address advertised to other nodes (host:port of the public WS endpoint). */
  advertiseAddr: string;
  env: string;

  ws: {
    host: string;
    port: number;
    path: string;
    /** Max inbound frame size in bytes. */
    maxPayloadBytes: number;
    /** Server-side ping interval (ms); a client that misses `pongTimeoutMs` is dropped. */
    pingIntervalMs: number;
    pongTimeoutMs: number;
    /** Time a socket may stay unauthenticated before being closed (ms). */
    authTimeoutMs: number;
    /** Drop the connection when the outbound socket buffer exceeds this (bytes). */
    maxBackpressureBytes: number;
    perMessageDeflate: boolean;
    /** Max concurrent connections per gate; 0 = unlimited. */
    maxConnections: number;
    /** Read the client ip from X-Forwarded-For. Only enable behind your own LB. */
    trustProxy: boolean;
    /** Wire formats this gate serves; clients pick one at handshake time. */
    codecs: CodecName[];
    /** Used when the client expresses no preference. */
    defaultCodec: CodecName;
  };

  session: {
    /** How long a disconnected session may be resumed (ms). */
    resumeWindowMs: number;
    /** Number of downstream packets retained per session for replay. */
    replayBufferSize: number;
    /** TTL of the redis session ownership record (ms). Refreshed by heartbeat. */
    registryTtlMs: number;
    registryRefreshMs: number;
    /**
     * Allow a client to resume on a gate other than the one it started on.
     * Identity is restored but the replay buffer is not, so the client is
     * told to resync. Required for resume to work behind a plain L4 balancer.
     */
    crossGateResume: boolean;
  };

  limits: {
    /** Inbound messages per second, per connection. */
    msgsPerSec: number;
    burst: number;
  };

  redis: {
    url: string;
    keyPrefix: string;
  };

  cluster: {
    /** Heartbeat interval for the gate node registry (ms). */
    heartbeatMs: number;
    /** A node is considered dead after this long without a heartbeat (ms). */
    nodeTtlMs: number;
    /** Forward online/offline/suspended/resumed events to backend services. */
    notifySessionEvents: boolean;
  };

  backend: {
    /** Request timeout for upstream service calls (ms). */
    requestTimeoutMs: number;
    /** How long a session stays pinned to a backend node (ms). */
    stickyTtlMs: number;
  };

  routes: RouteRule[];
  defaultService?: string;

  admin: {
    host: string;
    port: number;
    /** Bearer token required by the mutating admin endpoints. */
    token?: string;
  };

  jwt: JwtConfig;
  shutdownGraceMs: number;
}

const DEFAULT_ROUTES: RouteRule[] = [
  { prefix: 'game.', service: 'game' },
  { prefix: 'chat.', service: 'chat' },
];

function loadRoutes(): { routes: RouteRule[]; defaultService?: string } {
  const file = optStr('ROUTES_FILE');
  if (file) {
    const path = resolve(process.cwd(), file);
    if (!existsSync(path)) throw new Error(`ROUTES_FILE not found: ${path}`);
    const parsed = JSON.parse(readFileSync(path, 'utf8')) as {
      routes?: RouteRule[];
      defaultService?: string;
    };
    if (!Array.isArray(parsed.routes)) throw new Error(`${file}: "routes" must be an array`);
    return { routes: parsed.routes, defaultService: parsed.defaultService };
  }
  const inline = optStr('ROUTES');
  if (inline) {
    const parsed = JSON.parse(inline) as RouteRule[];
    return { routes: parsed, defaultService: optStr('DEFAULT_SERVICE') };
  }
  return { routes: DEFAULT_ROUTES, defaultService: optStr('DEFAULT_SERVICE') };
}

function loadJwt(): JwtConfig {
  const keyFile = optStr('JWT_PUBLIC_KEY_FILE');
  const publicKey = keyFile
    ? readFileSync(resolve(process.cwd(), keyFile), 'utf8')
    : optStr('JWT_PUBLIC_KEY');
  const secret = optStr('JWT_SECRET');
  if (!secret && !publicKey) {
    throw new Error('JWT verification key missing: set JWT_SECRET or JWT_PUBLIC_KEY[_FILE]');
  }
  const algorithms = str('JWT_ALGORITHMS', publicKey ? 'RS256' : 'HS256')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  return {
    secret,
    publicKey,
    algorithms,
    issuer: optStr('JWT_ISSUER'),
    audience: optStr('JWT_AUDIENCE'),
    uidClaim: str('JWT_UID_CLAIM', 'sub'),
    clockToleranceSec: int('JWT_CLOCK_TOLERANCE_SEC', 5),
  };
}

function loadCodecs(): { codecs: CodecName[]; defaultCodec: CodecName } {
  const codecs = parseCodecList(str('WS_CODECS', 'json,protobuf'));
  if (codecs.length === 0) throw new Error('WS_CODECS must enable at least one codec');
  const requested = optStr('WS_DEFAULT_CODEC');
  const defaultCodec = requested ? parseCodecName(requested) : codecs[0];
  if (!defaultCodec) throw new Error(`WS_DEFAULT_CODEC "${requested}" is not a known codec`);
  if (!codecs.includes(defaultCodec)) {
    throw new Error(`WS_DEFAULT_CODEC "${defaultCodec}" is not listed in WS_CODECS`);
  }
  return { codecs, defaultCodec };
}

export function loadConfig(): GateConfig {
  const port = int('WS_PORT', 7000);
  const { routes, defaultService } = loadRoutes();
  const { codecs, defaultCodec } = loadCodecs();
  return {
    gateId: str('GATE_ID', `${hostname()}-${port}`),
    advertiseAddr: str('ADVERTISE_ADDR', `${hostname()}:${port}`),
    env: str('NODE_ENV', 'development'),

    ws: {
      host: str('WS_HOST', '0.0.0.0'),
      port,
      path: str('WS_PATH', '/ws'),
      maxPayloadBytes: int('WS_MAX_PAYLOAD', 64 * 1024),
      pingIntervalMs: int('WS_PING_INTERVAL_MS', 15_000),
      pongTimeoutMs: int('WS_PONG_TIMEOUT_MS', 45_000),
      authTimeoutMs: int('WS_AUTH_TIMEOUT_MS', 10_000),
      maxBackpressureBytes: int('WS_MAX_BACKPRESSURE', 4 * 1024 * 1024),
      perMessageDeflate: bool('WS_PERMESSAGE_DEFLATE', false),
      maxConnections: int('WS_MAX_CONNECTIONS', 0),
      trustProxy: bool('WS_TRUST_PROXY', false),
      codecs,
      defaultCodec,
    },

    session: {
      resumeWindowMs: int('SESSION_RESUME_WINDOW_MS', 60_000),
      replayBufferSize: int('SESSION_REPLAY_BUFFER', 256),
      registryTtlMs: int('SESSION_REGISTRY_TTL_MS', 90_000),
      registryRefreshMs: int('SESSION_REGISTRY_REFRESH_MS', 30_000),
      crossGateResume: bool('SESSION_CROSS_GATE_RESUME', true),
    },

    limits: {
      msgsPerSec: int('LIMIT_MSGS_PER_SEC', 30),
      burst: int('LIMIT_BURST', 60),
    },

    redis: {
      url: str('REDIS_URL', 'redis://127.0.0.1:6379'),
      keyPrefix: str('REDIS_KEY_PREFIX', 'gate'),
    },

    cluster: {
      heartbeatMs: int('CLUSTER_HEARTBEAT_MS', 5_000),
      nodeTtlMs: int('CLUSTER_NODE_TTL_MS', 15_000),
      notifySessionEvents: bool('CLUSTER_NOTIFY_SESSION_EVENTS', true),
    },

    backend: {
      requestTimeoutMs: int('BACKEND_REQUEST_TIMEOUT_MS', 8_000),
      stickyTtlMs: int('BACKEND_STICKY_TTL_MS', 300_000),
    },

    routes,
    defaultService,

    admin: {
      host: str('ADMIN_HOST', '0.0.0.0'),
      port: int('ADMIN_PORT', port + 1000),
      token: optStr('ADMIN_TOKEN'),
    },

    jwt: loadJwt(),
    shutdownGraceMs: int('SHUTDOWN_GRACE_MS', 10_000),
  };
}
