import type { GateConfig } from './config';
import { JwtVerifier, AuthError } from './auth';
import { RedisPool } from '../framework/redis/client';
import { Keys } from '../framework/redis/keys';
import { SessionRegistry } from '../framework/redis/sessionRegistry';
import { NodeRegistry } from '../framework/redis/nodeRegistry';
import { createClusterBus, type ClusterBus } from '../framework/transport';
import { BackendClient, ServiceUnavailableError } from './router/backendClient';
import { RouteTable } from './router/routeTable';
import { SessionManager } from './session/sessionManager';
import type { Session } from './session/session';
import { WsServer } from './net/wsServer';
import type { Connection } from './net/connection';
import { Metrics } from './metrics/metrics';
import { AdminServer } from './metrics/adminServer';
import {
  CloseCode,
  ErrorCode,
  KICK_REASON,
  PacketType,
  type ClientPacket,
  type AuthPacket,
  type ResumePacket,
  type RequestPacket,
  type NotifyPacket,
  type HeartbeatPacket,
} from '../framework/protocol/packet';
import type { DownstreamMessage, UpstreamMessage } from '../framework/protocol/internal';
import { fromInternal, toInternal } from '../framework/protocol/payload';
import { logger } from '../framework/util/logger';

/**
 * The gate process.
 *
 * Responsibilities, in order of importance:
 *   1. terminate client WebSocket connections and verify their JWT
 *   2. keep sessions alive across network drops, and enforce one session per
 *      account across the whole cluster
 *   3. forward traffic to the right backend service and route replies back
 *
 * It is intentionally stateless with respect to game logic: payloads are
 * never inspected, so adding a new message only means adding a route.
 */
export class Gate {
  private readonly log = logger.child({ mod: 'gate' });
  private readonly metrics = new Metrics();
  private readonly keys: Keys;
  private readonly pool: RedisPool;
  private readonly sessionRegistry: SessionRegistry;
  private readonly nodes: NodeRegistry;
  private readonly bus: ClusterBus;
  private readonly backend: BackendClient;
  private readonly routes: RouteTable;
  private readonly sessions: SessionManager;
  private readonly jwt: JwtVerifier;
  private readonly ws: WsServer;
  private readonly admin: AdminServer;

  private heartbeatTimer: NodeJS.Timeout | null = null;
  private redisReady = false;
  private shuttingDown = false;

  constructor(private readonly cfg: GateConfig) {
    this.keys = new Keys(cfg.redis.keyPrefix);
    this.pool = new RedisPool(cfg.redis.url);
    this.sessionRegistry = new SessionRegistry(this.pool.cmd, this.keys);
    this.nodes = new NodeRegistry(this.pool.cmd, this.keys, cfg.cluster.nodeTtlMs);
    this.bus = createClusterBus(
      {
        kind: cfg.cluster.transport,
        redis: { pool: this.pool, keys: this.keys },
        nats: {
          subjectPrefix: cfg.nats.subjectPrefix,
          options: { ...cfg.nats, name: cfg.gateId },
        },
      },
      cfg.gateId,
    );
    this.routes = new RouteTable(cfg.routes, cfg.defaultService);
    this.jwt = new JwtVerifier(cfg.jwt);

    this.backend = new BackendClient(this.bus, this.nodes, this.pool.cmd, this.keys, {
      requestTimeoutMs: cfg.backend.requestTimeoutMs,
      stickyTtlMs: cfg.backend.stickyTtlMs,
    });

    this.sessions = new SessionManager(this.sessionRegistry, this.bus, {
      gateId: cfg.gateId,
      addr: cfg.advertiseAddr,
      resumeWindowMs: cfg.session.resumeWindowMs,
      replayBufferSize: cfg.session.replayBufferSize,
      registryTtlMs: cfg.session.registryTtlMs,
      registryRefreshMs: cfg.session.registryRefreshMs,
      heartbeatIntervalMs: cfg.ws.pingIntervalMs,
      crossGateResume: cfg.session.crossGateResume,
    });

    this.ws = new WsServer(
      {
        host: cfg.ws.host,
        port: cfg.ws.port,
        path: cfg.ws.path,
        maxPayloadBytes: cfg.ws.maxPayloadBytes,
        pingIntervalMs: cfg.ws.pingIntervalMs,
        pongTimeoutMs: cfg.ws.pongTimeoutMs,
        authTimeoutMs: cfg.ws.authTimeoutMs,
        maxBackpressureBytes: cfg.ws.maxBackpressureBytes,
        perMessageDeflate: cfg.ws.perMessageDeflate,
        maxConnections: cfg.ws.maxConnections,
        msgsPerSec: cfg.limits.msgsPerSec,
        burst: cfg.limits.burst,
        trustProxy: cfg.ws.trustProxy,
        codecs: cfg.ws.codecs,
        defaultCodec: cfg.ws.defaultCodec,
      },
      {
        onPacket: (conn, packet) => this.onPacket(conn, packet),
        onClose: (conn, code, reason) => this.onClose(conn, code, reason),
        onUpgrade: () => !this.shuttingDown,
      },
    );

    this.admin = new AdminServer(
      {
        host: cfg.admin.host,
        port: cfg.admin.port,
        gateId: cfg.gateId,
        ...(cfg.admin.token === undefined ? {} : { token: cfg.admin.token }),
      },
      this.metrics,
      {
        stats: () => this.stats(),
        ready: () => this.redisReady && !this.shuttingDown,
        kick: (target, reason) => this.kickCluster(target, reason),
        push: (uid, cmd, payload) => this.pushToUid(uid, cmd, payload),
        drain: () => void this.shutdown('admin drain'),
      },
    );

    this.metrics.registerGauge('sessions_total', () => this.sessions.size);
    this.metrics.registerGauge('sessions_online', () => this.sessions.onlineCount);
    this.metrics.registerGauge('sessions_suspended', () => this.sessions.suspendedCount);
    this.metrics.registerGauge('connections_current', () => this.ws.connectionCount);
    this.metrics.registerGauge('upstream_pending', () => this.backend.pendingCount);

    // Transport counters are owned by the net layer; expose them from there
    // rather than double-counting on every packet.
    const t = this.ws.stats;
    this.metrics.registerGauge('connections_accepted_total', () => t.accepted);
    this.metrics.registerGauge('connections_closed_total', () => t.closed);
    this.metrics.registerGauge('upgrades_rejected_total', () => t.rejected);
    this.metrics.registerGauge('packets_in_total', () => t.packetsRecv);
    this.metrics.registerGauge('packets_out_total', () => t.packetsSent);
    this.metrics.registerGauge('bytes_in_total', () => t.bytesRecv);
    this.metrics.registerGauge('bytes_out_total', () => t.bytesSent);
    this.metrics.registerGauge('rate_limited_total', () => t.rateLimited);
    this.metrics.registerGauge('protocol_errors_total', () => t.protocolErrors);
    this.metrics.registerGauge('backpressure_drops_total', () => t.backpressureDrops);

    // Transport counters, when the transport has any (NATS reconnects and
    // slow consumers are the two worth alerting on).
    if (this.bus.counters) {
      for (const name of Object.keys(this.bus.counters())) {
        this.metrics.registerGauge(`transport_${name}`, () => this.bus.counters?.()[name] ?? 0);
      }
    }

    this.backend.setTimeoutHandler(({ sid, id, cmd, service }) => {
      this.metrics.upstreamTimeouts += 1;
      const session = this.sessions.getBySid(sid);
      session?.respond(id, undefined, ErrorCode.ServiceTimeout, `${service} did not respond`);
      this.log.warn({ sid, cmd, service }, 'responded with upstream timeout');
    });

    this.wireSessionEvents();
  }

  // ---------------------------------------------------------- lifecycle ----

  async start(): Promise<void> {
    await this.pool.connect();
    this.redisReady = true;
    this.log.info({ url: redact(this.cfg.redis.url) }, 'connected to redis');

    await this.bus.start((msg: DownstreamMessage) => this.onClusterMessage(msg));
    this.sessions.start();
    await this.heartbeat();
    this.heartbeatTimer = setInterval(() => {
      void this.heartbeat();
    }, this.cfg.cluster.heartbeatMs);
    this.heartbeatTimer.unref();

    await this.ws.listen();
    await this.admin.listen();

    this.log.info(
      {
        gate: this.cfg.gateId,
        addr: this.cfg.advertiseAddr,
        transport: this.bus.kind,
        codecs: this.cfg.ws.codecs,
        defaultCodec: this.cfg.ws.defaultCodec,
        routes: this.routes.describe(),
      },
      'gate started',
    );
  }

  private async heartbeat(): Promise<void> {
    try {
      await this.nodes.heartbeatGate({
        id: this.cfg.gateId,
        addr: this.cfg.advertiseAddr,
        load: this.sessions.onlineCount,
        ts: Date.now(),
        meta: { path: this.cfg.ws.path },
      });
    } catch (err) {
      this.log.warn({ err: (err as Error).message }, 'gate heartbeat failed');
    }
  }

  async shutdown(reason: string): Promise<void> {
    if (this.shuttingDown) return;
    this.shuttingDown = true;
    this.log.info({ reason, sessions: this.sessions.size }, 'shutting down');

    // Stop taking new sockets first so the LB can drain us.
    this.ws.stopAccepting();
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    await this.nodes.unregisterGate(this.cfg.gateId).catch(() => undefined);

    // Give in-flight upstream replies a moment to land before cutting sessions.
    await new Promise((r) => setTimeout(r, Math.min(500, this.cfg.shutdownGraceMs)));

    await this.sessions.drain(KICK_REASON.Shutdown, { resumable: true });
    await this.bus.stop();
    await this.ws.close();
    await this.admin.close();
    await this.pool.close();
    this.log.info('shutdown complete');
  }

  // ------------------------------------------------------ client packets ---

  private onPacket(conn: Connection, packet: ClientPacket): void {
    switch (packet.t) {
      case PacketType.Auth:
        void this.handleAuth(conn, packet);
        return;
      case PacketType.Resume:
        void this.handleResume(conn, packet);
        return;
      case PacketType.Heartbeat:
        this.handleHeartbeat(conn, packet);
        return;
      case PacketType.Request:
        void this.handleRequest(conn, packet);
        return;
      case PacketType.Notify:
        void this.handleNotify(conn, packet);
        return;
      default:
        conn.sendError(ErrorCode.BadRequest, 'unsupported packet type');
    }
  }

  private async handleAuth(conn: Connection, packet: AuthPacket): Promise<void> {
    if (conn.session) {
      conn.sendError(ErrorCode.AlreadyAuthenticated, 'session already established');
      return;
    }

    let uid: string;
    try {
      const user = this.jwt.verify(packet.token);
      uid = user.uid;
    } catch (err) {
      const reason = err instanceof AuthError ? err.reason : 'malformed';
      this.metrics.authFailed += 1;
      this.log.info({ ip: conn.ip, reason }, 'auth rejected');
      conn.sendError(ErrorCode.AuthFailed, `authentication failed: ${reason}`);
      conn.close(CloseCode.AuthFailed, 'auth failed');
      return;
    }

    try {
      const { session, displaced } = await this.sessions.login(conn, uid, packet.device);
      this.metrics.authOk += 1;
      if (displaced) this.metrics.kicksDuplicateLogin += 1;
      // The socket can die while we are talking to redis. Suspending here
      // keeps the session resumable instead of leaking it until its TTL.
      if (conn.closed) this.sessions.suspend(session);
    } catch (err) {
      this.log.error({ err: (err as Error).message, uid }, 'login failed');
      conn.sendError(ErrorCode.Internal, 'login failed');
      conn.close(CloseCode.ProtocolError, 'login failed');
    }
  }

  private async handleResume(conn: Connection, packet: ResumePacket): Promise<void> {
    if (conn.session) {
      conn.sendError(ErrorCode.AlreadyAuthenticated, 'session already established');
      return;
    }

    const result = await this.sessions.resume(conn, packet.sid, packet.rt, packet.ack);
    if (!result.ok) {
      this.metrics.resumeFailed += 1;
      this.log.info({ sid: packet.sid, reason: result.reason, ip: conn.ip }, 'resume failed');
      conn.sendError(ErrorCode.ResumeFailed, `resume failed: ${result.reason}`);
      conn.close(CloseCode.ResumeFailed, result.reason);
      return;
    }
    this.metrics.resumeOk += 1;
    if (result.resync) this.metrics.resumeMigrated += 1;
    if (conn.closed) this.sessions.suspend(result.session);
  }

  private handleHeartbeat(conn: Connection, packet: HeartbeatPacket): void {
    if (packet.ack !== undefined) conn.session?.ackUpTo(packet.ack);
    conn.send({ t: PacketType.HeartbeatAck, ts: Date.now() });
  }

  private async handleRequest(conn: Connection, packet: RequestPacket): Promise<void> {
    const session = conn.session;
    if (!session) {
      conn.sendError(ErrorCode.Unauthenticated, 'authenticate first', packet.id);
      return;
    }

    // A resent packet has already been answered; its reply is in the replay
    // buffer, so forwarding it again would double-apply the action.
    if (!session.acceptUpstream(packet.cseq)) {
      this.log.debug({ sid: session.sid, cseq: packet.cseq }, 'dropping duplicate upstream packet');
      return;
    }

    if (this.routes.isInternal(packet.cmd)) {
      this.handleInternal(session, packet);
      return;
    }

    const route = this.routes.resolve(packet.cmd);
    if (!route) {
      this.metrics.routeMisses += 1;
      session.respond(packet.id, undefined, ErrorCode.RouteNotFound, `no route for ${packet.cmd}`);
      return;
    }

    const msg: UpstreamMessage = {
      k: 'req',
      gate: this.cfg.gateId,
      sid: session.sid,
      uid: session.uid,
      id: packet.id,
      cmd: packet.cmd,
      ...toInternal(packet.d),
      ts: Date.now(),
      meta: session.meta(),
    };

    this.backend.track(session.sid, packet.id, packet.cmd, route.service, route.timeoutMs);
    try {
      await this.backend.send(route.service, session.uid, msg, route.sticky);
      session.touchedServices.add(route.service);
      this.metrics.upstreamSent += 1;
    } catch (err) {
      this.backend.settle(session.sid, packet.id);
      if (err instanceof ServiceUnavailableError) {
        this.metrics.upstreamUnavailable += 1;
        session.respond(
          packet.id,
          undefined,
          ErrorCode.ServiceUnavailable,
          `${route.service} unavailable`,
        );
      } else {
        this.log.error({ err: (err as Error).message, cmd: packet.cmd }, 'upstream send failed');
        session.respond(packet.id, undefined, ErrorCode.Internal, 'forwarding failed');
      }
    }
  }

  private async handleNotify(conn: Connection, packet: NotifyPacket): Promise<void> {
    const session = conn.session;
    if (!session) {
      conn.sendError(ErrorCode.Unauthenticated, 'authenticate first');
      return;
    }
    if (!session.acceptUpstream(packet.cseq)) return;

    const route = this.routes.resolve(packet.cmd);
    if (!route) {
      this.metrics.routeMisses += 1;
      return;
    }

    const msg: UpstreamMessage = {
      k: 'notify',
      gate: this.cfg.gateId,
      sid: session.sid,
      uid: session.uid,
      cmd: packet.cmd,
      ...toInternal(packet.d),
      ts: Date.now(),
      meta: session.meta(),
    };

    try {
      await this.backend.send(route.service, session.uid, msg, route.sticky);
      session.touchedServices.add(route.service);
      this.metrics.upstreamSent += 1;
    } catch (err) {
      if (err instanceof ServiceUnavailableError) this.metrics.upstreamUnavailable += 1;
      this.log.debug({ cmd: packet.cmd, err: (err as Error).message }, 'notify dropped');
    }
  }

  /** Commands the gate answers itself, without touching a backend. */
  private handleInternal(session: Session, packet: RequestPacket): void {
    switch (packet.cmd) {
      case 'gate.ping':
      case 'gate.time':
        session.respond(packet.id, { ts: Date.now() });
        return;
      case 'gate.whoami':
        session.respond(packet.id, {
          uid: session.uid,
          sid: session.sid,
          gate: this.cfg.gateId,
          since: session.createdAt,
          seq: session.lastSeq,
        });
        return;
      default:
        session.respond(
          packet.id,
          undefined,
          ErrorCode.RouteNotFound,
          `unknown gate command ${packet.cmd}`,
        );
    }
  }

  private onClose(conn: Connection, code: number, reason: string): void {
    const session = conn.session;
    if (!session) return;
    conn.session = null;

    // The socket may already have been replaced by a resume; only suspend if
    // this connection is still the session's current one.
    if (session.conn !== null && session.conn !== conn) return;
    if (session.state === 'closed') return;

    this.backend.cancelSession(session.sid);
    this.log.debug({ uid: session.uid, sid: session.sid, code, reason }, 'socket closed');
    this.sessions.suspend(session);
  }

  // ----------------------------------------------------- cluster inbound ---

  private onClusterMessage(msg: DownstreamMessage): void {
    switch (msg.k) {
      case 'resp': {
        const session = this.sessions.getBySid(msg.sid);
        const latency = this.backend.settle(msg.sid, msg.id);
        if (latency !== null) this.metrics.observeUpstreamLatency(latency);
        if (!session) {
          this.metrics.downstreamDropped += 1;
          return;
        }
        this.metrics.downstreamResponses += 1;
        session.respond(msg.id, fromInternal(msg), msg.e, msg.m);
        return;
      }

      case 'push': {
        const session = msg.sid
          ? this.sessions.getBySid(msg.sid)
          : msg.uid
            ? this.sessions.getByUid(msg.uid)
            : undefined;
        if (!session) {
          this.metrics.downstreamDropped += 1;
          return;
        }
        this.metrics.downstreamPushes += 1;
        session.push(msg.cmd, fromInternal(msg));
        return;
      }

      case 'multicast': {
        const payload = fromInternal(msg);
        for (const uid of msg.uids) {
          const session = this.sessions.getByUid(uid);
          if (session) {
            this.metrics.downstreamPushes += 1;
            session.push(msg.cmd, payload);
          }
        }
        return;
      }

      case 'broadcast': {
        const payload = fromInternal(msg);
        for (const session of this.sessions.sessions()) {
          this.metrics.downstreamPushes += 1;
          session.push(msg.cmd, payload);
        }
        return;
      }

      case 'kick': {
        const handled = this.sessions.handleRemoteKick(
          { ...(msg.sid ? { sid: msg.sid } : {}), ...(msg.uid ? { uid: msg.uid } : {}) },
          msg.reason,
          msg.m,
        );
        if (handled && msg.reason === KICK_REASON.Admin) this.metrics.kicksAdmin += 1;
        if (handled && msg.reason === KICK_REASON.DuplicateLogin) {
          this.metrics.kicksDuplicateLogin += 1;
        }
        return;
      }
    }
  }

  // ------------------------------------------------------------- ops -------

  /** Kick a session wherever it lives in the cluster. */
  private async kickCluster(target: { uid?: string; sid?: string }, reason: string): Promise<boolean> {
    if (target.sid) {
      const local = this.sessions.getBySid(target.sid);
      if (local) {
        this.sessions.terminate(local, reason, CloseCode.KickedAdmin, reason);
        this.metrics.kicksAdmin += 1;
        return true;
      }
      const owner = await this.sessionRegistry.getBySid(target.sid);
      if (!owner) return false;
      await this.bus.publishToGate(owner.gate, { k: 'kick', sid: owner.sid, reason });
      return true;
    }

    const uid = target.uid as string;
    const local = this.sessions.getByUid(uid);
    if (local) {
      this.sessions.terminate(local, reason, CloseCode.KickedAdmin, reason);
      this.metrics.kicksAdmin += 1;
      return true;
    }
    const owner = await this.sessionRegistry.get(uid);
    if (!owner) return false;
    await this.bus.publishToGate(owner.gate, { k: 'kick', uid, sid: owner.sid, reason });
    return true;
  }

  private async pushToUid(uid: string, cmd: string, payload: unknown): Promise<boolean> {
    const local = this.sessions.getByUid(uid);
    if (local) {
      local.push(cmd, payload);
      return true;
    }
    const owner = await this.sessionRegistry.get(uid);
    if (!owner) return false;
    await this.bus.publishToGate(owner.gate, { k: 'push', uid, cmd, d: payload });
    return true;
  }

  /**
   * Tell backend services about session lifecycle changes so they can load or
   * park player state without waiting for the first request.
   */
  private wireSessionEvents(): void {
    if (!this.cfg.cluster.notifySessionEvents) return;

    const notify = (session: Session, ev: 'online' | 'offline' | 'suspended' | 'resumed', reason?: string) => {
      const msg: UpstreamMessage = {
        k: 'session',
        ev,
        gate: this.cfg.gateId,
        sid: session.sid,
        uid: session.uid,
        ts: Date.now(),
        ...(reason === undefined ? {} : { reason }),
        meta: session.meta(),
      };
      for (const service of this.routes.services()) {
        this.backend.send(service, session.uid, msg, true).catch(() => {
          // A service that is down does not need to hear about presence.
        });
      }
    };

    this.sessions.on('online', (s: Session) => notify(s, 'online'));
    this.sessions.on('resumed', (s: Session) => notify(s, 'resumed'));
    this.sessions.on('suspended', (s: Session) => notify(s, 'suspended'));
    this.sessions.on('offline', (s: Session, reason: string) => {
      notify(s, 'offline', reason);
      void this.backend.clearBindings(s.uid, [...s.touchedServices]);
    });
  }

  private stats(): unknown {
    return {
      gate: this.cfg.gateId,
      addr: this.cfg.advertiseAddr,
      transport: this.bus.kind,
      uptimeSec: Math.floor((Date.now() - this.metrics.startedAt) / 1000),
      draining: this.shuttingDown,
      connections: this.ws.connectionCount,
      sessions: {
        total: this.sessions.size,
        online: this.sessions.onlineCount,
        suspended: this.sessions.suspendedCount,
      },
      upstreamPending: this.backend.pendingCount,
      transportCounters: this.bus.counters?.() ?? null,
      routes: this.routes.describe(),
      counters: this.metrics.snapshot(),
    };
  }
}

function redact(url: string): string {
  return url.replace(/\/\/([^@/]*)@/, '//***@');
}
