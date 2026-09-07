import { EventEmitter } from 'node:events';
import { Session } from './session';
import type { Connection } from '../net/connection';
import type { ResumeTicket, SessionOwner, SessionRegistry } from '../redis/sessionRegistry';
import { SessionRegistry as Registry } from '../redis/sessionRegistry';
import type { ClusterBus } from '../redis/bus';
import { CloseCode, KICK_REASON, PacketType, type KickReason } from '../protocol/packet';
import { shortId } from '../util/id';
import { logger } from '../util/logger';

export interface SessionManagerOptions {
  gateId: string;
  addr: string;
  resumeWindowMs: number;
  replayBufferSize: number;
  registryTtlMs: number;
  registryRefreshMs: number;
  heartbeatIntervalMs: number;
  /** Allow resuming a session that was established on a different gate. */
  crossGateResume: boolean;
}

export interface LoginResult {
  session: Session;
  /** The session displaced by this login, if any (already kicked). */
  displaced: SessionOwner | null;
}

export type ResumeResult =
  | { ok: true; session: Session; replayed: number; resync: boolean; redirect?: string }
  | { ok: false; reason: 'unknown_session' | 'bad_token' | 'expired' | 'wrong_gate' | 'gone' };

export interface SessionEvents {
  online: [Session];
  offline: [Session, string];
  suspended: [Session];
  resumed: [Session];
}

/**
 * Owns the lifecycle of every session on this gate and coordinates with the
 * rest of the cluster through redis:
 *
 *  - login claims the account atomically, so a second login anywhere in the
 *    cluster displaces the first one (顶号踢人)
 *  - a dropped socket suspends the session for `resumeWindowMs` (断线重连)
 *  - a periodic touch keeps ownership fresh and detects being displaced
 */
export class SessionManager extends EventEmitter {
  private readonly log = logger.child({ mod: 'sessions' });
  private readonly byUid = new Map<string, Session>();
  private readonly bySid = new Map<string, Session>();
  private readonly resumeTimers = new Map<string, NodeJS.Timeout>();
  private refreshTimer: NodeJS.Timeout | null = null;
  private draining = false;

  constructor(
    private readonly registry: SessionRegistry,
    private readonly bus: ClusterBus,
    private readonly opts: SessionManagerOptions,
  ) {
    super();
  }

  get size(): number {
    return this.bySid.size;
  }

  get onlineCount(): number {
    let n = 0;
    for (const s of this.bySid.values()) if (s.online) n += 1;
    return n;
  }

  get suspendedCount(): number {
    let n = 0;
    for (const s of this.bySid.values()) if (s.state === 'suspended') n += 1;
    return n;
  }

  getByUid(uid: string): Session | undefined {
    return this.byUid.get(uid);
  }

  getBySid(sid: string): Session | undefined {
    return this.bySid.get(sid);
  }

  sessions(): Iterable<Session> {
    return this.bySid.values();
  }

  start(): void {
    if (this.refreshTimer) return;
    this.refreshTimer = setInterval(() => {
      void this.refreshOwnership();
    }, this.opts.registryRefreshMs);
    this.refreshTimer.unref();
  }

  // --------------------------------------------------------------- login ---

  /**
   * Establish a new session for an authenticated user, displacing any
   * existing session for the same account (wherever in the cluster it lives).
   */
  async login(conn: Connection, uid: string, device?: string): Promise<LoginResult> {
    if (this.draining) throw new Error('gate is draining');

    const sid = shortId(12);
    const session = new Session({
      uid,
      sid,
      gate: this.opts.gateId,
      addr: this.opts.addr,
      ...(device === undefined ? {} : { device }),
      replayCapacity: this.opts.replayBufferSize,
    });

    const owner: SessionOwner = {
      uid,
      sid,
      gate: this.opts.gateId,
      addr: this.opts.addr,
      since: Date.now(),
      ...(device === undefined ? {} : { device }),
    };

    // Atomic takeover: whoever writes last owns the account.
    const { previous } = await this.registry.claim(owner, this.opts.registryTtlMs);

    session.attach(conn);
    this.byUid.set(uid, session);
    this.bySid.set(sid, session);

    if (previous) {
      await this.evictPrevious(previous, sid);
    }

    const { secret } = await this.registry.issueResumeTicket(owner, this.opts.resumeWindowMs);

    session.sendControl({
      t: PacketType.AuthAck,
      uid,
      sid,
      rt: secret,
      ts: Date.now(),
      rw: this.opts.resumeWindowMs,
      hb: this.opts.heartbeatIntervalMs,
    });

    this.log.info({ uid, sid, ip: conn.ip, displaced: previous?.sid ?? null }, 'session online');
    this.emit('online', session);
    return { session, displaced: previous };
  }

  /** Kick whoever held this account before, locally or on another gate. */
  private async evictPrevious(previous: SessionOwner, newSid: string): Promise<void> {
    if (previous.gate === this.opts.gateId) {
      const stale = this.bySid.get(previous.sid);
      if (stale) {
        this.terminate(stale, KICK_REASON.DuplicateLogin, CloseCode.KickedDuplicateLogin);
      }
      return;
    }
    await this.bus.publishToGate(previous.gate, {
      k: 'kick',
      sid: previous.sid,
      uid: previous.uid,
      reason: KICK_REASON.DuplicateLogin,
      bySid: newSid,
      byGate: this.opts.gateId,
    });
    this.log.info(
      { uid: previous.uid, sid: previous.sid, gate: previous.gate },
      'requested remote kick for duplicate login',
    );
  }

  // -------------------------------------------------------------- resume ---

  /**
   * Re-bind a socket to an existing session.
   *
   * Same gate: the replay buffer is intact, so missed packets are re-sent and
   * the client continues where it left off. Different gate: identity is
   * restored but the buffer was left behind, so the client is told to resync.
   */
  async resume(conn: Connection, sid: string, secret: string, ack: number): Promise<ResumeResult> {
    if (this.draining) return { ok: false, reason: 'gone' };

    const ticket = await this.registry.getResumeTicket(sid);
    if (!ticket) return { ok: false, reason: 'expired' };
    if (!Registry.verifySecret(ticket, secret)) {
      this.log.warn({ sid, ip: conn.ip }, 'resume rejected: bad token');
      return { ok: false, reason: 'bad_token' };
    }

    const local = this.bySid.get(sid);
    if (local && local.state === 'closed') return { ok: false, reason: 'gone' };
    if (!local && !this.opts.crossGateResume) return { ok: false, reason: 'wrong_gate' };

    // Everything that touches redis happens before the socket is attached:
    // once a session has a live connection it can emit pushes, and those must
    // not overtake the ResumeAck.
    const session = local ?? (await this.adopt(ticket, sid));
    const newSecret = await this.mintResumeSecret(session);

    this.clearResumeTimer(sid);
    const previousConn = session.attach(conn);
    if (previousConn && previousConn !== conn) {
      // A racing socket for the same session (NAT rebind, fast reconnect):
      // the newest one wins.
      previousConn.session = null;
      previousConn.close(CloseCode.Superseded, 'superseded by resume');
    }

    session.ackUpTo(ack);
    // A migrated session has no local buffer, so the client must resync.
    const missing = local ? session.missingSince(ack) : null;
    const resync = missing === null;

    conn.send({
      t: PacketType.ResumeAck,
      uid: session.uid,
      sid: session.sid,
      ts: Date.now(),
      replay: missing ?? 0,
      cack: session.lastAcceptedCSeq,
      seq: session.lastSeq,
      rt: newSecret,
      rw: this.opts.resumeWindowMs,
      hb: this.opts.heartbeatIntervalMs,
      ...(resync ? { resync: true } : {}),
      ...(local ? {} : { redirect: ticket.addr }),
    });

    const replayed = resync ? 0 : (session.replayFrom(ack) ?? 0);

    this.log.info(
      { uid: session.uid, sid, replayed, resync, migrated: !local },
      local ? 'session resumed' : 'session migrated to this gate',
    );
    this.emit('resumed', session);
    return {
      ok: true,
      session,
      replayed,
      resync,
      ...(local ? {} : { redirect: ticket.addr }),
    };
  }

  /**
   * Take over a session that was established on another gate. Identity and
   * ownership move here; the replay buffer stays behind, which is why the
   * client is told to resync.
   */
  private async adopt(ticket: ResumeTicket, sid: string): Promise<Session> {
    const owner: SessionOwner = {
      uid: ticket.uid,
      sid,
      gate: this.opts.gateId,
      addr: this.opts.addr,
      since: Date.now(),
    };
    const { previous } = await this.registry.claim(owner, this.opts.registryTtlMs);

    const session = new Session({
      uid: ticket.uid,
      sid,
      gate: this.opts.gateId,
      addr: this.opts.addr,
      replayCapacity: this.opts.replayBufferSize,
    });
    this.byUid.set(ticket.uid, session);
    this.bySid.set(sid, session);

    // Tell the previous gate to drop its copy. This is not a kick of the
    // player - they are already talking to us - so it is delivered silently.
    if (ticket.gate !== this.opts.gateId) {
      await this.bus.publishToGate(ticket.gate, {
        k: 'kick',
        sid,
        uid: ticket.uid,
        reason: KICK_REASON.SessionExpired,
        m: 'migrated',
        byGate: this.opts.gateId,
      });
    }
    if (previous && previous.sid !== sid) {
      await this.evictPrevious(previous, sid);
    }
    return session;
  }

  /** Single-use resume tokens: every successful resume mints a fresh one. */
  private async mintResumeSecret(session: Session): Promise<string> {
    const { secret } = await this.registry.issueResumeTicket(
      {
        uid: session.uid,
        sid: session.sid,
        gate: this.opts.gateId,
        addr: this.opts.addr,
        since: session.createdAt,
      },
      this.opts.resumeWindowMs,
    );
    return secret;
  }

  // ------------------------------------------------------- disconnect ------

  /**
   * The socket for `session` is gone. Keep the session resumable until the
   * window closes, then tear it down for good.
   */
  suspend(session: Session): void {
    if (session.state === 'closed') return;
    session.detach();
    this.emit('suspended', session);

    void this.registry
      .refreshResumeTicket(session.sid, this.opts.resumeWindowMs)
      .catch((err: Error) => this.log.warn({ err: err.message }, 'resume ticket refresh failed'));

    this.clearResumeTimer(session.sid);
    const timer = setTimeout(() => {
      this.resumeTimers.delete(session.sid);
      if (session.state === 'suspended') {
        this.log.info({ uid: session.uid, sid: session.sid }, 'resume window expired');
        void this.destroy(session, KICK_REASON.SessionExpired);
      }
    }, this.opts.resumeWindowMs);
    timer.unref();
    this.resumeTimers.set(session.sid, timer);

    this.log.info(
      { uid: session.uid, sid: session.sid, window: this.opts.resumeWindowMs },
      'session suspended, awaiting resume',
    );
  }

  /** Close the socket with a Kick packet and destroy the session. */
  terminate(session: Session, reason: KickReason | string, code: CloseCode, message?: string): void {
    if (session.state !== 'closed') {
      session.sendControl({
        t: PacketType.Kick,
        reason,
        ...(message === undefined ? {} : { m: message }),
      });
      session.closeSocket(code, reason);
    }
    void this.destroy(session, reason);
  }

  /**
   * Remove a session for good: local maps, redis ownership, resume ticket.
   * Ownership release is conditional, so it never clobbers a takeover.
   */
  async destroy(
    session: Session,
    reason: string,
    opts: { keepResumeTicket?: boolean } = {},
  ): Promise<void> {
    if (session.state === 'closed') return;
    const { uid, sid } = session;
    session.markClosed();

    this.clearResumeTimer(sid);
    this.bySid.delete(sid);
    if (this.byUid.get(uid) === session) this.byUid.delete(uid);

    try {
      await this.registry.release(uid, sid, this.opts.gateId);
      if (!opts.keepResumeTicket) await this.registry.dropResumeTicket(sid);
    } catch (err) {
      this.log.warn({ err: (err as Error).message, uid, sid }, 'registry cleanup failed');
    }

    this.log.info({ uid, sid, reason }, 'session offline');
    this.emit('offline', session, reason);
  }

  /**
   * Handle a kick that arrived from elsewhere in the cluster.
   * `migrated` kicks are silent: the client is already talking to the gate
   * that sent them, so there is no socket here worth notifying.
   */
  handleRemoteKick(target: { sid?: string; uid?: string }, reason: string, message?: string): boolean {
    const session = target.sid
      ? this.bySid.get(target.sid)
      : target.uid
        ? this.byUid.get(target.uid)
        : undefined;
    if (!session) return false;

    const silent = message === 'migrated';
    if (silent) {
      // The session now lives on another gate, which has already written its
      // own ownership record and resume ticket - leave both alone.
      void this.destroy(session, 'migrated', { keepResumeTicket: true });
    } else {
      const code =
        reason === KICK_REASON.DuplicateLogin
          ? CloseCode.KickedDuplicateLogin
          : CloseCode.KickedAdmin;
      this.terminate(session, reason, code, message);
    }
    return true;
  }

  private clearResumeTimer(sid: string): void {
    const timer = this.resumeTimers.get(sid);
    if (timer) {
      clearTimeout(timer);
      this.resumeTimers.delete(sid);
    }
  }

  /** Keep redis ownership alive and notice if we have been displaced. */
  private async refreshOwnership(): Promise<void> {
    for (const session of [...this.bySid.values()]) {
      if (session.state === 'closed') continue;
      try {
        const result = await this.registry.touch(
          session.uid,
          session.sid,
          this.opts.gateId,
          this.opts.registryTtlMs,
        );
        if (result === 'stolen') {
          this.log.warn(
            { uid: session.uid, sid: session.sid },
            'ownership lost to another gate, dropping local session',
          );
          this.terminate(
            session,
            KICK_REASON.DuplicateLogin,
            CloseCode.KickedDuplicateLogin,
            'logged in elsewhere',
          );
        } else if (result === 'gone' && session.state === 'active') {
          // TTL lapsed (redis restart / long stall): re-assert ownership.
          await this.registry.claim(
            {
              uid: session.uid,
              sid: session.sid,
              gate: this.opts.gateId,
              addr: this.opts.addr,
              since: session.createdAt,
            },
            this.opts.registryTtlMs,
          );
        }
      } catch (err) {
        this.log.warn({ err: (err as Error).message }, 'ownership refresh failed');
      }
    }
  }

  /**
   * Stop accepting logins and release every session.
   *
   * On graceful shutdown the resume tickets are deliberately left in redis:
   * clients reconnect (through the load balancer, to a different gate) and
   * resume there, so a rolling restart does not log players out.
   */
  async drain(reason: string, opts: { resumable?: boolean } = {}): Promise<void> {
    this.draining = true;
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = null;
    }
    const resumable = opts.resumable !== false;
    const sessions = [...this.bySid.values()];
    for (const session of sessions) {
      session.sendControl({
        t: PacketType.Kick,
        reason,
        m: 'gate shutting down',
        ...(resumable ? { resumable: true } : {}),
      });
      session.closeSocket(CloseCode.ServerShutdown, reason);
    }
    await Promise.allSettled(
      sessions.map((s) => this.destroy(s, reason, { keepResumeTicket: resumable })),
    );
    for (const timer of this.resumeTimers.values()) clearTimeout(timer);
    this.resumeTimers.clear();
  }
}
