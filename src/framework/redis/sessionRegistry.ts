import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import type Redis from 'ioredis';
import type { Keys } from './keys';
import { logger } from '../util/logger';

/** Cluster-wide record of which gate currently owns an account. */
export interface SessionOwner {
  uid: string;
  sid: string;
  gate: string;
  /** Address of the owning gate, so clients can be redirected on resume. */
  addr: string;
  since: number;
  device?: string;
}

export interface ResumeTicket {
  uid: string;
  sid: string;
  gate: string;
  addr: string;
  /** sha256 of the resume secret handed to the client. */
  hash: string;
  createdAt: number;
}

export interface ClaimResult {
  /** The owner that was displaced, if the account was already logged in. */
  previous: SessionOwner | null;
}

const CLAIM_LUA = `
local old = redis.call('GET', KEYS[1])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[2])
return old
`;

// Both compare the gate id as well as the session id: after a session
// migrates, the old gate must never delete or extend the new owner's record.
const RELEASE_LUA = `
local cur = redis.call('GET', KEYS[1])
if not cur then return 0 end
local ok, o = pcall(cjson.decode, cur)
if ok and o and o.sid == ARGV[1] and o.gate == ARGV[2] then
  redis.call('DEL', KEYS[1])
  redis.call('DEL', KEYS[2])
  return 1
end
return 0
`;

const TOUCH_LUA = `
local cur = redis.call('GET', KEYS[1])
if not cur then return 0 end
local ok, o = pcall(cjson.decode, cur)
if ok and o and o.sid == ARGV[1] and o.gate == ARGV[2] then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
  return 1
end
return -1
`;

interface ScriptedRedis extends Redis {
  gateClaimSession(k1: string, k2: string, a1: string, a2: string, a3: string): Promise<string | null>;
  gateReleaseSession(k1: string, k2: string, a1: string, a2: string): Promise<number>;
  gateTouchSession(k1: string, k2: string, a1: string, a2: string, a3: string): Promise<number>;
}

/**
 * Single source of truth for "who is logged in where".
 *
 * `claim` is atomic: two simultaneous logins of the same account can never
 * both believe they own it, which is what makes duplicate-login kicks
 * (顶号踢人) correct under horizontal scale.
 */
export class SessionRegistry {
  private readonly redis: ScriptedRedis;
  private readonly log = logger.child({ mod: 'session-registry' });

  constructor(
    redis: Redis,
    private readonly keys: Keys,
  ) {
    this.redis = redis as ScriptedRedis;
    redis.defineCommand('gateClaimSession', { numberOfKeys: 2, lua: CLAIM_LUA });
    redis.defineCommand('gateReleaseSession', { numberOfKeys: 2, lua: RELEASE_LUA });
    redis.defineCommand('gateTouchSession', { numberOfKeys: 2, lua: TOUCH_LUA });
  }

  /** Take ownership of `owner.uid`, returning whoever held it before. */
  async claim(owner: SessionOwner, ttlMs: number): Promise<ClaimResult> {
    const raw = await this.redis.gateClaimSession(
      this.keys.session(owner.uid),
      this.keys.sidIndex(owner.sid),
      JSON.stringify(owner),
      String(ttlMs),
      owner.uid,
    );
    const previous = parseOwner(raw);
    if (previous && previous.sid !== owner.sid) {
      // Best-effort cleanup of the displaced session's lookup entries.
      await this.redis
        .pipeline()
        .del(this.keys.sidIndex(previous.sid))
        .del(this.keys.resumeTicket(previous.sid))
        .exec()
        .catch((err: Error) => this.log.warn({ err: err.message }, 'stale index cleanup failed'));
    }
    return { previous: previous && previous.sid !== owner.sid ? previous : null };
  }

  /** Refresh the TTL, but only while we are still the owner. */
  async touch(
    uid: string,
    sid: string,
    gate: string,
    ttlMs: number,
  ): Promise<'ok' | 'gone' | 'stolen'> {
    const r = await this.redis.gateTouchSession(
      this.keys.session(uid),
      this.keys.sidIndex(sid),
      sid,
      gate,
      String(ttlMs),
    );
    return r === 1 ? 'ok' : r === 0 ? 'gone' : 'stolen';
  }

  /** Drop ownership, but only if we still hold it (never clobber a takeover). */
  async release(uid: string, sid: string, gate: string): Promise<boolean> {
    const r = await this.redis.gateReleaseSession(
      this.keys.session(uid),
      this.keys.sidIndex(sid),
      sid,
      gate,
    );
    return r === 1;
  }

  async get(uid: string): Promise<SessionOwner | null> {
    return parseOwner(await this.redis.get(this.keys.session(uid)));
  }

  async getBySid(sid: string): Promise<SessionOwner | null> {
    const uid = await this.redis.get(this.keys.sidIndex(sid));
    if (!uid) return null;
    return this.get(uid);
  }

  // ------------------------------------------------------------- resume ----

  /**
   * Issue resume credentials. The plaintext secret is returned to the caller
   * (and only ever stored client-side); redis keeps a hash of it.
   */
  async issueResumeTicket(
    owner: SessionOwner,
    ttlMs: number,
  ): Promise<{ secret: string; ticket: ResumeTicket }> {
    const secret = randomBytes(24).toString('base64url');
    const ticket: ResumeTicket = {
      uid: owner.uid,
      sid: owner.sid,
      gate: owner.gate,
      addr: owner.addr,
      hash: sha256(secret),
      createdAt: Date.now(),
    };
    await this.redis.set(this.keys.resumeTicket(owner.sid), JSON.stringify(ticket), 'PX', ttlMs);
    return { secret, ticket };
  }

  /** Extend the resume window (called when a live session is suspended). */
  async refreshResumeTicket(sid: string, ttlMs: number): Promise<void> {
    await this.redis.pexpire(this.keys.resumeTicket(sid), ttlMs);
  }

  async getResumeTicket(sid: string): Promise<ResumeTicket | null> {
    const raw = await this.redis.get(this.keys.resumeTicket(sid));
    if (!raw) return null;
    try {
      return JSON.parse(raw) as ResumeTicket;
    } catch {
      return null;
    }
  }

  async dropResumeTicket(sid: string): Promise<void> {
    await this.redis.del(this.keys.resumeTicket(sid));
  }

  /** Constant-time secret check. */
  static verifySecret(ticket: ResumeTicket, secret: string): boolean {
    const a = Buffer.from(ticket.hash, 'hex');
    const b = Buffer.from(sha256(secret), 'hex');
    return a.length === b.length && timingSafeEqual(a, b);
  }
}

function sha256(s: string): string {
  return createHash('sha256').update(s).digest('hex');
}

function parseOwner(raw: string | null): SessionOwner | null {
  if (!raw) return null;
  try {
    const o = JSON.parse(raw) as SessionOwner;
    return typeof o.uid === 'string' && typeof o.sid === 'string' ? o : null;
  } catch {
    return null;
  }
}
