import type Redis from 'ioredis';

/**
 * One player's persisted state, as a bag of per-system slices.
 *
 * Systems own their own slice and never see each other's, so adding a system
 * does not change this record's shape or require a migration.
 */
export interface PlayerRecord {
  uid: string;
  /** systemName -> that system's serialized state. */
  systems: Record<string, unknown>;
  savedAt: number;
  /** Bumped on every save; lets a store detect a lost update. */
  version: number;
}

/**
 * Persistence seam. The shipped implementations are deliberately simple -
 * a real project swaps in MySQL / Mongo / whatever it already operates and
 * nothing above this interface changes.
 */
export interface PlayerStore {
  load(uid: string): Promise<PlayerRecord | null>;
  save(record: PlayerRecord): Promise<void>;
  /** Optional bulk save; defaults to a loop. */
  saveMany?(records: PlayerRecord[]): Promise<void>;
  close?(): Promise<void>;
}

export function emptyRecord(uid: string): PlayerRecord {
  return { uid, systems: {}, savedAt: 0, version: 0 };
}

/** For tests and single-process development. */
export class MemoryPlayerStore implements PlayerStore {
  private readonly rows = new Map<string, PlayerRecord>();

  async load(uid: string): Promise<PlayerRecord | null> {
    const row = this.rows.get(uid);
    return row ? (JSON.parse(JSON.stringify(row)) as PlayerRecord) : null;
  }

  async save(record: PlayerRecord): Promise<void> {
    this.rows.set(record.uid, JSON.parse(JSON.stringify(record)) as PlayerRecord);
  }

  get size(): number {
    return this.rows.size;
  }
}

/**
 * JSON-per-player in redis. Fine for development and small scale; for a real
 * game replace it with your database of record - this store has no indexes,
 * no partial updates and no query surface by design.
 */
export class RedisPlayerStore implements PlayerStore {
  constructor(
    private readonly redis: Redis,
    private readonly prefix: string,
  ) {}

  private key(uid: string): string {
    return `${this.prefix}:player:${uid}`;
  }

  async load(uid: string): Promise<PlayerRecord | null> {
    const raw = await this.redis.get(this.key(uid));
    if (!raw) return null;
    try {
      return JSON.parse(raw) as PlayerRecord;
    } catch {
      // Corrupt row: treat as missing rather than crashing the login.
      return null;
    }
  }

  async save(record: PlayerRecord): Promise<void> {
    await this.redis.set(this.key(record.uid), JSON.stringify(record));
  }

  async saveMany(records: PlayerRecord[]): Promise<void> {
    if (records.length === 0) return;
    const pipeline = this.redis.pipeline();
    for (const record of records) {
      pipeline.set(this.key(record.uid), JSON.stringify(record));
    }
    await pipeline.exec();
  }
}
