import { AsyncLocalStorage } from 'node:async_hooks';
import type { PlayerRecord } from './store';
import { emptyRecord } from './store';

/**
 * Tracks which player's mailbox the current async call stack belongs to.
 *
 * Node runs one callback at a time but a handler yields at every `await`, so
 * without this a system method that re-enters `enqueue` on the player it is
 * already processing would wait for a task that can never start - a deadlock.
 * With it, such a call runs inline, which is safe precisely because the
 * mailbox guarantees no other task is interleaved.
 */
const currentMailbox = new AsyncLocalStorage<Player>();

export class MailboxFullError extends Error {
  constructor(uid: string, limit: number) {
    super(`player ${uid} has ${limit} queued messages; rejecting`);
    this.name = 'MailboxFullError';
  }
}

export interface PlayerOptions {
  uid: string;
  record: PlayerRecord;
  mailboxLimit: number;
  slowHandlerMs: number;
  /** Injected by the manager: delivers a push through the owning gate. */
  push: (player: Player, cmd: string, payload?: unknown) => void;
  onSlowHandler?: (info: { uid: string; label: string; ms: number }) => void;
}

interface Task {
  label: string;
  run: () => Promise<void>;
  queuedAt: number;
}

/**
 * One online player, living in memory on the game node the gate pinned them
 * to. State is split into per-system slices; systems only ever touch their
 * own, which is what keeps them independent.
 *
 * Every message for a player runs through its mailbox, one at a time. That is
 * the whole concurrency model: inside a handler you can `await` freely without
 * another request for the same player interleaving and corrupting state.
 */
export class Player {
  readonly uid: string;
  readonly loadedAt = Date.now();
  lastActiveAt = Date.now();

  /** Gate that currently owns the session; changes if the client migrates. */
  gate = '';
  sid = '';
  online = false;

  private readonly slices = new Map<string, unknown>();
  private readonly dirtySystems = new Set<string>();
  private version: number;
  private readonly queue: Task[] = [];
  private draining = false;
  private closed = false;
  private peakQueue = 0;

  constructor(private readonly opts: PlayerOptions) {
    this.uid = opts.uid;
    this.version = opts.record.version;
    for (const [name, value] of Object.entries(opts.record.systems)) {
      this.slices.set(name, value);
    }
  }

  // ------------------------------------------------------------- state ----

  /** This system's slice. Missing slices are created by the manager on load. */
  state<T>(system: string): T {
    return this.slices.get(system) as T;
  }

  hasState(system: string): boolean {
    return this.slices.has(system);
  }

  /** Used by the manager when loading, and by a system resetting its slice. */
  setState(system: string, value: unknown): void {
    this.slices.set(system, value);
    this.dirtySystems.add(system);
  }

  /**
   * Mark a slice as changed. Nothing is persisted without this - a mutation
   * without markDirty is the classic "my item vanished after restart" bug.
   */
  markDirty(system: string): void {
    this.dirtySystems.add(system);
  }

  get dirty(): boolean {
    return this.dirtySystems.size > 0;
  }

  /** Snapshot for persistence. Clears the dirty set only on confirmed save. */
  toRecord(): PlayerRecord {
    const record = emptyRecord(this.uid);
    record.systems = Object.fromEntries(this.slices);
    record.savedAt = Date.now();
    record.version = this.version + 1;
    return record;
  }

  onSaved(record: PlayerRecord): void {
    this.version = record.version;
    this.dirtySystems.clear();
  }

  // ----------------------------------------------------------- mailbox ----

  get queueDepth(): number {
    return this.queue.length;
  }

  get peakQueueDepth(): number {
    return this.peakQueue;
  }

  /** True when the caller is already running inside this player's mailbox. */
  get inMailbox(): boolean {
    return currentMailbox.getStore() === this;
  }

  /**
   * Run `fn` with exclusive access to this player.
   *
   * Re-entrant calls (from inside this player's own task) execute inline
   * rather than deadlocking. Everything else queues.
   */
  async enqueue<T>(label: string, fn: () => Promise<T> | T): Promise<T> {
    if (this.closed) throw new Error(`player ${this.uid} is unloaded`);
    if (this.inMailbox) return fn();
    if (this.queue.length >= this.opts.mailboxLimit) {
      throw new MailboxFullError(this.uid, this.opts.mailboxLimit);
    }

    return new Promise<T>((resolve, reject) => {
      this.queue.push({
        label,
        queuedAt: Date.now(),
        run: async () => {
          const startedAt = Date.now();
          try {
            resolve(await currentMailbox.run(this, fn));
          } catch (err) {
            reject(err as Error);
          } finally {
            const ms = Date.now() - startedAt;
            if (ms >= this.opts.slowHandlerMs) {
              // A slow handler blocks every other message for this player.
              this.opts.onSlowHandler?.({ uid: this.uid, label, ms });
            }
          }
        },
      });
      this.peakQueue = Math.max(this.peakQueue, this.queue.length);
      void this.drain();
    });
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    try {
      while (this.queue.length > 0) {
        const task = this.queue.shift() as Task;
        this.lastActiveAt = Date.now();
        await task.run();
      }
    } finally {
      this.draining = false;
    }
  }

  /** Wait for the mailbox to empty; used before unloading. */
  async quiesce(): Promise<void> {
    while (this.queue.length > 0 || this.draining) {
      await new Promise((resolve) => setImmediate(resolve));
    }
  }

  // ------------------------------------------------------------ session ---

  bindSession(gate: string, sid: string): void {
    this.gate = gate;
    this.sid = sid;
    this.online = true;
    this.lastActiveAt = Date.now();
  }

  markOffline(): void {
    this.online = false;
  }

  /** Push to this player's client, through whichever gate owns them now. */
  push(cmd: string, payload?: unknown): void {
    if (!this.online) return;
    this.opts.push(this, cmd, payload);
  }

  markClosed(): void {
    this.closed = true;
  }

  get isClosed(): boolean {
    return this.closed;
  }
}
