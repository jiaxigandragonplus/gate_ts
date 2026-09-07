import { describe, expect, it, vi } from 'vitest';
import { MailboxFullError, Player } from '../../src/game/player';
import { emptyRecord } from '../../src/game/store';

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

function makePlayer(overrides: { mailboxLimit?: number } = {}) {
  const pushes: Array<{ cmd: string; payload: unknown }> = [];
  const player = new Player({
    uid: 'u1',
    record: emptyRecord('u1'),
    mailboxLimit: overrides.mailboxLimit ?? 8,
    slowHandlerMs: 10_000,
    push: (_p, cmd, payload) => pushes.push({ cmd, payload }),
  });
  return { player, pushes };
}

/** Fails fast and clearly instead of hanging until the test timeout. */
async function within<T>(ms: number, promise: Promise<T>, what: string): Promise<T> {
  let timer: NodeJS.Timeout;
  const guard = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} did not settle within ${ms}ms`)), ms);
  });
  try {
    return await Promise.race([promise, guard]);
  } finally {
    clearTimeout(timer!);
  }
}

describe('Player mailbox', () => {
  it('serialises concurrent messages for the same player', async () => {
    // The property that matters: a handler may await, and no other message
    // for this player runs in the gap. Without serialisation all three tasks
    // read 0 and the counter ends at 1.
    const { player } = makePlayer();
    const counter = { value: 0 };

    const increment = () =>
      player.enqueue('incr', async () => {
        const seen = counter.value;
        await sleep(5);
        counter.value = seen + 1;
      });

    await Promise.all([increment(), increment(), increment()]);
    expect(counter.value).toBe(3);
  });

  it('runs messages in the order they arrived', async () => {
    const { player } = makePlayer();
    const order: number[] = [];
    await Promise.all(
      [1, 2, 3, 4].map((n) =>
        player.enqueue(`task-${n}`, async () => {
          await sleep(n === 1 ? 8 : 1);
          order.push(n);
        }),
      ),
    );
    expect(order).toEqual([1, 2, 3, 4]);
  });

  it('returns each caller its own result', async () => {
    const { player } = makePlayer();
    const results = await Promise.all([
      player.enqueue('a', () => 'first'),
      player.enqueue('b', async () => {
        await sleep(2);
        return 'second';
      }),
    ]);
    expect(results).toEqual(['first', 'second']);
  });

  it('propagates a handler error to that caller only', async () => {
    const { player } = makePlayer();
    const failing = player.enqueue('boom', async () => {
      await sleep(1);
      throw new Error('handler exploded');
    });
    const ok = player.enqueue('fine', () => 'still works');

    await expect(failing).rejects.toThrow('handler exploded');
    expect(await ok).toBe('still works');
  });

  it('runs a re-entrant enqueue inline instead of deadlocking', async () => {
    // A system method that enqueues on the player it is already processing
    // would otherwise wait for a task that can never start.
    const { player } = makePlayer();
    let inner = false;

    await within(
      500,
      player.enqueue('outer', async () => {
        expect(player.inMailbox).toBe(true);
        await player.enqueue('inner', async () => {
          await sleep(1);
          inner = true;
        });
      }),
      're-entrant enqueue',
    );
    expect(inner).toBe(true);
  });

  it('knows when it is not inside the mailbox', () => {
    const { player } = makePlayer();
    expect(player.inMailbox).toBe(false);
  });

  it('rejects new messages once the queue is full', async () => {
    const { player } = makePlayer({ mailboxLimit: 3 });
    const slow = () => player.enqueue('slow', () => sleep(30));

    // One starts draining immediately; the next three fill the queue.
    const inflight = [slow(), slow(), slow(), slow()];
    await expect(player.enqueue('overflow', () => 'nope')).rejects.toThrow(MailboxFullError);
    await Promise.all(inflight);

    // Space frees up again once the backlog drains.
    expect(await player.enqueue('after', () => 'ok')).toBe('ok');
  });

  it('reports queue depth so a hot player is visible', async () => {
    const { player } = makePlayer();
    const inflight = [1, 2, 3].map(() => player.enqueue('slow', () => sleep(10)));
    expect(player.queueDepth).toBeGreaterThan(0);
    await Promise.all(inflight);
    expect(player.queueDepth).toBe(0);
    expect(player.peakQueueDepth).toBeGreaterThanOrEqual(2);
  });

  it('warns when one handler holds the mailbox too long', async () => {
    const onSlowHandler = vi.fn();
    const player = new Player({
      uid: 'u1',
      record: emptyRecord('u1'),
      mailboxLimit: 8,
      slowHandlerMs: 5,
      push: () => undefined,
      onSlowHandler,
    });
    await player.enqueue('slow', () => sleep(20));
    expect(onSlowHandler).toHaveBeenCalledWith(
      expect.objectContaining({ uid: 'u1', label: 'slow' }),
    );
  });

  it('quiesces only once the backlog is done', async () => {
    const { player } = makePlayer();
    const done: number[] = [];
    for (const n of [1, 2, 3]) {
      void player.enqueue(`t${n}`, async () => {
        await sleep(3);
        done.push(n);
      });
    }
    await within(1000, player.quiesce(), 'quiesce');
    expect(done).toEqual([1, 2, 3]);
  });

  it('refuses work after being unloaded', async () => {
    const { player } = makePlayer();
    player.markClosed();
    await expect(player.enqueue('late', () => 1)).rejects.toThrow(/unloaded/);
  });
});

describe('Player state', () => {
  it('keeps per-system slices separate and tracks dirtiness', () => {
    const { player } = makePlayer();
    expect(player.dirty).toBe(false);

    player.setState('bag', { slots: [] });
    player.setState('profile', { level: 1 });
    expect(player.dirty).toBe(true);
    expect(player.state('bag')).toEqual({ slots: [] });
    expect(player.hasState('quest')).toBe(false);

    const record = player.toRecord();
    expect(Object.keys(record.systems).sort()).toEqual(['bag', 'profile']);
    expect(record.version).toBe(1);

    player.onSaved(record);
    expect(player.dirty).toBe(false);

    // A mutation is only persisted if the system says so.
    (player.state('bag') as { slots: number[] }).slots.push(1);
    expect(player.dirty).toBe(false);
    player.markDirty('bag');
    expect(player.dirty).toBe(true);
  });

  it('only pushes while online', () => {
    const { player, pushes } = makePlayer();
    player.push('x.y', { a: 1 });
    expect(pushes).toHaveLength(0);

    player.bindSession('gate-1', 'sid-1');
    player.push('x.y', { a: 1 });
    expect(pushes).toEqual([{ cmd: 'x.y', payload: { a: 1 } }]);

    player.markOffline();
    player.push('x.y', {});
    expect(pushes).toHaveLength(1);
  });
});
