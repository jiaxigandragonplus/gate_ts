import type { Logger } from 'pino';
import type { Player } from './player';

/**
 * In-node event bus for loose coupling between systems.
 *
 * Use it for reactions ("quest progress reacts to an item being gained").
 * Use a direct system call for anything the caller depends on succeeding -
 * events deliberately do not report failures back to the emitter.
 */
export interface GameEvent {
  player: Player;
  [key: string]: unknown;
}

export type EventHandler = (event: GameEvent) => void | Promise<void>;

export class GameEvents {
  private readonly handlers = new Map<string, EventHandler[]>();

  constructor(private readonly log: Logger) {}

  on(event: string, handler: EventHandler): this {
    const list = this.handlers.get(event);
    if (list) list.push(handler);
    else this.handlers.set(event, [handler]);
    return this;
  }

  listenerCount(event: string): number {
    return this.handlers.get(event)?.length ?? 0;
  }

  /**
   * Run every handler for `event`, in registration order.
   *
   * Emitted from inside a player's mailbox, so handlers may touch that player
   * directly. A throwing handler is logged and the rest still run: a broken
   * reaction must not fail the action that triggered it.
   */
  async emit(event: string, payload: GameEvent): Promise<void> {
    const list = this.handlers.get(event);
    if (!list) return;
    for (const handler of list) {
      try {
        await handler(payload);
      } catch (err) {
        this.log.error(
          { err: (err as Error).message, event, uid: payload.player?.uid },
          'event handler threw',
        );
      }
    }
  }
}
