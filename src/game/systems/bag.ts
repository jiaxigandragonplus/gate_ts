import { ServiceError } from '../../framework/serviceNode';
import { ErrorCode } from '../../framework/protocol/packet';
import { GameSystem, type SystemHandler } from '../system';
import type { Player } from '../player';
import type { ProfileSystem } from './profile';

export interface ItemStack {
  id: number;
  count: number;
}

export interface BagState {
  slots: ItemStack[];
  capacity: number;
}

const DEFAULT_CAPACITY = 30;
/** Items that do something when used. A real game loads this from config. */
const CONSUMABLE_EXP: Record<number, number> = { 1001: 50, 1002: 200 };

/**
 * Inventory. Shows the two things every stateful system needs: validation
 * that rejects with a client-visible error, and a public API other systems
 * call instead of touching this slice themselves.
 */
export class BagSystem extends GameSystem<BagState> {
  readonly name = 'bag';

  override createState(): BagState {
    return { slots: [], capacity: DEFAULT_CAPACITY };
  }

  override handlers(): Record<string, SystemHandler<BagState>> {
    return {
      list: (ctx) => ({ slots: ctx.state.slots, capacity: ctx.state.capacity }),

      use: async (ctx, payload) => {
        const { id, count = 1 } = (payload ?? {}) as { id?: number; count?: number };
        const itemId = Number(id);
        const n = Math.max(1, Math.floor(Number(count)));
        if (!Number.isInteger(itemId)) {
          throw new ServiceError('item id is required', ErrorCode.BadRequest);
        }

        const exp = CONSUMABLE_EXP[itemId];
        if (exp === undefined) {
          throw new ServiceError(`item ${itemId} is not usable`, ErrorCode.BadRequest);
        }

        // Validate everything before mutating anything: the mailbox stops
        // other messages from observing an intermediate state, but it cannot
        // undo a half-applied change if a later step throws.
        if (this.countOf(ctx.player, itemId) < n) {
          throw new ServiceError(`not enough of item ${itemId}`, ErrorCode.BadRequest);
        }

        this.remove(ctx.player, itemId, n);
        const progress = await ctx.systems
          .get<ProfileSystem>('profile')
          .grantExp(ctx.player, exp * n);
        return { used: { id: itemId, count: n }, progress };
      },
    };
  }

  // ------------------------------------------- api for other systems ------

  /** Add items, respecting capacity. Throws when the bag is full. */
  add(player: Player, items: ItemStack[]): ItemStack[] {
    const state = this.stateOf(player);
    for (const item of items) {
      if (item.count <= 0) continue;
      const slot = state.slots.find((s) => s.id === item.id);
      if (slot) {
        slot.count += item.count;
      } else {
        if (state.slots.length >= state.capacity) {
          throw new ServiceError('bag is full', ErrorCode.BadRequest);
        }
        state.slots.push({ id: item.id, count: item.count });
      }
    }
    this.touch(player);
    return state.slots;
  }

  /** Remove items. Throws when the player does not have enough. */
  remove(player: Player, id: number, count: number): void {
    const state = this.stateOf(player);
    const index = state.slots.findIndex((s) => s.id === id);
    const slot = index >= 0 ? (state.slots[index] as ItemStack) : undefined;
    if (!slot || slot.count < count) {
      throw new ServiceError(`not enough of item ${id}`, ErrorCode.BadRequest);
    }
    slot.count -= count;
    if (slot.count === 0) state.slots.splice(index, 1);
    this.touch(player);
  }

  countOf(player: Player, id: number): number {
    return this.stateOf(player).slots.find((s) => s.id === id)?.count ?? 0;
  }
}
