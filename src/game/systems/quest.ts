import { ServiceError } from '../../framework/serviceNode';
import { ErrorCode } from '../../framework/protocol/packet';
import { GameSystem, type SystemHandler, type SystemInitContext } from '../system';
import type { Player } from '../player';
import type { BagSystem, ItemStack } from './bag';

export interface QuestProgress {
  id: number;
  progress: number;
  target: number;
  claimed: boolean;
}

export interface QuestState {
  active: QuestProgress[];
  completed: number[];
}

interface QuestDef {
  id: number;
  target: number;
  /** Event that advances it. */
  trigger: 'profile.levelUp' | 'bag.itemGained';
  reward: ItemStack[];
}

/** A real game loads these from config; inline here to keep the demo honest. */
const QUESTS: QuestDef[] = [
  { id: 1, target: 2, trigger: 'profile.levelUp', reward: [{ id: 2001, count: 1 }] },
  { id: 2, target: 5, trigger: 'bag.itemGained', reward: [{ id: 2002, count: 3 }] },
];

const defOf = (id: number): QuestDef | undefined => QUESTS.find((q) => q.id === id);

/**
 * Quests. The system that shows loose coupling: it never gets called by
 * profile or bag - it *reacts* to their events, so those systems know nothing
 * about quests existing.
 */
export class QuestSystem extends GameSystem<QuestState> {
  readonly name = 'quest';

  override createState(): QuestState {
    return { active: [], completed: [] };
  }

  override init(shared: SystemInitContext): void {
    // Subscribed here rather than in the handler map: these are reactions to
    // other systems, not client-facing actions.
    shared.events.on('profile.levelUp', (event) => {
      this.advance(event.player, 'profile.levelUp', Number(event['levelsGained'] ?? 1));
    });
    shared.events.on('bag.itemGained', (event) => {
      const items = (event['items'] as ItemStack[] | undefined) ?? [];
      const total = items.reduce((sum, i) => sum + i.count, 0);
      this.advance(event.player, 'bag.itemGained', total);
    });
  }

  override handlers(): Record<string, SystemHandler<QuestState>> {
    return {
      list: (ctx) => ({
        active: ctx.state.active,
        completed: ctx.state.completed,
        available: QUESTS.filter(
          (q) =>
            !ctx.state.active.some((a) => a.id === q.id) && !ctx.state.completed.includes(q.id),
        ).map((q) => ({ id: q.id, target: q.target, trigger: q.trigger })),
      }),

      accept: (ctx, payload) => {
        const id = Number((payload as { id?: unknown })?.id);
        const def = defOf(id);
        if (!def) throw new ServiceError(`unknown quest ${id}`, ErrorCode.BadRequest);
        if (ctx.state.completed.includes(id)) {
          throw new ServiceError('quest already completed', ErrorCode.BadRequest);
        }
        if (ctx.state.active.some((q) => q.id === id)) {
          throw new ServiceError('quest already accepted', ErrorCode.BadRequest);
        }
        const entry: QuestProgress = { id, progress: 0, target: def.target, claimed: false };
        ctx.state.active.push(entry);
        this.touch(ctx.player);
        return entry;
      },

      claim: (ctx, payload) => {
        const id = Number((payload as { id?: unknown })?.id);
        const index = ctx.state.active.findIndex((q) => q.id === id);
        const entry = index >= 0 ? (ctx.state.active[index] as QuestProgress) : undefined;
        if (!entry) throw new ServiceError(`quest ${id} is not active`, ErrorCode.BadRequest);
        if (entry.progress < entry.target) {
          throw new ServiceError('quest is not finished', ErrorCode.BadRequest);
        }

        const def = defOf(id) as QuestDef;
        // Grant through the owning system, never by touching its slice.
        ctx.systems.get<BagSystem>('bag').add(ctx.player, def.reward);

        ctx.state.active.splice(index, 1);
        ctx.state.completed.push(id);
        this.touch(ctx.player);
        return { claimed: id, reward: def.reward };
      },
    };
  }

  /**
   * Advance every active quest listening for `trigger`.
   *
   * Called from an event handler, which runs inside the player's mailbox, so
   * touching state here needs no extra locking.
   */
  private advance(player: Player, trigger: QuestDef['trigger'], amount: number): void {
    if (amount <= 0 || !player.hasState(this.name)) return;
    const state = this.stateOf(player);
    let changed = false;

    for (const entry of state.active) {
      const def = defOf(entry.id);
      if (!def || def.trigger !== trigger || entry.progress >= entry.target) continue;
      entry.progress = Math.min(entry.target, entry.progress + amount);
      changed = true;
      if (entry.progress >= entry.target) {
        player.push('game.quest.finished', { id: entry.id });
      }
    }
    if (changed) this.touch(player);
  }
}
