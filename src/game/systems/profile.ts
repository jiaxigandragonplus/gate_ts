import { ServiceError } from '../../framework/serviceNode';
import { ErrorCode } from '../../framework/protocol/packet';
import { GameSystem, type SystemContext, type SystemHandler } from '../system';
import type { Player } from '../player';

export interface ProfileState {
  nickname: string;
  level: number;
  exp: number;
  createdAt: number;
  lastLoginAt: number;
  loginCount: number;
}

/** exp needed to leave a given level. Flat curve; a real game reads a table. */
const expToNext = (level: number): number => 100 * level;
const MAX_LEVEL = 60;

/**
 * Identity and progression. The smallest interesting system: owns a slice,
 * exposes actions, and emits an event other systems react to.
 */
export class ProfileSystem extends GameSystem<ProfileState> {
  readonly name = 'profile';

  override createState(): ProfileState {
    return {
      nickname: '',
      level: 1,
      exp: 0,
      createdAt: Date.now(),
      lastLoginAt: 0,
      loginCount: 0,
    };
  }

  override handlers(): Record<string, SystemHandler<ProfileState>> {
    return {
      get: (ctx) => ({ ...ctx.state, expToNext: expToNext(ctx.state.level) }),

      rename: (ctx, payload) => {
        const name = String((payload as { name?: unknown })?.name ?? '').trim();
        if (name.length < 2 || name.length > 16) {
          throw new ServiceError('nickname must be 2-16 characters', ErrorCode.BadRequest);
        }
        ctx.state.nickname = name;
        this.touch(ctx.player);
        return { nickname: name };
      },

      // Exposed for the demo; in a real game exp comes from gameplay, not the
      // client. Left here to show how a system emits an event.
      addExp: async (ctx, payload) => {
        const amount = Math.max(0, Math.floor(Number((payload as { amount?: unknown })?.amount ?? 0)));
        if (amount === 0) throw new ServiceError('amount must be positive', ErrorCode.BadRequest);
        return this.grantExp(ctx.player, amount);
      },
    };
  }

  override onPlayerOnline(ctx: SystemContext<ProfileState>): void {
    ctx.state.lastLoginAt = Date.now();
    ctx.state.loginCount += 1;
    if (!ctx.state.nickname) ctx.state.nickname = `player_${ctx.player.uid.slice(-4)}`;
    this.touch(ctx.player);
  }

  // ------------------------------------------- api for other systems ------

  /**
   * Award exp, level up as needed, and tell everyone who cares.
   *
   * Takes a `Player`, not a context: any system may call this, and none of
   * them has a context typed for this system's slice.
   */
  async grantExp(
    player: Player,
    amount: number,
  ): Promise<{ level: number; exp: number; levelsGained: number }> {
    const state = this.stateOf(player);
    state.exp += amount;

    let gained = 0;
    while (state.level < MAX_LEVEL && state.exp >= expToNext(state.level)) {
      state.exp -= expToNext(state.level);
      state.level += 1;
      gained += 1;
    }
    this.touch(player);

    if (gained > 0) {
      player.push('game.profile.levelUp', { level: state.level });
      await this.shared.events.emit('profile.levelUp', {
        player,
        level: state.level,
        levelsGained: gained,
      });
    }
    return { level: state.level, exp: state.exp, levelsGained: gained };
  }

  levelOf(player: Player): number {
    return this.stateOf(player).level;
  }
}
