import type { Logger } from 'pino';
import type { Player } from './player';
import type { GameEvents } from './events';

/**
 * Minimal shape of a system, used to constrain lookups.
 *
 * `GameSystem<S>` cannot be the constraint here: `S` appears in both argument
 * and return positions, so `GameSystem<BagState>` is not assignable to
 * `GameSystem<unknown>`.
 */
export interface AnySystem {
  readonly name: string;
}

/** Looks up a sibling system. Throws on a typo rather than returning null. */
export interface SystemAccess {
  get<T extends AnySystem>(name: string): T;
  has(name: string): boolean;
  names(): string[];
}

/** Shared services handed to every system at startup. */
export interface SystemInitContext {
  systems: SystemAccess;
  events: GameEvents;
  log: Logger;
  nodeId: string;
  /** Push to any online player by uid, wherever their gate is. */
  pushToUid: (uid: string, cmd: string, payload?: unknown) => Promise<boolean>;
  /** Force a player offline (ban, anti-cheat). */
  kick: (uid: string, reason?: string, message?: string) => Promise<boolean>;
}

/** Context for one message, already inside the player's mailbox. */
export interface SystemContext<S = unknown> {
  readonly player: Player;
  /** This system's state slice, typed. */
  readonly state: S;
  readonly systems: SystemAccess;
  readonly events: GameEvents;
  readonly log: Logger;
  /** Command that got us here, e.g. "game.bag.use". */
  readonly cmd: string;
}

/**
 * A handler for one action. Its return value becomes the response payload;
 * throw a ServiceError to send a structured error back to the client.
 */
export type SystemHandler<S = unknown> = (
  ctx: SystemContext<S>,
  payload: unknown,
) => unknown | Promise<unknown>;

/**
 * One business system: a slice of player state plus the actions on it.
 *
 * Commands are routed as `<service>.<system>.<action>`, so `game.bag.use`
 * reaches the `use` handler of the system named `bag`. Adding a system needs
 * no gate change - the gate routes the whole `game.` prefix and never looks
 * inside.
 *
 * Systems must not touch another system's slice directly. Call the owning
 * system's methods (`ctx.systems.get<BagSystem>('bag').addItems(...)`) or
 * react to its events.
 */
export abstract class GameSystem<S = unknown> {
  /** Second command segment, e.g. "bag" for game.bag.*. */
  abstract readonly name: string;

  /** Fresh slice for a player who has never used this system. */
  abstract createState(): S;

  /** Action name -> handler. */
  handlers(): Record<string, SystemHandler<S>> {
    return {};
  }

  /** Called once at startup, after every system is constructed. */
  init?(ctx: SystemInitContext): void | Promise<void>;

  /** Player came online (fresh login or resumed session). */
  onPlayerOnline?(ctx: SystemContext<S>): void | Promise<void>;

  /** Player went offline; state is still loaded and saveable here. */
  onPlayerOffline?(ctx: SystemContext<S>): void | Promise<void>;

  /** Periodic hook, run inside the player's mailbox. */
  onTick?(ctx: SystemContext<S>): void | Promise<void>;

  /**
   * Upgrade a slice persisted by an older build. Return the migrated value,
   * or undefined to keep the raw one.
   */
  migrate?(raw: unknown): S | undefined;

  // ----------------------------------------------- helpers for subclasses --

  /**
   * Shared services, available from `init()` onwards.
   *
   * This is what lets a system's public API take just a `Player`: a method
   * another system calls must not require a context typed for the *caller's*
   * state slice, so it reads its own slice via `stateOf` and reaches events,
   * pushes and kicks through here.
   */
  protected shared!: SystemInitContext;

  /** @internal Called by the node before `init()`. */
  attach(shared: SystemInitContext): void {
    this.shared = shared;
  }

  /** This system's slice of `player`. Use in cross-system methods. */
  protected stateOf(player: Player): S {
    return player.state<S>(this.name);
  }

  /** Mark this system's slice as changed so it gets persisted. */
  protected touch(player: Player): void {
    player.markDirty(this.name);
  }
}

/**
 * A system whose state type is not known at the use site.
 *
 * TypeScript has no existential types, so a heterogeneous collection of
 * `GameSystem<S>` needs `any` here - `unknown` does not work because `S`
 * appears in both argument and return positions and is therefore invariant.
 * Confined to this alias so it does not spread through the codebase.
 */
/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
export type SomeSystem = GameSystem<any>;

/** Registry + router: turns "bag.use" into a call on the right system. */
export class SystemRegistry implements SystemAccess {
  private readonly systems = new Map<string, SomeSystem>();
  private readonly routes = new Map<
    string,
    { system: SomeSystem; handler: SystemHandler<never> }
  >();

  add(system: SomeSystem): void {
    if (!system.name || /[.\s]/.test(system.name)) {
      throw new Error(`invalid system name "${system.name}": no dots or whitespace`);
    }
    if (this.systems.has(system.name)) {
      throw new Error(`system "${system.name}" is registered twice`);
    }
    this.systems.set(system.name, system);

    for (const [action, handler] of Object.entries(system.handlers())) {
      if (!action || /[.\s]/.test(action)) {
        throw new Error(`invalid action "${system.name}.${action}": no dots or whitespace`);
      }
      this.routes.set(`${system.name}.${action}`, {
        system,
        handler: handler as SystemHandler<never>,
      });
    }
  }

  get<T extends AnySystem>(name: string): T {
    const system = this.systems.get(name);
    if (!system) {
      throw new Error(
        `system "${name}" is not registered (have: ${[...this.systems.keys()].join(', ')})`,
      );
    }
    return system as unknown as T;
  }

  has(name: string): boolean {
    return this.systems.has(name);
  }

  names(): string[] {
    return [...this.systems.keys()];
  }

  all(): SomeSystem[] {
    return [...this.systems.values()];
  }

  /** `key` is "<system>.<action>". */
  route(key: string): { system: SomeSystem; handler: SystemHandler<never> } | undefined {
    return this.routes.get(key);
  }

  describe(): Array<{ system: string; actions: string[] }> {
    return this.all().map((s) => ({
      system: s.name,
      actions: Object.keys(s.handlers()),
    }));
  }
}
