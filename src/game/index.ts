/**
 * The game node: the process that carries business logic.
 *
 * Business is divided into systems (./systems). A system owns one slice of
 * player state and the actions on it; commands route as
 * `<service>.<system>.<action>`, so `game.bag.use` reaches the `use` handler
 * of the `bag` system. The gate routes the whole `game.` prefix and never
 * looks inside, so adding a system needs no gate change.
 */
export { GameNode } from './gameNode';
export { loadGameConfig } from './config';
export type { GameConfig } from './config';
export { GameSystem, SystemRegistry } from './system';
export type {
  SystemContext,
  SystemHandler,
  SystemInitContext,
  SystemAccess,
  AnySystem,
  SomeSystem,
} from './system';
export { Player, MailboxFullError } from './player';
export { GameEvents } from './events';
export type { GameEvent, EventHandler } from './events';
export { PlayerManager, PlayerBusyElsewhereError } from './playerManager';
export { MemoryPlayerStore, RedisPlayerStore, emptyRecord } from './store';
export type { PlayerStore, PlayerRecord } from './store';

export { ProfileSystem } from './systems/profile';
export type { ProfileState } from './systems/profile';
export { BagSystem } from './systems/bag';
export type { BagState, ItemStack } from './systems/bag';
export { QuestSystem } from './systems/quest';
export type { QuestState, QuestProgress } from './systems/quest';
