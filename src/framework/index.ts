/**
 * Server-side framework: everything shared by the processes that make up this
 * cluster, so a new backend service is a handful of handlers rather than a
 * copy of the gate.
 *
 *   protocol/       wire contract (also the client's contract) + codecs
 *   redis/          cluster state: session ownership, node registry, bus
 *   util/           logging, ids, rate limiting
 *   serviceNode.ts  what a game / chat / ... service is built on
 *
 * The gate ([src/gate](../gate)) is one service built on this; the client SDK
 * ([src/client](../client)) deliberately depends on `protocol/` only, so it
 * stays usable outside node.
 *
 * This barrel is the surface a backend service needs. Reach into the
 * subdirectories directly for anything else.
 */
export { ServiceNode, ServiceError } from './serviceNode';
export type {
  ServiceNodeOptions,
  RequestContext,
  SessionContext,
  RequestHandler,
  SessionHandler,
} from './serviceNode';

export { ErrorCode, PacketType, CloseCode, KICK_REASON } from './protocol/packet';
export { logger, childLogger } from './util/logger';
