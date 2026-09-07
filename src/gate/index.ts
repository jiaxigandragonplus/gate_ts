/**
 * The gate server.
 *
 * Everything under this directory belongs to the gate process itself:
 * the client-facing WebSocket layer, JWT verification, session lifecycle,
 * command routing and the ops surface.
 *
 * The modules it shares with other participants live one level up:
 *   ../protocol  wire contract, spoken by clients, gates and services alike
 *   ../redis     cluster state (session ownership, node registry) and the bus
 *   ../sdk       ServiceNode (backend services) and GateClient (clients)
 *   ../util      logging, ids, rate limiting
 */
export { Gate } from './gate';
export { loadConfig } from './config';
export type { GateConfig, JwtConfig } from './config';
export type { RouteRule, ResolvedRoute } from './router/routeTable';
