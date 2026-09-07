/**
 * Internal (cluster-side) message protocol.
 *
 * Two directions, both carried over redis pub/sub:
 *
 *   gate -> service   channel `<prefix>:svc:<service>:<nodeId>`
 *   service -> gate   channel `<prefix>:node:<gateId>`  (or `<prefix>:node:all`)
 *
 * Gate-to-gate control traffic (takeover kicks, broadcasts) uses the same
 * inbound node channel, so a gate only ever needs one subscription.
 */
import type { ErrorCode } from './packet';
import type { InternalPayload } from './payload';

/**
 * Payloads travel in one of two fields, never both: `d` for a JSON payload,
 * `db` for base64 of an opaque byte payload (a protobuf client's own
 * message). Use the helpers in ./payload.ts rather than reading them
 * directly - see InternalPayload.
 */

// ---------------------------------------------------------------- upstream --

export interface UpstreamMeta {
  ip?: string;
  device?: string;
  /** Millis since the session was established. */
  connectedAt: number;
}

/** A client Request forwarded to a service. The service must reply with `resp`. */
export interface UpRequest extends InternalPayload {
  k: 'req';
  gate: string;
  sid: string;
  uid: string;
  id: number;
  cmd: string;
  ts: number;
  meta?: UpstreamMeta;
}

/** A client Notify forwarded to a service. No reply expected. */
export interface UpNotify extends InternalPayload {
  k: 'notify';
  gate: string;
  sid: string;
  uid: string;
  cmd: string;
  ts: number;
  meta?: UpstreamMeta;
}

/** Session lifecycle notification so services can track presence. */
export interface UpSessionEvent {
  k: 'session';
  ev: 'online' | 'offline' | 'suspended' | 'resumed';
  gate: string;
  sid: string;
  uid: string;
  ts: number;
  reason?: string;
  meta?: UpstreamMeta;
}

export type UpstreamMessage = UpRequest | UpNotify | UpSessionEvent;

// -------------------------------------------------------------- downstream --

/** Reply to an `UpRequest`. Routed to the originating session. */
export interface DownResponse extends InternalPayload {
  k: 'resp';
  sid: string;
  id: number;
  e?: ErrorCode;
  m?: string;
}

/** Unsolicited push. Target by session id (exact) or uid (whichever session is live). */
export interface DownPush extends InternalPayload {
  k: 'push';
  sid?: string;
  uid?: string;
  cmd: string;
}

/** Push to many uids at once (fan-out happens on each gate). */
export interface DownMulticast extends InternalPayload {
  k: 'multicast';
  uids: string[];
  cmd: string;
}

/** Push to every session on every gate. Published on the `all` channel. */
export interface DownBroadcast extends InternalPayload {
  k: 'broadcast';
  cmd: string;
}

/** Terminate a session. Used both by services (ban) and by gates (takeover). */
export interface DownKick {
  k: 'kick';
  uid?: string;
  sid?: string;
  reason: string;
  m?: string;
  /** Set when the kick comes from another gate claiming the same uid. */
  bySid?: string;
  byGate?: string;
}

export type DownstreamMessage = DownResponse | DownPush | DownMulticast | DownBroadcast | DownKick;

export function isDownstreamMessage(v: unknown): v is DownstreamMessage {
  if (typeof v !== 'object' || v === null) return false;
  const k = (v as { k?: unknown }).k;
  return k === 'resp' || k === 'push' || k === 'multicast' || k === 'broadcast' || k === 'kick';
}
