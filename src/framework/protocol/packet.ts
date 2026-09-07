/**
 * Client <-> gate wire protocol.
 *
 * One JSON object per WebSocket frame. Field names are short because every
 * byte is paid for on every packet:
 *
 *   t    packet type (PacketType)
 *   id   request id, echoed back on the matching Response (client-chosen)
 *   cmd  routed command name, e.g. "game.move" / "chat.send"
 *   d    payload (opaque to the gate - it is never inspected)
 *   seq  downstream sequence number (server -> client, monotonic per session)
 *   ack  highest downstream seq the peer has received (client -> server)
 *   cseq upstream sequence number (client -> server, monotonic per session)
 *   e    error code (ErrorCode)
 *   m    human readable message (diagnostics only, never parse it)
 */
export enum PacketType {
  /** client -> gate: authenticate with a JWT. First packet on a fresh session. */
  Auth = 1,
  /** gate -> client: auth result, carries sessionId + resume token. */
  AuthAck = 2,
  /** client -> gate: resume a suspended session after a network drop. */
  Resume = 3,
  /** gate -> client: resume result; replay follows immediately. */
  ResumeAck = 4,
  /** client -> gate: keepalive. */
  Heartbeat = 5,
  /** gate -> client: keepalive reply, carries server time. */
  HeartbeatAck = 6,
  /** client -> gate -> service: request expecting exactly one Response. */
  Request = 7,
  /** service -> gate -> client: reply to a Request (same `id`). */
  Response = 8,
  /** client -> gate -> service: fire-and-forget. */
  Notify = 9,
  /** service -> gate -> client: unsolicited push. */
  Push = 10,
  /** gate -> client: session terminated (duplicate login, admin kick, shutdown). */
  Kick = 11,
  /** gate -> client: protocol/transport level error, not tied to a request. */
  Error = 12,
}

export enum ErrorCode {
  Ok = 0,
  BadRequest = 1001,
  Unauthenticated = 1002,
  AuthFailed = 1003,
  AlreadyAuthenticated = 1004,
  ResumeFailed = 1005,
  RouteNotFound = 1006,
  ServiceUnavailable = 1007,
  ServiceTimeout = 1008,
  RateLimited = 1009,
  PayloadTooLarge = 1010,
  DuplicateSeq = 1011,
  Internal = 1500,
}

/** WebSocket close codes in the private-use range (4000-4999). */
export enum CloseCode {
  Normal = 1000,
  AuthTimeout = 4001,
  AuthFailed = 4002,
  /** Session taken over by a newer login of the same account. */
  KickedDuplicateLogin = 4003,
  KickedAdmin = 4004,
  ResumeFailed = 4005,
  RateLimited = 4006,
  ProtocolError = 4007,
  ServerShutdown = 4008,
  IdleTimeout = 4009,
  ServerFull = 4010,
  /** Transport-level replacement of the socket under a live session. */
  Superseded = 4011,
}

export const KICK_REASON = {
  DuplicateLogin: 'duplicate_login',
  Admin: 'admin',
  Shutdown: 'shutdown',
  SessionExpired: 'session_expired',
} as const;

export type KickReason = (typeof KICK_REASON)[keyof typeof KICK_REASON];

export interface AuthPacket {
  t: PacketType.Auth;
  token: string;
  /** Optional client-declared device id; surfaced to services for audit. */
  device?: string;
}

export interface AuthAckPacket {
  t: PacketType.AuthAck;
  uid: string;
  sid: string;
  /** Opaque token required to resume this session. Never leaves the client. */
  rt: string;
  /** Server time (ms) so the client can compute a clock offset. */
  ts: number;
  /** Resume window in ms: how long a drop may last before the session is gone. */
  rw: number;
  /** Heartbeat interval the client should use (ms). */
  hb: number;
}

export interface ResumePacket {
  t: PacketType.Resume;
  sid: string;
  rt: string;
  /** Highest downstream seq the client has processed. */
  ack: number;
}

export interface ResumeAckPacket {
  t: PacketType.ResumeAck;
  uid: string;
  sid: string;
  ts: number;
  /** Number of packets about to be replayed. */
  replay: number;
  /** Highest upstream cseq the gate has accepted, so the client can resend the rest. */
  cack: number;
  /**
   * Highest downstream seq this gate has sent on the session. Only meaningful
   * together with `resync`: a migrated session starts numbering again from
   * zero, and the client must rebase its own counter or it would discard the
   * new packets as duplicates.
   */
  seq: number;
  /** Rotated resume token - the previous one is now invalid. */
  rt: string;
  /** Resume window in ms. */
  rw: number;
  /** Heartbeat interval the client should use (ms). */
  hb: number;
  /**
   * Set when the session was recovered on a different gate than it started
   * on: identity is intact but the replay buffer was left behind, so the
   * client must re-fetch its game state.
   */
  resync?: boolean;
  /** Address of the gate that owns the replay buffer, for clients that can reconnect directly. */
  redirect?: string;
}

export interface HeartbeatPacket {
  t: PacketType.Heartbeat;
  ack?: number;
}

export interface HeartbeatAckPacket {
  t: PacketType.HeartbeatAck;
  ts: number;
}

export interface RequestPacket {
  t: PacketType.Request;
  id: number;
  cmd: string;
  d?: unknown;
  cseq?: number;
}

export interface ResponsePacket {
  t: PacketType.Response;
  id: number;
  seq: number;
  d?: unknown;
  e?: ErrorCode;
  m?: string;
}

export interface NotifyPacket {
  t: PacketType.Notify;
  cmd: string;
  d?: unknown;
  cseq?: number;
}

export interface PushPacket {
  t: PacketType.Push;
  seq: number;
  cmd: string;
  d?: unknown;
}

export interface KickPacket {
  t: PacketType.Kick;
  reason: KickReason | string;
  m?: string;
  /**
   * True when the session survives the disconnect and the client should
   * reconnect and Resume (e.g. this gate is shutting down) rather than
   * re-authenticating from scratch.
   */
  resumable?: boolean;
}

export interface ErrorPacket {
  t: PacketType.Error;
  e: ErrorCode;
  m?: string;
  /** Set when the error can be attributed to a specific request. */
  id?: number;
}

export type ClientPacket =
  | AuthPacket
  | ResumePacket
  | HeartbeatPacket
  | RequestPacket
  | NotifyPacket;

export type ServerPacket =
  | AuthAckPacket
  | ResumeAckPacket
  | HeartbeatAckPacket
  | ResponsePacket
  | PushPacket
  | KickPacket
  | ErrorPacket;

/** Packets that carry a downstream `seq` and are therefore replayable. */
export type SequencedServerPacket = ResponsePacket | PushPacket;

export function isSequenced(p: ServerPacket): p is SequencedServerPacket {
  return p.t === PacketType.Response || p.t === PacketType.Push;
}
