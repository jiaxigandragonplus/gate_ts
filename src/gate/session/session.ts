import { ReplayBuffer } from './replayBuffer';
import type { Connection } from '../net/connection';
import {
  CloseCode,
  ErrorCode,
  PacketType,
  type ServerPacket,
  type SequencedServerPacket,
} from '../../framework/protocol/packet';
import type { UpstreamMeta } from '../../framework/protocol/internal';

export type SessionState = 'active' | 'suspended' | 'closed';

export interface SessionInit {
  uid: string;
  sid: string;
  gate: string;
  addr: string;
  device?: string;
  replayCapacity: number;
}

/**
 * A logical player session. Outlives the socket: when the connection drops
 * the session goes `suspended` and keeps buffering downstream packets until
 * either the client resumes or the resume window expires.
 */
export class Session {
  readonly uid: string;
  readonly sid: string;
  readonly gate: string;
  readonly addr: string;
  readonly device?: string;
  readonly createdAt = Date.now();

  state: SessionState = 'active';
  conn: Connection | null = null;
  suspendedAt: number | null = null;
  /** Services this session has talked to; used to clean up sticky bindings. */
  readonly touchedServices = new Set<string>();

  private outSeq = 0;
  private lastCSeq = 0;
  private readonly buffer: ReplayBuffer;

  constructor(init: SessionInit) {
    this.uid = init.uid;
    this.sid = init.sid;
    this.gate = init.gate;
    this.addr = init.addr;
    if (init.device !== undefined) this.device = init.device;
    this.buffer = new ReplayBuffer(init.replayCapacity);
  }

  get online(): boolean {
    return this.state === 'active' && this.conn !== null && !this.conn.closed;
  }

  get lastSeq(): number {
    return this.outSeq;
  }

  get lastAcceptedCSeq(): number {
    return this.lastCSeq;
  }

  get pendingReplay(): number {
    return this.buffer.size;
  }

  meta(): UpstreamMeta {
    return {
      ...(this.conn ? { ip: this.conn.ip } : {}),
      ...(this.device === undefined ? {} : { device: this.device }),
      connectedAt: this.createdAt,
    };
  }

  // ------------------------------------------------------------ transport --

  attach(conn: Connection): Connection | null {
    const previous = this.conn;
    this.conn = conn;
    conn.session = this;
    conn.state = 'bound';
    this.state = 'active';
    this.suspendedAt = null;
    return previous;
  }

  /** Called when the socket dies. The session itself stays alive. */
  detach(): void {
    if (this.state === 'closed') return;
    this.conn = null;
    this.state = 'suspended';
    this.suspendedAt = Date.now();
  }

  markClosed(): void {
    this.state = 'closed';
    this.conn = null;
    this.buffer.clear();
  }

  // ----------------------------------------------------------- downstream --

  /** Unsequenced control packet: only meaningful to the socket that is live now. */
  sendControl(packet: ServerPacket): boolean {
    return this.conn?.send(packet) ?? false;
  }

  /**
   * Assign the next sequence number, buffer for replay, and deliver if a
   * socket is attached. Buffering happens even while suspended so pushes that
   * arrive during a network blip survive the reconnect.
   */
  private emit(packet: SequencedServerPacket): void {
    packet.seq = ++this.outSeq;
    this.buffer.push(packet);
    if (this.online) this.conn?.send(packet);
  }

  push(cmd: string, payload?: unknown): void {
    this.emit({ t: PacketType.Push, seq: 0, cmd, ...(payload === undefined ? {} : { d: payload }) });
  }

  respond(id: number, payload?: unknown, error?: ErrorCode, message?: string): void {
    this.emit({
      t: PacketType.Response,
      id,
      seq: 0,
      ...(payload === undefined ? {} : { d: payload }),
      ...(error === undefined ? {} : { e: error }),
      ...(message === undefined ? {} : { m: message }),
    });
  }

  /**
   * How many buffered packets the client is missing, or null when the gap is
   * bigger than the buffer and a full resync is needed.
   */
  missingSince(ack: number): number | null {
    return this.buffer.since(ack)?.length ?? null;
  }

  /** Re-send everything after `ack`. Returns the count, or null if impossible. */
  replayFrom(ack: number): number | null {
    const missing = this.buffer.since(ack);
    if (missing === null) return null;
    for (const packet of missing) this.conn?.send(packet);
    return missing.length;
  }

  ackUpTo(seq: number): void {
    if (seq > 0 && seq <= this.outSeq) this.buffer.ackUpTo(seq);
  }

  // ------------------------------------------------------------- upstream --

  /**
   * Idempotency gate for client packets. A client that resends after a
   * reconnect reuses its `cseq`, and we must not forward it twice.
   * Packets without a `cseq` opt out of dedup and are always accepted.
   */
  acceptUpstream(cseq: number | undefined): boolean {
    if (cseq === undefined) return true;
    if (cseq <= this.lastCSeq) return false;
    this.lastCSeq = cseq;
    return true;
  }

  closeSocket(code: CloseCode, reason: string): void {
    this.conn?.close(code, reason);
  }
}
