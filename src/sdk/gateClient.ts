import { EventEmitter } from 'node:events';
import WebSocket from 'ws';
import {
  CloseCode,
  ErrorCode,
  PacketType,
  type ClientPacket,
  type ServerPacket,
  type KickPacket,
} from '../protocol/packet';
import type { Codec, CodecName } from '../protocol/codec';
import { CODECS, SUBPROTOCOLS } from '../protocol/codecs';

export interface GateClientOptions {
  url: string;
  /** JWT obtained from the login/auth service. */
  token: string;
  device?: string;
  /** Reconnect automatically after an unexpected drop. Default true. */
  autoReconnect?: boolean;
  /** Backoff bounds for reconnect attempts (ms). */
  minBackoffMs?: number;
  maxBackoffMs?: number;
  /** Default request timeout (ms). */
  requestTimeoutMs?: number;
  /**
   * Wire format. 'json' is the default; 'protobuf' halves envelope size and
   * lets game payloads stay as raw bytes. Requested via the WebSocket
   * subprotocol, so the gate confirms it or the handshake fails.
   */
  codec?: CodecName;
}

function toBuffer(data: WebSocket.RawData): Buffer {
  if (Buffer.isBuffer(data)) return data;
  if (Array.isArray(data)) return Buffer.concat(data);
  return Buffer.from(data as ArrayBuffer);
}

export class RequestError extends Error {
  constructor(
    message: string,
    readonly code: number,
  ) {
    super(message);
    this.name = 'RequestError';
  }
}

interface Pending {
  id: number;
  cmd: string;
  payload: unknown;
  cseq: number;
  resolve: (value: unknown) => void;
  reject: (err: Error) => void;
  timer: NodeJS.Timeout;
}

type ClientState = 'idle' | 'connecting' | 'authenticating' | 'ready' | 'closed';

/**
 * Reference client. Implements the half of the reconnect protocol that lives
 * on the client: it keeps the session id and resume token, tracks the highest
 * downstream `seq` it has seen, and after a drop resumes and re-sends only
 * the requests the gate never accepted.
 *
 * Written against `ws` for node; the only browser-specific change is swapping
 * the WebSocket constructor.
 *
 * Events: `ready` ({resumed, resync}), `push` (cmd, payload), `kick`
 * (KickPacket), `close` (code, reason), `error` (Error), `reconnecting` (delayMs).
 */
export class GateClient extends EventEmitter {
  private ws: WebSocket | null = null;
  private state: ClientState = 'idle';

  private sid: string | null = null;
  private resumeToken: string | null = null;
  private uid: string | null = null;

  /** Highest downstream seq processed; sent as `ack` on resume/heartbeat. */
  private lastRecvSeq = 0;
  /** Monotonic upstream counter used for server-side dedup. */
  private cseq = 0;
  private nextRequestId = 1;

  private readonly pending = new Map<number, Pending>();
  private readonly opts: Required<Omit<GateClientOptions, 'device'>> & { device?: string };
  private readonly codec: Codec;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private attempt = 0;
  private intentionalClose = false;

  constructor(options: GateClientOptions) {
    super();
    this.opts = {
      url: options.url,
      token: options.token,
      autoReconnect: options.autoReconnect ?? true,
      minBackoffMs: options.minBackoffMs ?? 300,
      maxBackoffMs: options.maxBackoffMs ?? 10_000,
      requestTimeoutMs: options.requestTimeoutMs ?? 10_000,
      codec: options.codec ?? 'json',
      ...(options.device === undefined ? {} : { device: options.device }),
    };
    this.codec = CODECS[this.opts.codec];
  }

  get currentUid(): string | null {
    return this.uid;
  }

  /**
   * Point the client at a different gate. The session id and resume token are
   * kept, so the next connect resumes rather than re-authenticates - which is
   * how a client follows a `redirect` or an updated load-balancer address.
   */
  setUrl(url: string): void {
    this.opts.url = url;
  }

  get sessionId(): string | null {
    return this.sid;
  }

  get isReady(): boolean {
    return this.state === 'ready';
  }

  /** Wire format in use ('json' or 'protobuf'). */
  get codecName(): CodecName {
    return this.codec.name;
  }

  /** Connect (or reconnect) and resolve once the session is usable. */
  connect(): Promise<void> {
    if (this.state === 'ready') return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const onReady = () => {
        this.off('error', onError);
        resolve();
      };
      const onError = (err: Error) => {
        this.off('ready', onReady);
        reject(err);
      };
      this.once('ready', onReady);
      this.once('error', onError);
      this.open();
    });
  }

  private open(): void {
    if (this.state === 'closed') return;
    this.intentionalClose = false;
    this.state = 'connecting';
    // The subprotocol is how the codec is agreed on; a gate that does not
    // serve it fails the handshake instead of silently speaking JSON.
    const ws = new WebSocket(this.opts.url, [SUBPROTOCOLS[this.opts.codec]], {
      handshakeTimeout: 10_000,
    });
    this.ws = ws;

    ws.on('open', () => {
      this.state = 'authenticating';
      // A live session id means this is a reconnect: resume instead of
      // re-authenticating, so buffered pushes are replayed.
      if (this.sid && this.resumeToken) {
        this.sendRaw({
          t: PacketType.Resume,
          sid: this.sid,
          rt: this.resumeToken,
          ack: this.lastRecvSeq,
        });
      } else {
        this.sendRaw({
          t: PacketType.Auth,
          token: this.opts.token,
          ...(this.opts.device === undefined ? {} : { device: this.opts.device }),
        });
      }
    });

    ws.on('message', (data: WebSocket.RawData) => {
      let packet: ServerPacket;
      try {
        packet = this.codec.decodeServer(toBuffer(data));
      } catch (err) {
        this.emit('error', new Error(`malformed packet from gate: ${(err as Error).message}`));
        return;
      }
      this.handle(packet);
    });

    ws.on('error', (err: Error) => {
      this.emit('error', err);
    });

    ws.on('close', (code: number, reason: Buffer) => {
      this.stopHeartbeat();
      this.ws = null;
      const wasReady = this.state === 'ready';
      if (this.state !== 'closed') this.state = 'idle';
      this.emit('close', code, reason.toString('utf8'));

      if (this.intentionalClose || this.state === 'closed') return;

      // Auth-level failures are terminal: retrying with the same token or a
      // dead session would just loop.
      if (
        code === CloseCode.AuthFailed ||
        code === CloseCode.KickedDuplicateLogin ||
        code === CloseCode.KickedAdmin
      ) {
        this.failAllPending(new Error(`connection refused (${code})`));
        return;
      }
      if (code === CloseCode.ResumeFailed) {
        // Session is unrecoverable; start over with the token.
        this.sid = null;
        this.resumeToken = null;
        this.lastRecvSeq = 0;
        this.cseq = 0;
      }
      if (this.opts.autoReconnect) this.scheduleReconnect(wasReady);
    });
  }

  private handle(packet: ServerPacket): void {
    switch (packet.t) {
      case PacketType.AuthAck: {
        this.uid = packet.uid;
        this.sid = packet.sid;
        this.resumeToken = packet.rt;
        this.lastRecvSeq = 0;
        this.cseq = 0;
        this.attempt = 0;
        this.state = 'ready';
        this.startHeartbeat(packet.hb);
        this.emit('ready', { resumed: false, resync: false, uid: packet.uid, sid: packet.sid });
        return;
      }

      case PacketType.ResumeAck: {
        this.uid = packet.uid;
        this.sid = packet.sid;
        this.resumeToken = packet.rt;
        this.attempt = 0;
        this.state = 'ready';
        this.startHeartbeat(packet.hb);
        // A resynced session (recovered on a different gate) numbers its
        // downstream packets from scratch, so rebase or we would drop them
        // all as duplicates.
        if (packet.resync === true) this.lastRecvSeq = packet.seq ?? 0;
        // Anything the gate never accepted must be sent again; anything it did
        // accept will arrive (or has arrived) as a replayed Response.
        this.resendAfter(packet.cack);
        this.emit('ready', {
          resumed: true,
          resync: packet.resync === true,
          replayed: packet.replay,
          uid: packet.uid,
          sid: packet.sid,
        });
        return;
      }

      case PacketType.HeartbeatAck:
        this.emit('heartbeat', packet.ts);
        return;

      case PacketType.Response: {
        if (!this.acceptSeq(packet.seq)) return;
        const pending = this.pending.get(packet.id);
        if (!pending) return;
        clearTimeout(pending.timer);
        this.pending.delete(packet.id);
        if (packet.e !== undefined && packet.e !== ErrorCode.Ok) {
          pending.reject(new RequestError(packet.m ?? `request failed (${packet.e})`, packet.e));
        } else {
          pending.resolve(packet.d);
        }
        return;
      }

      case PacketType.Push: {
        if (!this.acceptSeq(packet.seq)) return;
        this.emit('push', packet.cmd, packet.d);
        return;
      }

      case PacketType.Kick: {
        const kick = packet as KickPacket;
        if (!kick.resumable) {
          // The session is gone for good (e.g. logged in elsewhere).
          this.sid = null;
          this.resumeToken = null;
          this.opts.autoReconnect = false;
        }
        this.emit('kick', kick);
        return;
      }

      case PacketType.Error: {
        const err = new RequestError(packet.m ?? `gate error ${packet.e}`, packet.e);
        if (packet.id !== undefined) {
          const pending = this.pending.get(packet.id);
          if (pending) {
            clearTimeout(pending.timer);
            this.pending.delete(packet.id);
            pending.reject(err);
            return;
          }
        }
        this.emit('error', err);
        return;
      }
    }
  }

  /** Drop duplicates that arrive when a replay overlaps what we already had. */
  private acceptSeq(seq: number): boolean {
    if (seq <= this.lastRecvSeq) return false;
    this.lastRecvSeq = seq;
    return true;
  }

  // ------------------------------------------------------------- sending ---

  request<T = unknown>(cmd: string, payload?: unknown, timeoutMs?: number): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const id = this.nextRequestId++;
      const cseq = ++this.cseq;
      // Not unref'd: a pending request must keep the process alive, including
      // while the client is reconnecting.
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new RequestError(`request "${cmd}" timed out`, ErrorCode.ServiceTimeout));
      }, timeoutMs ?? this.opts.requestTimeoutMs);

      this.pending.set(id, {
        id,
        cmd,
        payload,
        cseq,
        resolve: resolve as (v: unknown) => void,
        reject,
        timer,
      });

      // While disconnected the request stays pending: it is re-sent as soon
      // as the session resumes.
      if (this.state === 'ready') {
        this.sendRaw({ t: PacketType.Request, id, cmd, d: payload, cseq });
      }
    });
  }

  notify(cmd: string, payload?: unknown): void {
    const cseq = ++this.cseq;
    this.sendRaw({ t: PacketType.Notify, cmd, d: payload, cseq });
  }

  /** Re-send requests the gate had not accepted before the drop. */
  private resendAfter(cack: number): void {
    for (const pending of [...this.pending.values()].sort((a, b) => a.cseq - b.cseq)) {
      if (pending.cseq > cack) {
        this.sendRaw({
          t: PacketType.Request,
          id: pending.id,
          cmd: pending.cmd,
          d: pending.payload,
          cseq: pending.cseq,
        });
      }
    }
  }

  private sendRaw(packet: ClientPacket): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(this.codec.encodeClient(packet));
    }
  }

  private startHeartbeat(intervalMs: number): void {
    this.stopHeartbeat();
    const period = Math.max(1000, Math.floor(intervalMs));
    this.heartbeatTimer = setInterval(() => {
      this.sendRaw({ t: PacketType.Heartbeat, ack: this.lastRecvSeq });
    }, period);
    this.heartbeatTimer.unref();
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private scheduleReconnect(immediate: boolean): void {
    if (this.reconnectTimer) return;
    const base = Math.min(this.opts.maxBackoffMs, this.opts.minBackoffMs * 2 ** this.attempt);
    // Full jitter, so a gate restart does not bring every client back at once.
    const delay = immediate && this.attempt === 0 ? 0 : Math.floor(Math.random() * base);
    this.attempt += 1;
    this.emit('reconnecting', delay, this.attempt);
    // Not unref'd: while we are waiting to reconnect there may be no other
    // handle keeping the event loop alive.
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.open();
    }, delay);
  }

  private failAllPending(err: Error): void {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(err);
    }
    this.pending.clear();
  }

  /** Close without reconnecting and abandon the session. */
  close(): void {
    this.intentionalClose = true;
    this.state = 'closed';
    this.stopHeartbeat();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.failAllPending(new Error('client closed'));
    this.ws?.close(CloseCode.Normal, 'client closed');
    this.ws = null;
  }

  /** Kill the socket without a close handshake - used to simulate a drop. */
  simulateNetworkDrop(): void {
    this.ws?.terminate();
  }
}
