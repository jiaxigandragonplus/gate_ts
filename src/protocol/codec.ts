import { PacketType, type ClientPacket, type ServerPacket } from './packet';

export class DecodeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DecodeError';
  }
}

/** Wire format for one connection. Negotiated at handshake time. */
export type CodecName = 'json' | 'protobuf';

/**
 * A codec owns the envelope only: it turns frames into `ClientPacket`s and
 * `ServerPacket`s into frames. Game payloads pass through untouched, so
 * adding a codec never requires knowing anything about game messages.
 */
export interface Codec {
  readonly name: CodecName;
  /** True when frames should be sent as binary rather than text. */
  readonly binary: boolean;

  // Server side.
  decode(frame: Buffer): ClientPacket;
  encode(packet: ServerPacket): Buffer | string;

  // Client side - used by the client SDK, and by the conformance tests to
  // round-trip both directions through every codec.
  encodeClient(packet: ClientPacket): Buffer | string;
  decodeServer(frame: Buffer): ServerPacket;
}

// ------------------------------------------------------ shared validation --

export const CMD_RE = /^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)+$/;
export const MAX_CMD_LEN = 64;
export const MAX_TOKEN_LEN = 4096;
export const MAX_SID_LEN = 64;

/** Commands must be `service.action`, so the router can resolve them. */
export function checkCmd(cmd: string): string {
  if (cmd.length === 0) throw new DecodeError('"cmd" must not be empty');
  if (cmd.length > MAX_CMD_LEN) throw new DecodeError(`"cmd" exceeds ${MAX_CMD_LEN} chars`);
  if (!CMD_RE.test(cmd)) throw new DecodeError(`invalid cmd "${cmd}" (expected "service.action")`);
  return cmd;
}

export function checkToken(token: string): string {
  if (token.length === 0) throw new DecodeError('"token" must not be empty');
  if (token.length > MAX_TOKEN_LEN) throw new DecodeError(`"token" exceeds ${MAX_TOKEN_LEN} chars`);
  return token;
}

export function checkSid(sid: string): string {
  if (sid.length === 0) throw new DecodeError('"sid" must not be empty');
  if (sid.length > MAX_SID_LEN) throw new DecodeError(`"sid" exceeds ${MAX_SID_LEN} chars`);
  return sid;
}

/** Request ids start at 1; 0 is reserved for "no request". */
export function checkRequestId(id: number): number {
  if (!Number.isSafeInteger(id) || id < 1) {
    throw new DecodeError('"id" must be an integer >= 1');
  }
  return id;
}

// ---------------------------------------------------------------- json ----

function asObject(raw: string): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new DecodeError('malformed JSON');
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new DecodeError('packet must be a JSON object');
  }
  return parsed as Record<string, unknown>;
}

function reqStr(o: Record<string, unknown>, key: string): string {
  const v = o[key];
  if (typeof v !== 'string') throw new DecodeError(`"${key}" must be a string`);
  return v;
}

function reqUint(o: Record<string, unknown>, key: string): number {
  const v = o[key];
  if (typeof v !== 'number' || !Number.isSafeInteger(v) || v < 0) {
    throw new DecodeError(`"${key}" must be a non-negative integer`);
  }
  return v;
}

function optUint(o: Record<string, unknown>, key: string): number | undefined {
  if (o[key] === undefined || o[key] === null) return undefined;
  return reqUint(o, key);
}

/**
 * Reads the payload. `b: 1` marks `d` as base64 of an opaque byte payload,
 * which is how bytes reach a JSON client (and how a protobuf-payload client
 * can speak the JSON envelope).
 *
 * A zero-length byte payload reads back as "no payload", matching proto3
 * where the two are indistinguishable - services must behave the same
 * whichever codec the client picked.
 */
function payload(o: Record<string, unknown>): unknown {
  if (o['b'] === 1 || o['b'] === true) {
    const d = o['d'];
    if (typeof d !== 'string') throw new DecodeError('"d" must be a base64 string when b is set');
    const bytes = Buffer.from(d, 'base64');
    return bytes.length === 0 ? undefined : bytes;
  }
  return o['d'];
}

/**
 * JSON envelope, one object per frame. The default codec: easy to debug, easy
 * to speak from any client, and fast enough for most traffic.
 */
export class JsonCodec implements Codec {
  readonly name = 'json' as const;
  readonly binary = false;

  decode(frame: Buffer): ClientPacket {
    const o = asObject(frame.toString('utf8'));
    const t = o['t'];
    if (typeof t !== 'number') throw new DecodeError('"t" (packet type) is required');

    switch (t) {
      case PacketType.Auth: {
        const device = o['device'];
        return {
          t: PacketType.Auth,
          token: checkToken(reqStr(o, 'token')),
          ...(typeof device === 'string' ? { device: device.slice(0, 128) } : {}),
        };
      }

      case PacketType.Resume:
        return {
          t: PacketType.Resume,
          sid: checkSid(reqStr(o, 'sid')),
          rt: checkToken(reqStr(o, 'rt')),
          ack: reqUint(o, 'ack'),
        };

      case PacketType.Heartbeat: {
        const ack = optUint(o, 'ack');
        return { t: PacketType.Heartbeat, ...(ack === undefined ? {} : { ack }) };
      }

      case PacketType.Request: {
        const cseq = optUint(o, 'cseq');
        return {
          t: PacketType.Request,
          id: checkRequestId(reqUint(o, 'id')),
          cmd: checkCmd(reqStr(o, 'cmd')),
          d: payload(o),
          ...(cseq === undefined ? {} : { cseq }),
        };
      }

      case PacketType.Notify: {
        const cseq = optUint(o, 'cseq');
        return {
          t: PacketType.Notify,
          cmd: checkCmd(reqStr(o, 'cmd')),
          d: payload(o),
          ...(cseq === undefined ? {} : { cseq }),
        };
      }

      default:
        throw new DecodeError(`unexpected packet type ${t} from client`);
    }
  }

  encode(packet: ServerPacket): string {
    return stringify(packet);
  }

  encodeClient(packet: ClientPacket): string {
    return stringify(packet);
  }

  decodeServer(frame: Buffer): ServerPacket {
    const o = asObject(frame.toString('utf8'));
    if (typeof o['t'] !== 'number') throw new DecodeError('"t" (packet type) is required');
    if (o['b'] === 1 || o['b'] === true) {
      const d = payload(o);
      const { b: _flag, ...rest } = o;
      return { ...rest, d } as unknown as ServerPacket;
    }
    return o as unknown as ServerPacket;
  }
}

/** Buffers cannot go into JSON as-is: mark them and base64 the bytes. */
function stringify(packet: ClientPacket | ServerPacket): string {
  if ('d' in packet && Buffer.isBuffer(packet.d)) {
    return JSON.stringify({ ...packet, d: packet.d.toString('base64'), b: 1 });
  }
  return JSON.stringify(packet);
}

export const jsonCodec = new JsonCodec();
