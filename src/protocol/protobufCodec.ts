import protobuf from 'protobufjs';
import { gateDescriptor } from './pb/descriptor';
import {
  DecodeError,
  checkCmd,
  checkRequestId,
  checkSid,
  checkToken,
  type Codec,
} from './codec';
import { PacketType, type ClientPacket, type ServerPacket } from './packet';

const root = protobuf.Root.fromJSON(gateDescriptor as unknown as protobuf.INamespace);
const ClientEnvelope = root.lookupType('gate.v1.ClientEnvelope');
const ServerEnvelope = root.lookupType('gate.v1.ServerEnvelope');

/**
 * protobufjs hands 64-bit fields back as `Long`. Sequence numbers are
 * logically far below 2^53, so they are narrowed to plain numbers here and a
 * value that could not have come from a well-behaved client is rejected.
 */
function num(value: unknown, field: string): number {
  if (value === undefined || value === null) return 0;
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new DecodeError(`"${field}" must be a non-negative integer`);
    }
    return value;
  }
  if (typeof value === 'object' && typeof (value as { toNumber?: unknown }).toNumber === 'function') {
    const n = (value as { toNumber(): number }).toNumber();
    if (!Number.isSafeInteger(n) || n < 0) {
      throw new DecodeError(`"${field}" is out of range`);
    }
    return n;
  }
  throw new DecodeError(`"${field}" must be a number`);
}

function str(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

/**
 * A zero-length `d` is indistinguishable from an absent one in proto3, so an
 * empty payload is reported as "no payload".
 */
function decodePayload(raw: unknown, isJson: boolean): unknown {
  if (raw === undefined || raw === null) return undefined;
  // An unset bytes field decodes to protobufjs' shared empty array rather
  // than a Buffer, so all three shapes have to be accepted here.
  const bytes = Buffer.isBuffer(raw)
    ? raw
    : raw instanceof Uint8Array
      ? Buffer.from(raw)
      : Array.isArray(raw)
        ? Buffer.from(raw as number[])
        : undefined;
  if (bytes === undefined) throw new DecodeError('"d" must be bytes');
  if (bytes.length === 0) return undefined;
  if (!isJson) return bytes;
  try {
    return JSON.parse(bytes.toString('utf8'));
  } catch {
    throw new DecodeError('d_json is set but the payload is not valid JSON');
  }
}

function encodePayload(payload: unknown): { d?: Buffer; dJson?: boolean } {
  if (payload === undefined) return {};
  if (Buffer.isBuffer(payload)) return { d: payload };
  // A JSON payload reaching a protobuf client (JSON-payload service, protobuf
  // envelope) travels as UTF-8 JSON, flagged so the client knows not to feed
  // it to its own protobuf parser.
  return { d: Buffer.from(JSON.stringify(payload), 'utf8'), dJson: true };
}

interface DecodedEnvelope {
  body?: string;
  auth?: { token?: unknown; device?: unknown };
  resume?: { sid?: unknown; rt?: unknown; ack?: unknown };
  heartbeat?: { ack?: unknown };
  request?: { id?: unknown; cmd?: unknown; d?: unknown; cseq?: unknown; dJson?: unknown };
  notify?: { cmd?: unknown; d?: unknown; cseq?: unknown; dJson?: unknown };
}

/**
 * Protobuf envelope codec.
 *
 * Only the envelope is typed: `d` stays `bytes`, so the gate remains unable
 * (and unwilling) to parse game payloads. Roughly half the bytes of the JSON
 * codec on small packets, and no number/string parsing on the hot path.
 */
export class ProtobufCodec implements Codec {
  readonly name = 'protobuf' as const;
  readonly binary = true;

  decode(frame: Buffer): ClientPacket {
    let env: DecodedEnvelope;
    try {
      env = ClientEnvelope.decode(frame) as unknown as DecodedEnvelope;
    } catch (err) {
      throw new DecodeError(`malformed protobuf: ${(err as Error).message}`);
    }

    switch (env.body) {
      case 'auth': {
        const device = str(env.auth?.device);
        return {
          t: PacketType.Auth,
          token: checkToken(str(env.auth?.token)),
          ...(device ? { device: device.slice(0, 128) } : {}),
        };
      }

      case 'resume':
        return {
          t: PacketType.Resume,
          sid: checkSid(str(env.resume?.sid)),
          rt: checkToken(str(env.resume?.rt)),
          ack: num(env.resume?.ack, 'ack'),
        };

      case 'heartbeat': {
        const ack = num(env.heartbeat?.ack, 'ack');
        return { t: PacketType.Heartbeat, ...(ack > 0 ? { ack } : {}) };
      }

      case 'request': {
        const cseq = num(env.request?.cseq, 'cseq');
        return {
          t: PacketType.Request,
          id: checkRequestId(num(env.request?.id, 'id')),
          cmd: checkCmd(str(env.request?.cmd)),
          d: decodePayload(env.request?.d, env.request?.dJson === true),
          ...(cseq > 0 ? { cseq } : {}),
        };
      }

      case 'notify': {
        const cseq = num(env.notify?.cseq, 'cseq');
        return {
          t: PacketType.Notify,
          cmd: checkCmd(str(env.notify?.cmd)),
          d: decodePayload(env.notify?.d, env.notify?.dJson === true),
          ...(cseq > 0 ? { cseq } : {}),
        };
      }

      default:
        throw new DecodeError('envelope carries no known packet');
    }
  }

  encode(packet: ServerPacket): Buffer {
    return Buffer.from(ServerEnvelope.encode(this.toEnvelope(packet)).finish());
  }

  encodeClient(packet: ClientPacket): Buffer {
    return Buffer.from(ClientEnvelope.encode(toClientEnvelope(packet)).finish());
  }

  private toEnvelope(packet: ServerPacket): Record<string, unknown> {
    switch (packet.t) {
      case PacketType.AuthAck:
        return {
          authAck: {
            uid: packet.uid,
            sid: packet.sid,
            rt: packet.rt,
            ts: packet.ts,
            rw: packet.rw,
            hb: packet.hb,
          },
        };

      case PacketType.ResumeAck:
        return {
          resumeAck: {
            uid: packet.uid,
            sid: packet.sid,
            ts: packet.ts,
            replay: packet.replay,
            cack: packet.cack,
            seq: packet.seq,
            rt: packet.rt,
            rw: packet.rw,
            hb: packet.hb,
            ...(packet.resync === true ? { resync: true } : {}),
            ...(packet.redirect === undefined ? {} : { redirect: packet.redirect }),
          },
        };

      case PacketType.HeartbeatAck:
        return { heartbeatAck: { ts: packet.ts } };

      case PacketType.Response:
        return {
          response: {
            id: packet.id,
            seq: packet.seq,
            ...encodePayload(packet.d),
            ...(packet.e === undefined ? {} : { e: packet.e }),
            ...(packet.m === undefined ? {} : { m: packet.m }),
          },
        };

      case PacketType.Push:
        return {
          push: { seq: packet.seq, cmd: packet.cmd, ...encodePayload(packet.d) },
        };

      case PacketType.Kick:
        return {
          kick: {
            reason: packet.reason,
            ...(packet.m === undefined ? {} : { m: packet.m }),
            ...(packet.resumable === true ? { resumable: true } : {}),
          },
        };

      case PacketType.Error:
        return {
          error: {
            e: packet.e,
            ...(packet.m === undefined ? {} : { m: packet.m }),
            ...(packet.id === undefined ? {} : { id: packet.id }),
          },
        };
    }
  }

  /** Decode a server frame. Used by clients (and by the conformance tests). */
  decodeServer(frame: Buffer): ServerPacket {
    let env: Record<string, unknown> & { body?: string };
    try {
      env = ServerEnvelope.decode(frame) as unknown as Record<string, unknown> & { body?: string };
    } catch (err) {
      throw new DecodeError(`malformed protobuf: ${(err as Error).message}`);
    }
    const body = env.body;
    if (!body) throw new DecodeError('envelope carries no known packet');
    const m = env[body] as Record<string, unknown>;

    switch (body) {
      case 'authAck':
        return {
          t: PacketType.AuthAck,
          uid: str(m['uid']),
          sid: str(m['sid']),
          rt: str(m['rt']),
          ts: num(m['ts'], 'ts'),
          rw: num(m['rw'], 'rw'),
          hb: num(m['hb'], 'hb'),
        };

      case 'resumeAck': {
        const redirect = str(m['redirect']);
        return {
          t: PacketType.ResumeAck,
          uid: str(m['uid']),
          sid: str(m['sid']),
          ts: num(m['ts'], 'ts'),
          replay: num(m['replay'], 'replay'),
          cack: num(m['cack'], 'cack'),
          seq: num(m['seq'], 'seq'),
          rt: str(m['rt']),
          rw: num(m['rw'], 'rw'),
          hb: num(m['hb'], 'hb'),
          ...(m['resync'] === true ? { resync: true } : {}),
          ...(redirect ? { redirect } : {}),
        };
      }

      case 'heartbeatAck':
        return { t: PacketType.HeartbeatAck, ts: num(m['ts'], 'ts') };

      case 'response': {
        const e = num(m['e'], 'e');
        const message = str(m['m']);
        const d = decodePayload(m['d'], m['dJson'] === true);
        return {
          t: PacketType.Response,
          id: num(m['id'], 'id'),
          seq: num(m['seq'], 'seq'),
          ...(d === undefined ? {} : { d }),
          ...(e === 0 ? {} : { e }),
          ...(message ? { m: message } : {}),
        };
      }

      case 'push': {
        const d = decodePayload(m['d'], m['dJson'] === true);
        return {
          t: PacketType.Push,
          seq: num(m['seq'], 'seq'),
          cmd: str(m['cmd']),
          ...(d === undefined ? {} : { d }),
        };
      }

      case 'kick': {
        const message = str(m['m']);
        return {
          t: PacketType.Kick,
          reason: str(m['reason']),
          ...(message ? { m: message } : {}),
          ...(m['resumable'] === true ? { resumable: true } : {}),
        };
      }

      case 'error': {
        const message = str(m['m']);
        const id = num(m['id'], 'id');
        return {
          t: PacketType.Error,
          e: num(m['e'], 'e'),
          ...(message ? { m: message } : {}),
          ...(id > 0 ? { id } : {}),
        };
      }

      default:
        throw new DecodeError(`unexpected server packet "${body}"`);
    }
  }
}

export const protobufCodec = new ProtobufCodec();

function toClientEnvelope(packet: ClientPacket): Record<string, unknown> {
  switch (packet.t) {
    case PacketType.Auth:
      return {
        auth: {
          token: packet.token,
          ...(packet.device === undefined ? {} : { device: packet.device }),
        },
      };
    case PacketType.Resume:
      return { resume: { sid: packet.sid, rt: packet.rt, ack: packet.ack } };
    case PacketType.Heartbeat:
      return { heartbeat: { ...(packet.ack === undefined ? {} : { ack: packet.ack }) } };
    case PacketType.Request:
      return {
        request: {
          id: packet.id,
          cmd: packet.cmd,
          ...encodePayload(packet.d),
          ...(packet.cseq === undefined ? {} : { cseq: packet.cseq }),
        },
      };
    case PacketType.Notify:
      return {
        notify: {
          cmd: packet.cmd,
          ...encodePayload(packet.d),
          ...(packet.cseq === undefined ? {} : { cseq: packet.cseq }),
        },
      };
  }
}
