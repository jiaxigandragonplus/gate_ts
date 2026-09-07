import { jsonCodec, type Codec, type CodecName } from './codec';
import { protobufCodec } from './protobufCodec';

export const CODECS: Readonly<Record<CodecName, Codec>> = {
  json: jsonCodec,
  protobuf: protobufCodec,
};

/**
 * WebSocket subprotocol per codec. This is the primary way a client picks a
 * wire format: `new WebSocket(url, 'gate.pb.v1')`. The server echoes the one
 * it selected, so a mismatch is impossible.
 */
export const SUBPROTOCOLS: Readonly<Record<CodecName, string>> = {
  json: 'gate.json.v1',
  protobuf: 'gate.pb.v1',
};

const BY_SUBPROTOCOL = new Map<string, CodecName>(
  Object.entries(SUBPROTOCOLS).map(([name, sub]) => [sub, name as CodecName]),
);

/** Accepts the canonical names plus the obvious aliases. */
export function parseCodecName(value: string): CodecName | undefined {
  switch (value.trim().toLowerCase()) {
    case 'json':
      return 'json';
    case 'protobuf':
    case 'proto':
    case 'pb':
      return 'protobuf';
    default:
      return undefined;
  }
}

export function parseCodecList(value: string): CodecName[] {
  const names: CodecName[] = [];
  for (const part of value.split(',')) {
    if (part.trim() === '') continue;
    const name = parseCodecName(part);
    if (!name) throw new Error(`unknown codec "${part.trim()}" (expected json or protobuf)`);
    if (!names.includes(name)) names.push(name);
  }
  return names;
}

export interface NegotiationRequest {
  /** Values from the `Sec-WebSocket-Protocol` header, in client preference order. */
  offered: string[];
  /** Value of the `codec` query parameter, if any. */
  query?: string | null | undefined;
  allowed: readonly CodecName[];
  fallback: CodecName;
}

export type NegotiationResult =
  | { ok: true; codec: Codec; subprotocol?: string }
  | { ok: false; reason: string };

/**
 * Resolve the wire format for one connection.
 *
 * Order: an offered subprotocol we support, then `?codec=`, then the
 * configured default. An *explicit* request for a codec the gate does not
 * serve is refused rather than silently downgraded - a protobuf client that
 * quietly receives JSON is a much worse failure than a rejected handshake.
 */
export function negotiateCodec(req: NegotiationRequest): NegotiationResult {
  for (const offer of req.offered) {
    const name = BY_SUBPROTOCOL.get(offer.trim());
    if (name && req.allowed.includes(name)) {
      return { ok: true, codec: CODECS[name], subprotocol: SUBPROTOCOLS[name] };
    }
  }

  if (req.query) {
    const name = parseCodecName(req.query);
    if (!name) return { ok: false, reason: `unknown codec "${req.query}"` };
    if (!req.allowed.includes(name)) return { ok: false, reason: `codec "${name}" is not enabled` };
    return { ok: true, codec: CODECS[name], subprotocol: SUBPROTOCOLS[name] };
  }

  // A client that offered only subprotocols we do not know still gets served
  // with the default codec; the header is advisory.
  return { ok: true, codec: CODECS[req.fallback] };
}
