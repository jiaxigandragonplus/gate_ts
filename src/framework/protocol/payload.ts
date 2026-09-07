/**
 * Game payloads are opaque to the gate. They come in two shapes:
 *
 *   - a decoded JSON value, from a client speaking the JSON codec
 *   - a `Buffer` of bytes, from a client speaking protobuf (the bytes are the
 *     client's own protobuf message, whose schema the gate does not have)
 *
 * A `Buffer` in a packet's `d` field therefore means "opaque bytes, pass
 * through untouched". The helpers here move that distinction across the
 * internal (JSON) cluster protocol, where bytes travel base64-encoded in a
 * separate `db` field so services can tell the two apart.
 */

/** True when this payload is opaque bytes rather than a JSON value. */
export function isBytes(payload: unknown): payload is Buffer {
  return Buffer.isBuffer(payload);
}

export interface InternalPayload {
  /** JSON payload, when the client sent JSON. */
  d?: unknown;
  /** Base64 of an opaque byte payload, when the client sent bytes. */
  db?: string;
}

/** Split a payload into the fields used by the internal cluster protocol. */
export function toInternal(payload: unknown): InternalPayload {
  if (payload === undefined) return {};
  if (isBytes(payload)) return { db: payload.toString('base64') };
  return { d: payload };
}

/** Rebuild a payload from an internal cluster message. */
export function fromInternal(msg: InternalPayload): unknown {
  if (msg.db !== undefined) return Buffer.from(msg.db, 'base64');
  return msg.d;
}
