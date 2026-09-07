import { describe, expect, it } from 'vitest';
import {
  negotiateCodec,
  parseCodecList,
  parseCodecName,
  SUBPROTOCOLS,
} from '../../src/protocol/codecs';

const both = ['json', 'protobuf'] as const;

describe('parseCodecName', () => {
  it('accepts canonical names and the obvious aliases', () => {
    expect(parseCodecName('json')).toBe('json');
    expect(parseCodecName('JSON')).toBe('json');
    expect(parseCodecName('protobuf')).toBe('protobuf');
    expect(parseCodecName(' pb ')).toBe('protobuf');
    expect(parseCodecName('proto')).toBe('protobuf');
  });

  it('rejects anything else', () => {
    expect(parseCodecName('msgpack')).toBeUndefined();
    expect(parseCodecName('')).toBeUndefined();
  });
});

describe('parseCodecList', () => {
  it('parses and de-duplicates', () => {
    expect(parseCodecList('json,protobuf')).toEqual(['json', 'protobuf']);
    expect(parseCodecList('pb, json , pb')).toEqual(['protobuf', 'json']);
    expect(parseCodecList('')).toEqual([]);
  });

  it('fails loudly on a typo instead of silently dropping it', () => {
    expect(() => parseCodecList('json,protobuff')).toThrow(/unknown codec/);
  });
});

describe('negotiateCodec', () => {
  it('honours an offered subprotocol', () => {
    const r = negotiateCodec({
      offered: [SUBPROTOCOLS.protobuf],
      allowed: both,
      fallback: 'json',
    });
    expect(r).toMatchObject({ ok: true, subprotocol: SUBPROTOCOLS.protobuf });
    expect(r.ok && r.codec.name).toBe('protobuf');
  });

  it('takes the client preference order', () => {
    const r = negotiateCodec({
      offered: [SUBPROTOCOLS.json, SUBPROTOCOLS.protobuf],
      allowed: both,
      fallback: 'protobuf',
    });
    expect(r.ok && r.codec.name).toBe('json');
  });

  it('skips an offered codec that is disabled', () => {
    const r = negotiateCodec({
      offered: [SUBPROTOCOLS.protobuf, SUBPROTOCOLS.json],
      allowed: ['json'],
      fallback: 'json',
    });
    expect(r).toMatchObject({ ok: true, subprotocol: SUBPROTOCOLS.json });
  });

  it('falls back to the default when nothing offered is recognised', () => {
    const r = negotiateCodec({
      offered: ['soap', 'graphql-ws'],
      allowed: both,
      fallback: 'protobuf',
    });
    // No subprotocol is selected, which makes an RFC 6455 client that
    // *required* one fail the handshake rather than silently mis-parse.
    expect(r).toEqual({ ok: true, codec: expect.objectContaining({ name: 'protobuf' }) });
    expect(r.ok && r.subprotocol).toBeUndefined();
  });

  it('honours the query parameter when no subprotocol is offered', () => {
    const r = negotiateCodec({ offered: [], query: 'pb', allowed: both, fallback: 'json' });
    expect(r.ok && r.codec.name).toBe('protobuf');
  });

  it('prefers a subprotocol over the query parameter', () => {
    const r = negotiateCodec({
      offered: [SUBPROTOCOLS.json],
      query: 'protobuf',
      allowed: both,
      fallback: 'protobuf',
    });
    expect(r.ok && r.codec.name).toBe('json');
  });

  it('refuses an explicit request for a disabled codec instead of downgrading', () => {
    // Silently speaking JSON to a client that asked for protobuf is a far
    // worse outcome than a failed handshake.
    expect(negotiateCodec({ offered: [], query: 'pb', allowed: ['json'], fallback: 'json' })).toEqual(
      { ok: false, reason: 'codec "protobuf" is not enabled' },
    );
  });

  it('refuses an unknown explicit codec', () => {
    expect(
      negotiateCodec({ offered: [], query: 'msgpack', allowed: both, fallback: 'json' }),
    ).toMatchObject({ ok: false });
  });

  it('uses the default when the client says nothing', () => {
    expect(negotiateCodec({ offered: [], allowed: both, fallback: 'json' }).ok).toBe(true);
    expect(
      negotiateCodec({ offered: [], query: null, allowed: both, fallback: 'protobuf' }),
    ).toMatchObject({ ok: true, codec: expect.objectContaining({ name: 'protobuf' }) });
  });
});
