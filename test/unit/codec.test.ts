import { describe, expect, it } from 'vitest';
import { resolve } from 'node:path';
import protobuf from 'protobufjs';
import { DecodeError, jsonCodec, type Codec } from '../../src/framework/protocol/codec';
import { protobufCodec } from '../../src/framework/protocol/protobufCodec';
import { CODECS, SUBPROTOCOLS } from '../../src/framework/protocol/codecs';
import { gateDescriptor } from '../../src/framework/protocol/pb/descriptor';
import {
  CloseCode,
  ErrorCode,
  KICK_REASON,
  PacketType,
  type ClientPacket,
  type ServerPacket,
} from '../../src/framework/protocol/packet';

const codecs: Array<[string, Codec]> = [
  ['json', jsonCodec],
  ['protobuf', protobufCodec],
];

function frame(encoded: Buffer | string): Buffer {
  return typeof encoded === 'string' ? Buffer.from(encoded, 'utf8') : encoded;
}

// ---------------------------------------------------------- conformance ----

describe.each(codecs)('%s codec: client packets round-trip', (_name, codec) => {
  const cases: Array<[string, ClientPacket]> = [
    ['auth', { t: PacketType.Auth, token: 'header.body.sig', device: 'ios-17' }],
    ['auth without device', { t: PacketType.Auth, token: 'tok' }],
    ['resume', { t: PacketType.Resume, sid: 'sid-abc', rt: 'secret-xyz', ack: 42 }],
    ['resume from zero', { t: PacketType.Resume, sid: 's', rt: 'r', ack: 0 }],
    ['heartbeat with ack', { t: PacketType.Heartbeat, ack: 7 }],
    ['heartbeat without ack', { t: PacketType.Heartbeat }],
    [
      'request',
      { t: PacketType.Request, id: 3, cmd: 'game.move', d: { dx: 1, dy: -2 }, cseq: 9 },
    ],
    ['request without payload', { t: PacketType.Request, id: 1, cmd: 'game.enter' }],
    ['request without cseq', { t: PacketType.Request, id: 2, cmd: 'game.enter', d: { a: 1 } }],
    ['notify', { t: PacketType.Notify, cmd: 'chat.typing', d: { on: true }, cseq: 4 }],
    ['notify without payload', { t: PacketType.Notify, cmd: 'chat.typing' }],
  ];

  it.each(cases)('%s', (_label, packet) => {
    expect(codec.decode(frame(codec.encodeClient(packet)))).toEqual(packet);
  });
});

describe.each(codecs)('%s codec: server packets round-trip', (_name, codec) => {
  const cases: Array<[string, ServerPacket]> = [
    [
      'auth ack',
      {
        t: PacketType.AuthAck,
        uid: 'player-1',
        sid: 'sid-1',
        rt: 'resume-secret',
        ts: 1_700_000_000_123,
        rw: 60_000,
        hb: 15_000,
      },
    ],
    [
      'resume ack',
      {
        t: PacketType.ResumeAck,
        uid: 'player-1',
        sid: 'sid-1',
        ts: 1_700_000_000_123,
        replay: 3,
        cack: 12,
        seq: 20,
        rt: 'new-secret',
        rw: 60_000,
        hb: 15_000,
      },
    ],
    [
      'resume ack with resync and redirect',
      {
        t: PacketType.ResumeAck,
        uid: 'u',
        sid: 's',
        ts: 1,
        replay: 0,
        cack: 0,
        seq: 0,
        rt: 'r',
        rw: 1000,
        hb: 500,
        resync: true,
        redirect: '10.0.0.4:7000',
      },
    ],
    ['heartbeat ack', { t: PacketType.HeartbeatAck, ts: 1_700_000_000_999 }],
    ['response', { t: PacketType.Response, id: 5, seq: 8, d: { ok: true, list: [1, 2] } }],
    ['response without payload', { t: PacketType.Response, id: 5, seq: 8 }],
    [
      'error response',
      {
        t: PacketType.Response,
        id: 5,
        seq: 9,
        e: ErrorCode.ServiceTimeout,
        m: 'game did not respond',
      },
    ],
    ['push', { t: PacketType.Push, seq: 11, cmd: 'chat.message', d: { from: 'a', text: 'hi' } }],
    ['push without payload', { t: PacketType.Push, seq: 12, cmd: 'game.tick' }],
    ['kick', { t: PacketType.Kick, reason: KICK_REASON.DuplicateLogin }],
    [
      'resumable kick',
      { t: PacketType.Kick, reason: KICK_REASON.Shutdown, m: 'gate shutting down', resumable: true },
    ],
    ['error', { t: PacketType.Error, e: ErrorCode.RateLimited, m: 'too many messages' }],
    ['error tied to a request', { t: PacketType.Error, e: ErrorCode.BadRequest, m: 'bad', id: 4 }],
  ];

  it.each(cases)('%s', (_label, packet) => {
    expect(codec.decodeServer(frame(codec.encode(packet)))).toEqual(packet);
  });
});

// -------------------------------------------------------------- payloads ---

describe.each(codecs)('%s codec: payloads', (_name, codec) => {
  it('carries opaque bytes through unchanged, in both directions', () => {
    // This is what a protobuf client's own game message looks like to the
    // gate: bytes it must not interpret.
    const bytes = Buffer.from([0x08, 0x96, 0x01, 0x00, 0xff, 0x7f]);

    const up = codec.decode(
      frame(codec.encodeClient({ t: PacketType.Request, id: 1, cmd: 'game.move', d: bytes })),
    );
    expect(Buffer.isBuffer((up as { d: unknown }).d)).toBe(true);
    expect((up as { d: Buffer }).d).toEqual(bytes);

    const down = codec.decodeServer(
      frame(codec.encode({ t: PacketType.Push, seq: 1, cmd: 'game.state', d: bytes })),
    );
    expect((down as { d: Buffer }).d).toEqual(bytes);
  });

  it('keeps a JSON payload a JSON payload', () => {
    const value = { nested: { list: [1, 'two', null, true] } };
    const out = codec.decode(
      frame(codec.encodeClient({ t: PacketType.Request, id: 1, cmd: 'game.x', d: value })),
    );
    expect((out as { d: unknown }).d).toEqual(value);
    expect(Buffer.isBuffer((out as { d: unknown }).d)).toBe(false);
  });

  it('treats an empty payload as no payload', () => {
    const out = codec.decode(
      frame(
        codec.encodeClient({
          t: PacketType.Request,
          id: 1,
          cmd: 'game.x',
          d: Buffer.alloc(0),
        }),
      ),
    );
    expect((out as { d?: unknown }).d).toBeUndefined();
  });
});

// ---------------------------------------------------------- json specific --

describe('json codec', () => {
  it('is text-framed and self-describing', () => {
    expect(jsonCodec.binary).toBe(false);
    const raw = jsonCodec.encode({ t: PacketType.Push, seq: 1, cmd: 'a.b', d: { x: 1 } });
    expect(JSON.parse(raw)).toEqual({ t: PacketType.Push, seq: 1, cmd: 'a.b', d: { x: 1 } });
  });

  it.each([
    ['not json', 'nope{'],
    ['an array', '[]'],
    ['a missing type', '{"cmd":"game.move"}'],
    ['an unknown type', '{"t":99}'],
    ['a server-only type', `{"t":${PacketType.Push}}`],
    ['a request without an id', `{"t":${PacketType.Request},"cmd":"game.move"}`],
    ['a zero id', `{"t":${PacketType.Request},"id":0,"cmd":"game.move"}`],
    ['a negative id', `{"t":${PacketType.Request},"id":-1,"cmd":"game.move"}`],
    ['a fractional id', `{"t":${PacketType.Request},"id":1.5,"cmd":"game.move"}`],
    ['a cmd without a namespace', `{"t":${PacketType.Request},"id":1,"cmd":"move"}`],
    ['a cmd with bad characters', `{"t":${PacketType.Request},"id":1,"cmd":"game.mo ve"}`],
    ['an empty token', `{"t":${PacketType.Auth},"token":""}`],
    ['a resume without ack', `{"t":${PacketType.Resume},"sid":"s","rt":"r"}`],
    ['a base64 flag over a non-string payload', `{"t":${PacketType.Notify},"cmd":"a.b","b":1,"d":{}}`],
  ])('rejects %s', (_label, raw) => {
    expect(() => jsonCodec.decode(Buffer.from(raw, 'utf8'))).toThrow(DecodeError);
  });

  it('rejects an oversized token rather than forwarding it to the verifier', () => {
    const raw = JSON.stringify({ t: PacketType.Auth, token: 'a'.repeat(5000) });
    expect(() => jsonCodec.decode(Buffer.from(raw, 'utf8'))).toThrow(/exceeds/);
  });
});

// ------------------------------------------------------ protobuf specific --

describe('protobuf codec', () => {
  it('is binary-framed', () => {
    expect(protobufCodec.binary).toBe(true);
    expect(Buffer.isBuffer(protobufCodec.encode({ t: PacketType.HeartbeatAck, ts: 1 }))).toBe(true);
  });

  it('rejects bytes that are not a valid envelope', () => {
    expect(() => protobufCodec.decode(Buffer.from('definitely not protobuf!!'))).toThrow(DecodeError);
  });

  it('rejects an envelope with no packet in it', () => {
    expect(() => protobufCodec.decode(Buffer.alloc(0))).toThrow(/no known packet/);
  });

  it('validates commands and ids just like the json codec', () => {
    const bad = protobufCodec.encodeClient({ t: PacketType.Request, id: 1, cmd: 'game.move' });
    expect(protobufCodec.decode(bad)).toBeTruthy();

    // Hand-build envelopes that a hostile client could send.
    const root = protobuf.Root.fromJSON(gateDescriptor as unknown as protobuf.INamespace);
    const ClientEnvelope = root.lookupType('gate.v1.ClientEnvelope');
    const noNamespace = Buffer.from(
      ClientEnvelope.encode({ request: { id: 1, cmd: 'move' } }).finish(),
    );
    expect(() => protobufCodec.decode(noNamespace)).toThrow(/expected "service.action"/);

    const zeroId = Buffer.from(
      ClientEnvelope.encode({ request: { id: 0, cmd: 'game.move' } }).finish(),
    );
    expect(() => protobufCodec.decode(zeroId)).toThrow(/must be an integer >= 1/);

    const emptyToken = Buffer.from(ClientEnvelope.encode({ auth: { token: '' } }).finish());
    expect(() => protobufCodec.decode(emptyToken)).toThrow(/must not be empty/);
  });

  it('flags a JSON payload so the client does not feed it to its own parser', () => {
    const root = protobuf.Root.fromJSON(gateDescriptor as unknown as protobuf.INamespace);
    const ServerEnvelope = root.lookupType('gate.v1.ServerEnvelope');
    const encoded = protobufCodec.encode({
      t: PacketType.Push,
      seq: 1,
      cmd: 'chat.message',
      d: { text: 'hi' },
    });
    const raw = ServerEnvelope.decode(encoded) as unknown as {
      push: { dJson: boolean; d: Uint8Array };
    };
    expect(raw.push.dJson).toBe(true);
    expect(Buffer.from(raw.push.d).toString('utf8')).toBe('{"text":"hi"}');

    // Opaque bytes are not flagged.
    const binary = ServerEnvelope.decode(
      protobufCodec.encode({ t: PacketType.Push, seq: 1, cmd: 'x.y', d: Buffer.from([1]) }),
    ) as unknown as { push: { dJson: boolean } };
    expect(binary.push.dJson).toBe(false);
  });

  it('rejects a payload that claims to be JSON but is not', () => {
    const root = protobuf.Root.fromJSON(gateDescriptor as unknown as protobuf.INamespace);
    const ClientEnvelope = root.lookupType('gate.v1.ClientEnvelope');
    const bogus = Buffer.from(
      ClientEnvelope.encode({
        request: { id: 1, cmd: 'game.x', d: Buffer.from([0xff, 0xfe]), dJson: true },
      }).finish(),
    );
    expect(() => protobufCodec.decode(bogus)).toThrow(/not valid JSON/);
  });

  it('is smaller on the wire than json for typical packets', () => {
    const packet: ServerPacket = {
      t: PacketType.Push,
      seq: 12_345,
      cmd: 'game.state',
      d: Buffer.from([1, 2, 3, 4, 5, 6, 7, 8]),
    };
    const pb = protobufCodec.encode(packet).length;
    const json = Buffer.byteLength(jsonCodec.encode(packet));
    expect(pb).toBeLessThan(json);
  });

  it('skips unknown fields, so a newer client cannot break an older gate', () => {
    const root = protobuf.Root.fromJSON(gateDescriptor as unknown as protobuf.INamespace);
    const extended = new protobuf.Root();
    // Same message, with a field the gate has never heard of.
    extended.addJSON({
      gate: {
        nested: {
          v2: {
            nested: {
              ClientEnvelope: {
                oneofs: { body: { oneof: ['request'] } },
                fields: { request: { type: 'Request', id: 4 } },
              },
              Request: {
                fields: {
                  id: { type: 'uint32', id: 1 },
                  cmd: { type: 'string', id: 2 },
                  future: { type: 'string', id: 99 },
                },
              },
            },
          },
        },
      },
    });
    const V2 = extended.lookupType('gate.v2.ClientEnvelope');
    const wire = Buffer.from(
      V2.encode({ request: { id: 1, cmd: 'game.move', future: 'brand new' } }).finish(),
    );
    expect(protobufCodec.decode(wire)).toEqual({
      t: PacketType.Request,
      id: 1,
      cmd: 'game.move',
      d: undefined,
    });
    expect(root).toBeTruthy();
  });
});

// ------------------------------------------------------------ descriptor ---

describe('generated descriptor', () => {
  it('matches proto/gate.proto (run `npm run proto:gen` after editing it)', () => {
    const root = protobuf.loadSync(resolve(__dirname, '../../proto/gate.proto'));
    expect(root.toJSON()).toEqual(gateDescriptor);
  });
});

// ----------------------------------------------------------- negotiation ---

describe('codec registry', () => {
  it('maps every codec to a subprotocol and back', () => {
    for (const [name, codec] of Object.entries(CODECS)) {
      expect(codec.name).toBe(name);
      expect(SUBPROTOCOLS[codec.name]).toMatch(/^gate\./);
    }
  });

  it('agrees with the close-code table it ships with', () => {
    // Sanity check that the shared enums are actually shared, not copies.
    expect(CloseCode.KickedDuplicateLogin).toBe(4003);
  });
});
