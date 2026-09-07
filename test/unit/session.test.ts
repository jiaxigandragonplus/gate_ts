import { describe, expect, it } from 'vitest';
import { Session } from '../../src/gate/session/session';
import { PacketType, type ServerPacket } from '../../src/framework/protocol/packet';
import type { Connection } from '../../src/gate/net/connection';

function makeSession(replayCapacity = 8) {
  const session = new Session({
    uid: 'u1',
    sid: 's1',
    gate: 'gate-1',
    addr: '127.0.0.1:7000',
    replayCapacity,
  });
  const sent: ServerPacket[] = [];
  const conn = {
    ip: '1.2.3.4',
    closed: false,
    session: null,
    state: 'unauthenticated',
    send: (p: ServerPacket) => {
      sent.push(p);
      return true;
    },
  } as unknown as Connection;
  return { session, conn, sent };
}

describe('Session sequencing', () => {
  it('numbers downstream packets monotonically', () => {
    const { session, conn, sent } = makeSession();
    session.attach(conn);
    session.push('a');
    session.respond(1, { ok: true });
    session.push('b');
    expect(sent.map((p) => (p as { seq: number }).seq)).toEqual([1, 2, 3]);
    expect(session.lastSeq).toBe(3);
  });

  it('buffers pushes that arrive while the socket is gone, then replays them', () => {
    const { session, conn, sent } = makeSession();
    session.attach(conn);
    session.push('before');
    expect(sent).toHaveLength(1);

    session.detach();
    session.push('during-1');
    session.push('during-2');
    expect(sent).toHaveLength(1);
    expect(session.state).toBe('suspended');

    const { conn: conn2, sent: sent2 } = makeSession();
    session.attach(conn2);
    expect(session.replayFrom(1)).toBe(2);
    expect(sent2.map((p) => (p as { cmd: string }).cmd)).toEqual(['during-1', 'during-2']);
  });

  it('reports an unrecoverable gap when the buffer has overflowed', () => {
    const { session, conn } = makeSession(2);
    session.attach(conn);
    for (let i = 0; i < 5; i++) session.push('tick');
    expect(session.missingSince(1)).toBeNull();
    expect(session.replayFrom(1)).toBeNull();
    expect(session.missingSince(4)).toBe(1);
  });
});

describe('Session upstream dedup', () => {
  it('accepts strictly increasing cseq and rejects replays', () => {
    const { session } = makeSession();
    expect(session.acceptUpstream(1)).toBe(true);
    expect(session.acceptUpstream(2)).toBe(true);
    expect(session.acceptUpstream(2)).toBe(false);
    expect(session.acceptUpstream(1)).toBe(false);
    expect(session.acceptUpstream(3)).toBe(true);
    expect(session.lastAcceptedCSeq).toBe(3);
  });

  it('always accepts packets that opt out of dedup', () => {
    const { session } = makeSession();
    expect(session.acceptUpstream(undefined)).toBe(true);
    expect(session.acceptUpstream(undefined)).toBe(true);
    expect(session.lastAcceptedCSeq).toBe(0);
  });
});

describe('Session lifecycle', () => {
  it('is only online while an open socket is attached', () => {
    const { session, conn } = makeSession();
    expect(session.online).toBe(false);
    session.attach(conn);
    expect(session.online).toBe(true);
    expect(conn.session).toBe(session);
    session.detach();
    expect(session.online).toBe(false);
    expect(session.suspendedAt).not.toBeNull();
  });

  it('drops its buffer once closed', () => {
    const { session, conn } = makeSession();
    session.attach(conn);
    session.push('x');
    session.markClosed();
    expect(session.state).toBe('closed');
    expect(session.pendingReplay).toBe(0);
    expect(session.sendControl({ t: PacketType.HeartbeatAck, ts: 1 })).toBe(false);
  });
});
