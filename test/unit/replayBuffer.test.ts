import { describe, expect, it } from 'vitest';
import { ReplayBuffer } from '../../src/gate/session/replayBuffer';
import { PacketType, type SequencedServerPacket } from '../../src/framework/protocol/packet';

const push = (seq: number): SequencedServerPacket => ({
  t: PacketType.Push,
  seq,
  cmd: 'game.tick',
});

describe('ReplayBuffer', () => {
  it('returns everything after the ack', () => {
    const buf = new ReplayBuffer(10);
    for (let i = 1; i <= 5; i++) buf.push(push(i));
    expect(buf.since(3)?.map((p) => p.seq)).toEqual([4, 5]);
  });

  it('returns nothing when the client is already current', () => {
    const buf = new ReplayBuffer(10);
    buf.push(push(1));
    expect(buf.since(1)).toEqual([]);
    expect(buf.since(9)).toEqual([]);
  });

  it('returns an empty replay for a fresh session', () => {
    expect(new ReplayBuffer(4).since(0)).toEqual([]);
  });

  it('signals an unrecoverable gap once packets have been evicted', () => {
    const buf = new ReplayBuffer(3);
    for (let i = 1; i <= 6; i++) buf.push(push(i));
    // Holds 4,5,6 - a client that only saw 2 has lost packet 3 for good.
    expect(buf.since(2)).toBeNull();
    expect(buf.since(3)?.map((p) => p.seq)).toEqual([4, 5, 6]);
  });

  it('drops acknowledged packets to bound memory', () => {
    const buf = new ReplayBuffer(10);
    for (let i = 1; i <= 5; i++) buf.push(push(i));
    buf.ackUpTo(3);
    expect(buf.size).toBe(2);
    expect(buf.oldestSeq).toBe(4);
    expect(buf.since(3)?.map((p) => p.seq)).toEqual([4, 5]);
  });

  it('never grows past its capacity', () => {
    const buf = new ReplayBuffer(4);
    for (let i = 1; i <= 100; i++) buf.push(push(i));
    expect(buf.size).toBe(4);
    expect(buf.newestSeq).toBe(100);
  });
});
