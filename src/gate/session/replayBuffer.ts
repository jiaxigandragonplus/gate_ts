import type { SequencedServerPacket } from '../../framework/protocol/packet';

/**
 * Fixed-size ring of the most recent downstream packets, kept so a resuming
 * client can be caught up without the services having to replay anything.
 *
 * Packets are appended with strictly increasing `seq`. `since(ack)` returns
 * null when the client is further behind than the buffer reaches - the caller
 * must then fall back to a full resync.
 */
export class ReplayBuffer {
  private readonly items: SequencedServerPacket[] = [];

  constructor(private readonly capacity: number) {
    if (capacity < 1) throw new Error('replay buffer capacity must be >= 1');
  }

  push(packet: SequencedServerPacket): void {
    this.items.push(packet);
    if (this.items.length > this.capacity) this.items.shift();
  }

  /** Packets with seq > ack, or null if some of them have been discarded. */
  since(ack: number): SequencedServerPacket[] | null {
    if (this.items.length === 0) return [];
    const oldest = (this.items[0] as SequencedServerPacket).seq;
    const newest = (this.items[this.items.length - 1] as SequencedServerPacket).seq;
    if (ack >= newest) return [];
    // We can only replay if the first packet the client is missing is still here.
    if (ack + 1 < oldest) return null;
    return this.items.filter((p) => p.seq > ack);
  }

  /** Discard packets the client has confirmed. */
  ackUpTo(seq: number): void {
    while (this.items.length > 0 && (this.items[0] as SequencedServerPacket).seq <= seq) {
      this.items.shift();
    }
  }

  clear(): void {
    this.items.length = 0;
  }

  get size(): number {
    return this.items.length;
  }

  get oldestSeq(): number | null {
    return this.items.length === 0 ? null : (this.items[0] as SequencedServerPacket).seq;
  }

  get newestSeq(): number | null {
    return this.items.length === 0
      ? null
      : (this.items[this.items.length - 1] as SequencedServerPacket).seq;
  }
}
