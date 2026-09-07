/**
 * Token-bucket rate limiter. One instance per connection (cheap: 3 numbers).
 */
export class TokenBucket {
  private tokens: number;
  private last: number;

  constructor(
    private readonly capacity: number,
    private readonly refillPerSec: number,
  ) {
    this.tokens = capacity;
    this.last = Date.now();
  }

  /** Consume `n` tokens. Returns false when the bucket is empty (caller should reject). */
  tryConsume(n = 1): boolean {
    const ts = Date.now();
    const delta = (ts - this.last) / 1000;
    if (delta > 0) {
      this.tokens = Math.min(this.capacity, this.tokens + delta * this.refillPerSec);
      this.last = ts;
    }
    if (this.tokens < n) return false;
    this.tokens -= n;
    return true;
  }

  get available(): number {
    return this.tokens;
  }
}
