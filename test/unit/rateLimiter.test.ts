import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { TokenBucket } from '../../src/framework/util/rateLimiter';

describe('TokenBucket', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('allows a full burst then rejects', () => {
    const bucket = new TokenBucket(5, 5);
    for (let i = 0; i < 5; i++) expect(bucket.tryConsume()).toBe(true);
    expect(bucket.tryConsume()).toBe(false);
  });

  it('refills over time at the configured rate', () => {
    const bucket = new TokenBucket(10, 10);
    for (let i = 0; i < 10; i++) bucket.tryConsume();
    expect(bucket.tryConsume()).toBe(false);

    vi.advanceTimersByTime(500);
    // 0.5s at 10/s = 5 tokens back.
    for (let i = 0; i < 5; i++) expect(bucket.tryConsume()).toBe(true);
    expect(bucket.tryConsume()).toBe(false);
  });

  it('never refills past its capacity', () => {
    const bucket = new TokenBucket(3, 100);
    bucket.tryConsume(3);
    vi.advanceTimersByTime(60_000);
    expect(bucket.tryConsume(3)).toBe(true);
    expect(bucket.tryConsume()).toBe(false);
  });
});
