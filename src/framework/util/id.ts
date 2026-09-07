import { randomBytes } from 'node:crypto';

/** Short, URL-safe, collision-resistant id. */
export function shortId(bytes = 9): string {
  return randomBytes(bytes).toString('base64url');
}

/** Monotonic-ish counter for per-process connection ids. */
let connCounter = 0;
export function nextConnId(): number {
  connCounter = (connCounter + 1) >>> 0;
  return connCounter;
}
