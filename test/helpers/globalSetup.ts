import { redisAvailable, REDIS_URL } from './testGate';

/**
 * Integration tests need a real redis. Probe once here and let the suites
 * skip themselves rather than fail, so `npm test` works on a bare checkout.
 */
export default async function setup(): Promise<void> {
  const ok = await redisAvailable();
  process.env.GATE_TEST_REDIS = ok ? '1' : '0';
  if (!ok) {
    console.warn(
      `\n[gate-ts] redis unreachable at ${REDIS_URL} - skipping integration tests.` +
        `\n[gate-ts] start one with: docker compose up -d redis\n`,
    );
  }
}
