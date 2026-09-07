import { natsAvailable, redisAvailable, NATS_URL, REDIS_URL } from './testGate';

/**
 * Integration tests need a real redis (cluster state) and, for the NATS
 * transport, a real nats-server. Probe both once here and let the suites skip
 * what is unavailable, so `npm test` works on a bare checkout.
 */
export default async function setup(): Promise<void> {
  const [redis, nats] = await Promise.all([redisAvailable(), natsAvailable()]);
  process.env.GATE_TEST_REDIS = redis ? '1' : '0';
  process.env.GATE_TEST_NATS = nats ? '1' : '0';

  if (!redis) {
    console.warn(
      `\n[gate-ts] redis unreachable at ${REDIS_URL} - skipping integration tests.` +
        `\n[gate-ts] start one with: docker compose up -d redis\n`,
    );
  }
  if (!nats) {
    console.warn(
      `\n[gate-ts] nats unreachable at ${NATS_URL} - skipping the NATS transport suite.` +
        `\n[gate-ts] start one with: docker compose up -d nats\n`,
    );
  }
}
