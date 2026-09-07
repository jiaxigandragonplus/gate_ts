/**
 * Load generator, for sizing a gate and for proving that N gates behind a
 * balancer actually share the traffic.
 *
 *   npm run load -- --clients 500 --rate 5 --seconds 30 \
 *                   --url ws://127.0.0.1:7000/ws --cmd game.echo
 *   npm run load -- --codec pb --admin http://127.0.0.1:8000
 *
 * With --admin the gate's own byte counters are sampled before and after, so
 * two runs can be compared directly (e.g. json vs protobuf).
 *
 * Reports connect time and request latency percentiles, plus reconnects, so
 * a run that silently degraded into reconnect churn is obvious.
 */
import jwt from 'jsonwebtoken';
import * as dotenv from 'dotenv';
import { GateClient } from '../src/sdk/gateClient';
import { parseCodecName } from '../src/protocol/codecs';

dotenv.config();

function arg(name: string, fallback: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? (process.argv[i + 1] as string) : fallback;
}

const clientCount = Number.parseInt(arg('clients', '100'), 10);
const ratePerClient = Number.parseFloat(arg('rate', '2'));
const seconds = Number.parseInt(arg('seconds', '20'), 10);
const url = arg('url', 'ws://127.0.0.1:7000/ws');
const cmd = arg('cmd', 'game.echo');
const uidPrefix = arg('prefix', 'load');
const rampMs = Number.parseInt(arg('ramp', '2000'), 10);
const secret = process.env.JWT_SECRET ?? 'dev-secret-change-me';
const codec = parseCodecName(arg('codec', 'json'));
if (!codec) throw new Error('--codec must be json or protobuf');
const adminUrl = arg('admin', '');

const latencies: number[] = [];
const connectMs: number[] = [];
let sent = 0;
let ok = 0;
let failed = 0;
let reconnects = 0;
let connectFailures = 0;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx] as number;
}

async function runClient(index: number, endAt: number): Promise<void> {
  const uid = `${uidPrefix}-${index}`;
  const token = jwt.sign({ sub: uid }, secret, { algorithm: 'HS256', expiresIn: 3600 });
  const client = new GateClient({ url, token, device: 'loadtest', requestTimeoutMs: 15_000, codec });
  client.on('reconnecting', () => {
    reconnects += 1;
  });
  client.on('error', () => undefined);

  const t0 = Date.now();
  try {
    await client.connect();
    connectMs.push(Date.now() - t0);
  } catch {
    connectFailures += 1;
    return;
  }

  const interval = 1000 / ratePerClient;
  while (Date.now() < endAt) {
    const started = Date.now();
    sent += 1;
    try {
      await client.request(cmd, { i: sent, at: started });
      ok += 1;
      latencies.push(Date.now() - started);
    } catch {
      failed += 1;
    }
    const wait = interval - (Date.now() - started);
    if (wait > 0) await sleep(wait);
  }
  client.close();
}

/** Reads the gate's counters, when --admin points at one. */
async function counters(): Promise<Record<string, number> | null> {
  if (!adminUrl) return null;
  try {
    const res = (await (await fetch(`${adminUrl}/stats`)).json()) as {
      counters: Record<string, number>;
    };
    return res.counters;
  } catch (err) {
    console.warn('[load] could not read gate stats:', (err as Error).message);
    return null;
  }
}

async function main(): Promise<void> {
  console.log(
    `[load] ${clientCount} clients, ${ratePerClient} req/s each ` +
      `(~${Math.round(clientCount * ratePerClient)} req/s total) for ${seconds}s ` +
      `-> ${url} [${codec}]`,
  );
  const before = await counters();
  const endAt = Date.now() + rampMs + seconds * 1000;
  const startedAt = Date.now();

  const runners: Array<Promise<void>> = [];
  for (let i = 0; i < clientCount; i++) {
    // Ramp connections so the gate is not hit by a thundering herd.
    runners.push(sleep((rampMs / clientCount) * i).then(() => runClient(i, endAt)));
  }
  await Promise.all(runners);

  const elapsedSec = (Date.now() - startedAt) / 1000;
  const after = await counters();
  const sortedLat = [...latencies].sort((a, b) => a - b);
  const sortedConn = [...connectMs].sort((a, b) => a - b);

  const delta = (key: string): number => (after?.[key] ?? 0) - (before?.[key] ?? 0);
  const bytes =
    before && after
      ? {
          gateBytesIn: delta('bytes_in_total'),
          gateBytesOut: delta('bytes_out_total'),
          bytesPerRequest: Math.round(
            (delta('bytes_in_total') + delta('bytes_out_total')) / Math.max(1, ok),
          ),
        }
      : {};

  console.log('\n[load] results');
  console.table({
    codec,
    clients: clientCount,
    connected: connectMs.length,
    connectFailures,
    reconnects,
    requestsSent: sent,
    ok,
    failed,
    throughputPerSec: Math.round(ok / elapsedSec),
    connectP50ms: percentile(sortedConn, 50),
    connectP99ms: percentile(sortedConn, 99),
    latencyP50ms: percentile(sortedLat, 50),
    latencyP95ms: percentile(sortedLat, 95),
    latencyP99ms: percentile(sortedLat, 99),
    latencyMaxms: sortedLat[sortedLat.length - 1] ?? 0,
    ...bytes,
  });
  process.exit(failed > 0 || connectFailures > 0 ? 1 : 0);
}

main().catch((err: Error) => {
  console.error('[load] failed:', err.message);
  process.exit(1);
});
