/**
 * Interactive-ish smoke client.
 *
 *   npm run client -- --uid player-1 [--url ws://127.0.0.1:7000/ws] [--drop]
 *   npm run client -- --codec pb          # same flows over the protobuf codec
 *
 * --drop kills the TCP connection mid-flight to exercise resume: the request
 * issued while offline must still resolve after the session comes back.
 */
import jwt from 'jsonwebtoken';
import * as dotenv from 'dotenv';
import { GateClient } from '../src/sdk/gateClient';
import { parseCodecName } from '../src/protocol/codecs';
import { logger } from '../src/util/logger';

dotenv.config();

function arg(name: string, fallback?: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : fallback;
}
const flag = (name: string): boolean => process.argv.includes(`--${name}`);

const uid = arg('uid', 'player-1') as string;
const url = arg('url', 'ws://127.0.0.1:7000/ws') as string;
const codec = parseCodecName(arg('codec', 'json') as string);
if (!codec) throw new Error('--codec must be json or protobuf');
const secret = process.env.JWT_SECRET ?? 'dev-secret-change-me';
const log = logger.child({ mod: 'client', uid });

const token = jwt.sign({ sub: uid }, secret, { algorithm: 'HS256', expiresIn: 3600 });

const client = new GateClient({ url, token, device: 'smoke-test', codec });

client.on('ready', (info) => log.info(info, 'session ready'));
client.on('push', (cmd: string, payload: unknown) => log.info({ cmd, payload }, 'push'));
client.on('kick', (kick) => log.warn(kick, 'kicked'));
client.on('reconnecting', (delay: number, attempt: number) =>
  log.warn({ delay, attempt }, 'reconnecting'),
);
client.on('close', (code: number, reason: string) => log.warn({ code, reason }, 'socket closed'));
client.on('error', (err: Error) => log.error({ err: err.message }, 'client error'));

async function main(): Promise<void> {
  await client.connect();
  log.info({ codec: client.codecName }, 'connected');

  log.info(await client.request('gate.whoami'), 'gate.whoami');
  log.info(await client.request('game.enter', {}), 'game.enter');
  log.info(await client.request('game.move', { dx: 1, dy: 2 }), 'game.move');
  log.info(await client.request('chat.send', { text: `hello from ${uid}` }), 'chat.send');

  if (flag('drop')) {
    log.warn('simulating a network drop mid-request');
    const inflight = client.request('game.move', { dx: 10, dy: 0 });
    client.simulateNetworkDrop();
    log.info(await inflight, 'request survived the drop');
    log.info(await client.request('game.move', { dx: 0, dy: 5 }), 'after resume');
  }

  if (flag('keepalive')) {
    log.info('staying connected; ctrl-c to exit');
    return;
  }

  await new Promise((r) => setTimeout(r, 500));
  client.close();
  process.exit(0);
}

process.on('SIGINT', () => {
  client.close();
  process.exit(0);
});

main().catch((err: Error) => {
  log.fatal({ err: err.message }, 'client failed');
  process.exit(1);
});
