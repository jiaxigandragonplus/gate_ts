/**
 * Stand-in game / chat service, and a worked example of the ServiceNode SDK.
 *
 *   npm run mock:game        # or: tsx tools/mockBackend.ts --service chat
 *
 * Registering the same service twice (different node ids) is how you verify
 * that sticky routing keeps one player on one node.
 */
import { ServiceNode, ServiceError } from '../src/sdk/serviceNode';
import { ErrorCode } from '../src/protocol/packet';
import { logger } from '../src/util/logger';

function arg(name: string, fallback?: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

const service = arg('service', 'game') as string;
const log = logger.child({ mod: 'mock', service });

const node = new ServiceNode({
  service,
  ...(arg('node') ? { nodeId: arg('node') as string } : {}),
});

/** Toy per-player state, to show that sticky routing actually sticks. */
const players = new Map<string, { pos: { x: number; y: number }; moves: number }>();

node
  .on(`${service}.echo`, (ctx) => ({ echo: ctx.payload, node: node.nodeId, uid: ctx.uid }))

  .on(`${service}.whoami`, (ctx) => ({
    uid: ctx.uid,
    sid: ctx.sid,
    gate: ctx.gate,
    servedBy: node.nodeId,
    ip: ctx.meta?.ip,
  }))

  .on('game.enter', (ctx) => {
    const state = players.get(ctx.uid) ?? { pos: { x: 0, y: 0 }, moves: 0 };
    players.set(ctx.uid, state);
    // Show off an unsolicited push right after the reply.
    setTimeout(() => {
      void node.pushToSession(ctx.gate, ctx.sid, 'game.welcome', { message: `welcome ${ctx.uid}` });
    }, 50);
    return { entered: true, state, servedBy: node.nodeId };
  })

  .on('game.move', (ctx) => {
    const state = players.get(ctx.uid);
    if (!state) throw new ServiceError('call game.enter first', ErrorCode.BadRequest);
    const { dx = 0, dy = 0 } = (ctx.payload ?? {}) as { dx?: number; dy?: number };
    state.pos.x += Number(dx) || 0;
    state.pos.y += Number(dy) || 0;
    state.moves += 1;
    return { pos: state.pos, moves: state.moves, servedBy: node.nodeId };
  })

  .on('game.slow', async (ctx) => {
    // Used to exercise the gate's upstream timeout path.
    const ms = Number((ctx.payload as { ms?: number } | undefined)?.ms ?? 15_000);
    await new Promise((r) => setTimeout(r, ms));
    return { sleptMs: ms };
  })

  .on('game.ban', async (ctx) => {
    const target = (ctx.payload as { uid?: string } | undefined)?.uid ?? ctx.uid;
    const kicked = await node.kick(target, 'admin', 'banned by game service');
    return { kicked, target };
  })

  .on('chat.send', async (ctx) => {
    const { text = '', to } = (ctx.payload ?? {}) as { text?: string; to?: string };
    if (!text.trim()) throw new ServiceError('empty message', ErrorCode.BadRequest);
    const message = { from: ctx.uid, text: text.slice(0, 500), at: Date.now() };
    if (to) {
      const delivered = await node.pushToUid(to, 'chat.message', message);
      return { delivered, private: true };
    }
    await node.broadcast('chat.message', message);
    return { delivered: true, broadcast: true };
  })

  .onSession((ctx) => {
    if (ctx.event === 'offline') players.delete(ctx.uid);
    log.info({ ev: ctx.event, uid: ctx.uid, sid: ctx.sid, gate: ctx.gate }, 'session event');
  });

async function main(): Promise<void> {
  await node.start();
  log.info({ node: node.nodeId }, 'mock backend running');
}

const stop = () => {
  void node.stop().then(() => process.exit(0));
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);

main().catch((err: Error) => {
  log.fatal({ err: err.message }, 'mock backend failed');
  process.exit(1);
});
