import { GameNode } from './gameNode';
import { loadGameConfig } from './config';
import { BagSystem } from './systems/bag';
import { ProfileSystem } from './systems/profile';
import { QuestSystem } from './systems/quest';
import { logger } from '../framework/util/logger';

/**
 * Game node entry point. Registering systems is the only thing this file
 * does - which is the point of the split.
 */
async function main(): Promise<void> {
  const cfg = loadGameConfig();
  const node = new GameNode(cfg).use(new ProfileSystem(), new BagSystem(), new QuestSystem());

  let stopping = false;
  const stop = (signal: string): void => {
    if (stopping) {
      logger.warn({ signal }, 'second signal received, exiting immediately');
      process.exit(1);
    }
    stopping = true;
    const timer = setTimeout(() => {
      logger.error('graceful shutdown timed out, forcing exit');
      process.exit(1);
    }, cfg.shutdownGraceMs + 5000);
    timer.unref();

    node
      .shutdown(signal)
      .then(() => process.exit(0))
      .catch((err: Error) => {
        logger.error({ err: err.message }, 'shutdown failed');
        process.exit(1);
      });
  };

  process.on('SIGTERM', () => stop('SIGTERM'));
  process.on('SIGINT', () => stop('SIGINT'));
  process.on('unhandledRejection', (reason) =>
    logger.error({ reason: String(reason) }, 'unhandled rejection'),
  );

  await node.start();
}

main().catch((err: Error) => {
  logger.fatal({ err: err.message, stack: err.stack }, 'game node failed to start');
  process.exit(1);
});
