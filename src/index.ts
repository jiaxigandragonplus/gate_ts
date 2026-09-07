import { loadConfig } from './config';
import { Gate } from './gate';
import { logger } from './util/logger';

async function main(): Promise<void> {
  const cfg = loadConfig();
  const gate = new Gate(cfg);

  let shuttingDown = false;
  const stop = (signal: string) => {
    if (shuttingDown) {
      logger.warn({ signal }, 'second signal received, exiting immediately');
      process.exit(1);
    }
    shuttingDown = true;
    const timer = setTimeout(() => {
      logger.error('graceful shutdown timed out, forcing exit');
      process.exit(1);
    }, cfg.shutdownGraceMs + 5000);
    timer.unref();

    gate
      .shutdown(signal)
      .then(() => process.exit(0))
      .catch((err: Error) => {
        logger.error({ err: err.message }, 'shutdown failed');
        process.exit(1);
      });
  };

  process.on('SIGTERM', () => stop('SIGTERM'));
  process.on('SIGINT', () => stop('SIGINT'));
  process.on('unhandledRejection', (reason) => {
    logger.error({ reason: String(reason) }, 'unhandled rejection');
  });
  process.on('uncaughtException', (err: Error) => {
    logger.fatal({ err: err.message, stack: err.stack }, 'uncaught exception');
    stop('uncaughtException');
  });

  await gate.start();
}

main().catch((err: Error) => {
  logger.fatal({ err: err.message, stack: err.stack }, 'gate failed to start');
  process.exit(1);
});
