/**
 * Process entrypoint: wires every module together and starts listening.
 * Real Redis client, real event publisher (SQS or mock, auto-selected),
 * real/mock gateway adapters (auto-selected per-gateway via env — see
 * adapters/index.js), and the background health-probe loop all get
 * constructed here and threaded through as `deps` into buildApp(). Nothing
 * else in the codebase reaches for a global — everything is passed in,
 * which is what keeps buildApp.js usable standalone in tests.
 */
import pino from 'pino';
import { env } from '../config/env.js';
import { getRedisClient, closeRedisClient } from '../health/redisClient.js';
import { createEventPublisher } from '../events/eventPublisher.js';
import { buildAdapters } from '../adapters/index.js';
import { startHealthProbeLoop } from '../health/healthProbe.js';
import { buildApp } from './buildApp.js';

const logger = pino(
  env.logLevel === 'debug'
    ? { level: env.logLevel, transport: { target: 'pino-pretty' } }
    : { level: env.logLevel } // structured JSON logs in non-debug envs
);

async function main() {
  const redis = getRedisClient();
  const publisher = createEventPublisher(logger);
  const adapters = buildAdapters();

  const deps = { redis, adapters, publisher, logger };

  const app = buildApp(deps); // logger: false — we use our own pino instance for domain events

  const stopProbeLoop = startHealthProbeLoop(redis, Object.values(adapters), logger);

  app.addHook('onClose', async () => {
    stopProbeLoop();
    await closeRedisClient();
  });

  try {
    await app.listen({ port: env.port, host: '0.0.0.0' });
    logger.info({ port: env.port, eventsDriver: env.events.driver, gateways: env.gateways }, 'payment-orchestrator listening');
  } catch (err) {
    logger.error({ err: err.message }, 'failed to start server');
    process.exit(1);
  }

  const shutdown = async (signal) => {
    logger.info({ signal }, 'shutting down');
    await app.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main();
