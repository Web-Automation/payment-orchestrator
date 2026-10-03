import Redis from 'ioredis';
import { env } from '../config/env.js';

let client;

/**
 * Lazily-created singleton Redis client. Tests inject their own client
 * (see createRedisClient) so this singleton is only used by the running app.
 */
export function getRedisClient() {
  if (!client) {
    client = new Redis(env.redis.url, {
      maxRetriesPerRequest: 2,
      lazyConnect: false,
    });
    client.on('error', (err) => {
      // eslint-disable-next-line no-console
      console.error('[redis] connection error:', err.message);
    });
  }
  return client;
}

/** Factory used by tests / anywhere that wants an isolated instance. */
export function createRedisClient(RedisImpl = Redis, url = env.redis.url) {
  return new RedisImpl(url);
}

export async function closeRedisClient() {
  if (client) {
    await client.quit();
    client = undefined;
  }
}
