import { env } from '../config/env.js';

/**
 * Per-gateway, per-second request counters in Redis, used to enforce a safe
 * TPS buffer on a gateway even when it's the cheapest and healthiest option
 * — e.g. capping a 0%-MDR primary gateway at a safe throughput ceiling and
 * overflowing the excess to a (paid) secondary rather than letting the
 * primary collapse under load it can't sustain.
 */

const secondBucket = () => Math.floor(Date.now() / 1000);
const tpsKey = (gatewayId, bucket) => `tps:${gatewayId}:${bucket}`;

/** Call once per actual attempt dispatched to a gateway (not per routing decision). */
export async function incrementTps(redis, gatewayId) {
  const key = tpsKey(gatewayId, secondBucket());
  const count = await redis.incr(key);
  if (count === 1) {
    await redis.expire(key, 2); // self-cleans; only the current second's bucket matters
  }
  return count;
}

export async function getCurrentTps(redis, gatewayId) {
  const val = await redis.get(tpsKey(gatewayId, secondBucket()));
  return Number(val || 0);
}

export function getMaxTps(gatewayId, config = env.routing.maxTpsByGateway) {
  return config[gatewayId] ?? 0; // 0 = uncapped
}

export function isAtCapacity(currentTps, maxTps) {
  if (!maxTps || maxTps <= 0) return false;
  return currentTps >= maxTps;
}
