import { env } from '../config/env.js';

/** Transaction outcome classifications. */
export const Outcome = Object.freeze({
  SUCCESS: 'SUCCESS',
  USER_ERROR: 'USER_ERROR',
  PROVIDER_ERROR: 'PROVIDER_ERROR',
});

/** Gateway health states used for routing eligibility. */
export const HealthStatus = Object.freeze({
  HEALTHY: 'HEALTHY',
  DOWN: 'DOWN',
});

const windowKey = (gatewayId) => `asr:window:${gatewayId}`;
const countsKey = (gatewayId) => `asr:counts:${gatewayId}`;
const probeKey = (gatewayId) => `health:probe:${gatewayId}`;

/**
 * Atomically pushes an outcome into the gateway's sliding window and keeps
 * a running count (success / user_error / provider_error) in sync, evicting
 * the oldest entry once the window exceeds its configured size. Implemented
 * as a Lua script so the push + evict + count-adjust happens as one atomic
 * unit under concurrent writers.
 */
const RECORD_SCRIPT = `
local window = KEYS[1]
local counts = KEYS[2]
local outcome = ARGV[1]
local windowSize = tonumber(ARGV[2])

redis.call('LPUSH', window, outcome)
redis.call('HINCRBY', counts, outcome, 1)

local len = redis.call('LLEN', window)
if len > windowSize then
  local evicted = redis.call('RPOP', window)
  if evicted then
    redis.call('HINCRBY', counts, evicted, -1)
  end
end

return 1
`;

/**
 * Record a single transaction outcome for a gateway. This feeds directly
 * into the ASR sliding window used by getGatewayHealth().
 */
export async function recordTransaction(redis, gatewayId, outcome) {
  if (!Object.values(Outcome).includes(outcome)) {
    throw new Error(`Invalid outcome: ${outcome}`);
  }
  return redis.eval(
    RECORD_SCRIPT,
    2,
    windowKey(gatewayId),
    countsKey(gatewayId),
    outcome,
    env.asr.windowSize
  );
}

/** Raw success/user_error/provider_error counts within the current window. */
export async function getRawCounts(redis, gatewayId) {
  const values = await redis.hmget(
    countsKey(gatewayId),
    Outcome.SUCCESS,
    Outcome.USER_ERROR,
    Outcome.PROVIDER_ERROR
  );
  return {
    success: Number(values[0] || 0),
    userError: Number(values[1] || 0),
    providerError: Number(values[2] || 0),
  };
}

/**
 * ASR = Successful / (Total Attempts - User Errors)
 *     = Successful / (Successful + Provider Errors)
 * User errors are excluded entirely from the denominator so they can never
 * depress a gateway's health score.
 *
 * Bayesian smoothing / default weighting: real counts are blended with a
 * virtual pseudo-count prior (default: 48 successes / 2 failures, i.e. a
 * 96% success rate over a virtual 50-transaction seed window). This means:
 *   - a brand-new gateway with zero real transactions starts at exactly the
 *     prior (96% by default), not at "unknown"/null and not at 0%
 *   - a single early real failure barely moves the score, instead of
 *     cratering it to 0% the way a naive success/total ratio would
 *   - as real traffic accumulates, the prior's influence is diluted away
 *     and ASR converges to the gateway's true observed rate
 * The prior is deliberately kept ABOVE ASR_HEALTHY_THRESHOLD (default 92%),
 * not sitting right on it: if the prior mean equaled the threshold exactly,
 * a single real failure would always push it below (the denominator grows,
 * the numerator doesn't), so a cold-start gateway would fail eligibility on
 * its very first real hiccup.
 * This always returns a finite number — there is no "insufficient sample"
 * null case to special-case downstream.
 */
export function computeAsr(counts, prior = { success: env.asr.priorSuccess, failure: env.asr.priorFailure }) {
  const numerator = prior.success + counts.success;
  const denominator = prior.success + prior.failure + counts.success + counts.providerError;
  return numerator / denominator;
}

/** Records synthetic health-check probe results (latency + up/down). */
export async function recordProbeResult(redis, gatewayId, { up, latencyMs }) {
  await redis.hset(probeKey(gatewayId), {
    up: up ? '1' : '0',
    latencyMs: String(latencyMs ?? -1),
    lastCheckedAt: String(Date.now()),
  });
}

export async function getProbeResult(redis, gatewayId) {
  const data = await redis.hgetall(probeKey(gatewayId));
  if (!data || Object.keys(data).length === 0) {
    return { up: null, latencyMs: null, lastCheckedAt: null };
  }
  return {
    up: data.up === '1',
    latencyMs: Number(data.latencyMs),
    lastCheckedAt: Number(data.lastCheckedAt),
  };
}

/**
 * A gateway is eligible for routing when its (Bayesian-smoothed) ASR is at
 * or above the configured threshold (default 80%) AND the live synthetic
 * probe hasn't flagged it unreachable. Probe-down always wins regardless of
 * ASR — a gateway can look statistically fine on a sliding window and still
 * be unreachable right now.
 */
export function computeStatus({ asr, probe }, thresholds = env) {
  const { healthyThreshold } = thresholds.asr;
  const { latencyDownMs } = thresholds.health;

  if (probe.up === false) return HealthStatus.DOWN;
  if (probe.latencyMs !== null && probe.latencyMs >= 0 && probe.latencyMs >= latencyDownMs) {
    return HealthStatus.DOWN;
  }
  return asr >= healthyThreshold ? HealthStatus.HEALTHY : HealthStatus.DOWN;
}

/**
 * Returns the current ASR, health status, and last-probe latency for a
 * gateway. Backed entirely by O(1) Redis hash reads (HMGET/HGETALL), so
 * this comfortably meets the sub-5ms read target even under load.
 */
export async function getGatewayHealth(redis, gatewayId) {
  const [counts, probe] = await Promise.all([
    getRawCounts(redis, gatewayId),
    getProbeResult(redis, gatewayId),
  ]);
  const asr = computeAsr(counts);
  const sampleSize = counts.success + counts.providerError;
  const status = computeStatus({ asr, probe });

  return {
    gatewayId,
    asr,
    sampleSize,
    counts,
    status,
    probe,
  };
}

export async function getAllGatewaysHealth(redis, gatewayIds) {
  const results = await Promise.all(
    gatewayIds.map((id) => getGatewayHealth(redis, id))
  );
  return results;
}
