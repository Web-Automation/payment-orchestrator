import 'dotenv/config';

/**
 * Central, typed access to all environment configuration.
 * No hardcoded values live anywhere else in the codebase — everything
 * that could plausibly change between environments is read here.
 */
function num(val, fallback) {
  if (val === undefined) return fallback;
  const n = Number(val);
  return Number.isNaN(n) ? fallback : n;
}

function list(val, fallback) {
  if (!val) return fallback;
  return val.split(',').map((s) => s.trim()).filter(Boolean);
}

export const env = {
  port: num(process.env.PORT, 3000),
  logLevel: process.env.LOG_LEVEL || 'info',

  redis: {
    url: process.env.REDIS_URL || 'redis://localhost:6379',
  },

  events: {
    // 'sqs' or 'mock'. Falls back to 'mock' automatically if no queue URL is set.
    driver: process.env.EVENTS_DRIVER || (process.env.SQS_QUEUE_URL ? 'sqs' : 'mock'),
    sqsQueueUrl: process.env.SQS_QUEUE_URL || '',
    sqsEndpoint: process.env.SQS_ENDPOINT || undefined, // for localstack
    region: process.env.AWS_REGION || 'us-east-1',
  },

  asr: {
    windowSize: num(process.env.ASR_WINDOW_SIZE, 1000),
    // Single eligibility cutoff: a gateway routes only when ASR >= this.
    healthyThreshold: num(process.env.ASR_HEALTHY_THRESHOLD, 0.92),
    // Bayesian smoothing / default weighting for cold-start gateways: a new
    // gateway is seeded with a virtual pseudo-count prior so one early
    // failure can't crater its score to 0%, but it isn't blindly trusted
    // either — the prior gets diluted as real traffic accumulates.
    // Default: 48 success / 2 failure (96% over a virtual 50-transaction
    // window) — deliberately set ABOVE the 92% eligibility threshold (with
    // headroom, not sitting right on it) so a cold-start gateway is
    // eligible from its very first request, and can absorb a couple of
    // early real failures before dropping below threshold. If the prior
    // mean sat exactly at the threshold, a single real failure would always
    // push it below (denominator grows, numerator doesn't), so raising the
    // eligibility bar means raising the prior too, not just the threshold.
    priorSuccess: num(process.env.ASR_PRIOR_SUCCESS, 48),
    priorFailure: num(process.env.ASR_PRIOR_FAILURE, 2),
  },

  health: {
    // Synthetic probes populate the sliding window independently of live
    // traffic — default matches the spec's "every 60 seconds".
    probeIntervalMs: num(process.env.HEALTH_PROBE_INTERVAL_MS, 60000),
    probeTimeoutMs: num(process.env.HEALTH_PROBE_TIMEOUT_MS, 3000),
    latencyDownMs: num(process.env.HEALTH_LATENCY_DOWN_MS, 3000),
  },

  routing: {
    // Optional capacity-aware overflow: per-gateway max TPS before the
    // orchestrator starts overflowing cheaper-but-capacity-capped traffic to
    // the next candidate, even though it's still healthy and cheapest.
    // Keyed by gatewayId; 0/undefined = uncapped.
    maxTpsByGateway: (() => {
      const raw = process.env.ROUTING_MAX_TPS_BY_GATEWAY; // e.g. "gatewayA:180,gatewayB:0"
      if (!raw) return {};
      return Object.fromEntries(
        raw.split(',').map((pair) => {
          const [id, tps] = pair.split(':');
          return [id.trim(), Number(tps)];
        })
      );
    })(),
  },

  failover: {
    maxCascades: num(process.env.FAILOVER_MAX_CASCADES, 2),
    // Timeout for the initiate() call to a gateway before we treat it as a
    // provider-side drop and enter pre-failover verification.
    attemptTimeoutMs: num(process.env.FAILOVER_ATTEMPT_TIMEOUT_MS, 5000),
    // Out-of-band status-query timeout during pre-failover verification.
    statusCheckTimeoutMs: num(process.env.FAILOVER_STATUS_CHECK_TIMEOUT_MS, 3000),
    // Distributed lock TTL for the pre-failover verification / reconciliation
    // critical section on a given order.
    lockTtlMs: num(process.env.FAILOVER_LOCK_TTL_MS, 10000),
  },

  gateways: list(process.env.GATEWAY_IDS, ['gatewayA', 'gatewayB', 'gatewayC']),
};

export default env;
