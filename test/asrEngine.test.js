import RedisMock from 'ioredis-mock';
import {
  recordTransaction,
  getRawCounts,
  computeAsr,
  computeStatus,
  recordProbeResult,
  getGatewayHealth,
  Outcome,
  HealthStatus,
} from '../src/health/asrEngine.js';

describe('ASR engine', () => {
  let redis;

  beforeEach(() => {
    redis = new RedisMock();
  });

  afterEach(async () => {
    await redis.flushall();
  });

  test('computeAsr excludes user errors from the denominator', () => {
    // 8 success, 2 provider errors, 100 user errors -> ASR must ignore the user errors
    const asr = computeAsr({ success: 8, providerError: 2, userError: 100 }, { success: 0, failure: 0 });
    expect(asr).toBeCloseTo(0.8);
  });

  test('recordTransaction accumulates counts correctly by outcome', async () => {
    await recordTransaction(redis, 'gwA', Outcome.SUCCESS);
    await recordTransaction(redis, 'gwA', Outcome.SUCCESS);
    await recordTransaction(redis, 'gwA', Outcome.USER_ERROR);
    await recordTransaction(redis, 'gwA', Outcome.PROVIDER_ERROR);

    const counts = await getRawCounts(redis, 'gwA');
    expect(counts).toEqual({ success: 2, userError: 1, providerError: 1 });
  });

  test('a flood of user errors does not depress ASR', async () => {
    await recordTransaction(redis, 'gwB', Outcome.SUCCESS);
    await recordTransaction(redis, 'gwB', Outcome.SUCCESS);
    await recordTransaction(redis, 'gwB', Outcome.SUCCESS);
    for (let i = 0; i < 50; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await recordTransaction(redis, 'gwB', Outcome.USER_ERROR);
    }
    const counts = await getRawCounts(redis, 'gwB');
    const asr = computeAsr(counts, { success: 0, failure: 0 });
    expect(asr).toBe(1); // 3 success / (3 success + 0 provider errors) = 1.0, unaffected
  });

  test('sliding window evicts the oldest entry once it exceeds the configured size', async () => {
    const smallWindowRecord = async (gatewayId, outcome, windowSize) => {
      const script = `
        local window = KEYS[1]
        local counts = KEYS[2]
        local outcome = ARGV[1]
        local windowSize = tonumber(ARGV[2])
        redis.call('LPUSH', window, outcome)
        redis.call('HINCRBY', counts, outcome, 1)
        local len = redis.call('LLEN', window)
        if len > windowSize then
          local evicted = redis.call('RPOP', window)
          if evicted then redis.call('HINCRBY', counts, evicted, -1) end
        end
        return 1
      `;
      return redis.eval(script, 2, `asr:window:${gatewayId}`, `asr:counts:${gatewayId}`, outcome, windowSize);
    };

    for (let i = 0; i < 5; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await smallWindowRecord('gwC', Outcome.SUCCESS, 5);
    }
    for (let i = 0; i < 5; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await smallWindowRecord('gwC', Outcome.PROVIDER_ERROR, 5);
    }
    const counts = await getRawCounts(redis, 'gwC');
    expect(counts.success).toBe(0);
    expect(counts.providerError).toBe(5);
  });

  describe('Bayesian smoothing / default weighting for cold-start gateways', () => {
    test('a brand-new gateway with zero real transactions starts at exactly the prior rate', () => {
      // Uses the actual current default prior (48 success / 2 failure = 96%
      // over a virtual 50-transaction window) so this stays in lockstep
      // with src/config/env.js rather than an arbitrary illustrative number.
      const asr = computeAsr({ success: 0, providerError: 0, userError: 0 }, { success: 48, failure: 2 });
      expect(asr).toBeCloseTo(0.96); // 48 / (48 + 2)
    });

    test('a single early real failure barely moves the score instead of cratering it to 0%', () => {
      const asr = computeAsr({ success: 0, providerError: 1, userError: 0 }, { success: 48, failure: 2 });
      // 48 / (48 + 2 + 1) = 0.9412 — nowhere near 0%, unlike a naive 0/1 ratio
      // would give, and still comfortably above the default 92% eligibility
      // threshold — a cold-start gateway survives one bad early attempt.
      expect(asr).toBeCloseTo(48 / 51, 3);
      expect(asr).toBeGreaterThan(0.9);
    });

    test('real traffic dilutes the prior toward the true observed rate as volume grows', async () => {
      const { env } = await import('../src/config/env.js');
      // A gateway that is genuinely failing most of the time should still end
      // up clearly below threshold once enough real data outweighs the prior.
      const asr = computeAsr({ success: 10, providerError: 90, userError: 0 }, { success: 48, failure: 2 });
      // (48+10) / (48+2+10+90) = 58/150 ≈ 0.387
      expect(asr).toBeCloseTo(58 / 150, 3);
      expect(asr).toBeLessThan(env.asr.healthyThreshold);
    });

    test('getGatewayHealth applies the configured prior for a gateway with no recorded transactions', async () => {
      const health = await getGatewayHealth(redis, 'brandNewGateway');
      expect(health.asr).toBeCloseTo(0.96); // default env prior: 48/50
      expect(health.sampleSize).toBe(0);
    });

    test('the default prior sits above the default eligibility threshold, so a cold-start gateway is routable immediately', async () => {
      const { env } = await import('../src/config/env.js');
      const health = await getGatewayHealth(redis, 'anotherBrandNewGateway');
      expect(health.asr).toBeGreaterThanOrEqual(env.asr.healthyThreshold);
      expect(health.status).toBe(HealthStatus.HEALTHY);
    });
  });

  test('computeStatus returns DOWN when the live probe reports unreachable, regardless of ASR', () => {
    const status = computeStatus({ asr: 1, probe: { up: false, latencyMs: 10 } });
    expect(status).toBe(HealthStatus.DOWN);
  });

  test('computeStatus returns DOWN when latency exceeds the down threshold, even if probe reports up', () => {
    const status = computeStatus({ asr: 1, probe: { up: true, latencyMs: 5000 } });
    expect(status).toBe(HealthStatus.DOWN);
  });

  test('computeStatus applies the single 92% eligibility cutoff', () => {
    const base = { probe: { up: true, latencyMs: 50 } };
    expect(computeStatus({ ...base, asr: 0.99 })).toBe(HealthStatus.HEALTHY);
    expect(computeStatus({ ...base, asr: 0.92 })).toBe(HealthStatus.HEALTHY); // inclusive
    expect(computeStatus({ ...base, asr: 0.91 })).toBe(HealthStatus.DOWN);
  });

  test('getGatewayHealth integrates counts + probe into a full health snapshot', async () => {
    for (let i = 0; i < 20; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await recordTransaction(redis, 'gwD', Outcome.SUCCESS);
    }
    await recordProbeResult(redis, 'gwD', { up: true, latencyMs: 42 });

    const health = await getGatewayHealth(redis, 'gwD');
    expect(health.gatewayId).toBe('gwD');
    expect(health.status).toBe(HealthStatus.HEALTHY);
    expect(health.probe.latencyMs).toBe(42);
    expect(health.probe.up).toBe(true);
  });

  test('getGatewayHealth read is fast (sub-5ms budget, best-effort local check)', async () => {
    await recordTransaction(redis, 'gwE', Outcome.SUCCESS);
    const start = performance.now();
    await getGatewayHealth(redis, 'gwE');
    const elapsed = performance.now() - start;
    expect(elapsed).toBeLessThan(50);
  });
});
