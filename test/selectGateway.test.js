import RedisMock from 'ioredis-mock';
import { selectGateway } from '../src/routing/selectGateway.js';
import { recordTransaction, recordProbeResult, Outcome } from '../src/health/asrEngine.js';
import { updateCostConfig, resetCostConfig } from '../src/routing/costConfig.js';
import { incrementTps } from '../src/routing/capacity.js';

describe('selectGateway (least-cost, health-aware, capacity-aware routing)', () => {
  let redis;

  beforeEach(() => {
    redis = new RedisMock();
    resetCostConfig();
  });

  afterEach(async () => {
    await redis.flushall();
  });

  async function makeHealthy(gatewayId, successCount = 20) {
    for (let i = 0; i < successCount; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await recordTransaction(redis, gatewayId, Outcome.SUCCESS);
    }
    await recordProbeResult(redis, gatewayId, { up: true, latencyMs: 50 });
  }

  async function makeDown(gatewayId) {
    for (let i = 0; i < 20; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await recordTransaction(redis, gatewayId, Outcome.PROVIDER_ERROR);
    }
    await recordProbeResult(redis, gatewayId, { up: false, latencyMs: 5000 });
  }

  test('selects the lowest-cost healthy gateway as primary', async () => {
    await makeHealthy('gatewayA');
    await makeHealthy('gatewayB');
    await makeHealthy('gatewayC');

    // Default config: UPI rates are A=0.2%, B=0.15%, C=0.25% -> B should win on cost.
    const result = await selectGateway(redis, 'UPI', 10000);
    expect(result.ok).toBe(true);
    expect(result.candidates[0].gatewayId).toBe('gatewayB');
    expect(result.candidates.map((c) => c.gatewayId)).toEqual(['gatewayB', 'gatewayA', 'gatewayC']);
  });

  test('excludes DOWN gateways from candidate list even if cheapest', async () => {
    await makeDown('gatewayB'); // cheapest for UPI, but down
    await makeHealthy('gatewayA');
    await makeHealthy('gatewayC');

    const result = await selectGateway(redis, 'UPI', 10000);
    expect(result.ok).toBe(true);
    expect(result.candidates.map((c) => c.gatewayId)).not.toContain('gatewayB');
    expect(result.candidates[0].gatewayId).toBe('gatewayA');
  });

  test('returns a typed error result (not a throw) when no gateway is healthy for the channel', async () => {
    await makeDown('gatewayA');
    await makeDown('gatewayB');
    await makeDown('gatewayC');

    const result = await selectGateway(redis, 'UPI', 10000);
    expect(result.ok).toBe(false);
    expect(result.reason).toBe('NO_HEALTHY_GATEWAY');
    expect(result.candidates).toEqual([]);
  });

  test('returns a typed error result when no gateway supports the channel at all', async () => {
    updateCostConfig({ onlyUpiGateway: { UPI: { type: 'flat', value: 1 } } }, { merge: false });
    await makeHealthy('onlyUpiGateway');

    const result = await selectGateway(redis, 'CARD', 5000);
    expect(result.ok).toBe(false);
    expect(result.reason).toBe('NO_HEALTHY_GATEWAY');
  });

  test('a gateway with ASR exactly at the 92% eligibility cutoff is included as a candidate', async () => {
    // Default Bayesian prior is 48/2 (96% over a virtual 50-transaction
    // window). Adding 21 real successes + 4 real provider errors lands
    // exactly on the 92% cutoff: (48+21)/(50+21+4) = 69/75 = 0.92.
    for (let i = 0; i < 21; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await recordTransaction(redis, 'gatewayA', Outcome.SUCCESS);
    }
    for (let i = 0; i < 4; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await recordTransaction(redis, 'gatewayA', Outcome.PROVIDER_ERROR);
    }
    await recordProbeResult(redis, 'gatewayA', { up: true, latencyMs: 50 });
    await makeHealthy('gatewayB');
    await makeHealthy('gatewayC');

    const result = await selectGateway(redis, 'UPI', 10000);
    expect(result.candidates.map((c) => c.gatewayId)).toContain('gatewayA');
  });

  test('a gateway with ASR just below the 92% eligibility cutoff is excluded', async () => {
    // Same prior; 21 successes + 5 provider errors: (48+21)/(50+21+5) = 69/76 ≈ 0.908 < 0.92.
    for (let i = 0; i < 21; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await recordTransaction(redis, 'gatewayA', Outcome.SUCCESS);
    }
    for (let i = 0; i < 5; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await recordTransaction(redis, 'gatewayA', Outcome.PROVIDER_ERROR);
    }
    await recordProbeResult(redis, 'gatewayA', { up: true, latencyMs: 50 });
    await makeHealthy('gatewayB');
    await makeHealthy('gatewayC');

    const result = await selectGateway(redis, 'UPI', 10000);
    expect(result.candidates.map((c) => c.gatewayId)).not.toContain('gatewayA');
  });

  test('cost computation reflects percentage vs flat fee rate types', async () => {
    await makeHealthy('gatewayA');
    await makeHealthy('gatewayB');
    await makeHealthy('gatewayC');

    // NETBANKING uses flat fees: A=8, B=10, C=6 -> C should win.
    const result = await selectGateway(redis, 'NETBANKING', 50000);
    expect(result.candidates[0].gatewayId).toBe('gatewayC');
    expect(result.candidates[0].cost).toBe(6);
  });

  describe('capacity-aware overflow', () => {
    test('a cheap primary at its TPS cap is pushed behind an under-capacity alternate', async () => {
      await makeHealthy('gatewayA');
      await makeHealthy('gatewayB'); // cheapest for UPI
      await makeHealthy('gatewayC');

      // Push gatewayB (the cheapest) to its configured cap.
      await incrementTps(redis, 'gatewayB');
      await incrementTps(redis, 'gatewayB');

      const maxTpsByGateway = { gatewayB: 2 };
      const result = await selectGateway(redis, 'UPI', 10000, { maxTpsByGateway });

      expect(result.ok).toBe(true);
      // gatewayB is still a valid fallback candidate, just no longer primary.
      expect(result.candidates.map((c) => c.gatewayId)).toContain('gatewayB');
      expect(result.candidates[0].gatewayId).not.toBe('gatewayB');
    });

    test('an uncapped gateway (maxTps=0) is never treated as at-capacity', async () => {
      await makeHealthy('gatewayA');
      await makeHealthy('gatewayB');
      await makeHealthy('gatewayC');

      for (let i = 0; i < 500; i += 1) {
        // eslint-disable-next-line no-await-in-loop
        await incrementTps(redis, 'gatewayB');
      }

      const result = await selectGateway(redis, 'UPI', 10000, { maxTpsByGateway: {} });
      expect(result.candidates[0].gatewayId).toBe('gatewayB'); // still cheapest, still primary
    });

    test('below the cap, the cheap primary stays primary', async () => {
      await makeHealthy('gatewayA');
      await makeHealthy('gatewayB');
      await makeHealthy('gatewayC');

      await incrementTps(redis, 'gatewayB'); // well under cap of 180

      const result = await selectGateway(redis, 'UPI', 10000, { maxTpsByGateway: { gatewayB: 180 } });
      expect(result.candidates[0].gatewayId).toBe('gatewayB');
    });
  });
});
