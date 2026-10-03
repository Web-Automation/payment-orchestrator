import RedisMock from 'ioredis-mock';
import { processPayment } from '../src/orchestrator/processPayment.js';
import { handleWebhook } from '../src/orchestrator/reconciliation.js';
import { createMockGatewayAdapter } from '../src/adapters/mockGatewayAdapter.js';
import { MockEventPublisher, EventType } from '../src/events/eventPublisher.js';
import { recordTransaction, getGatewayHealth, Outcome } from '../src/health/asrEngine.js';
import { resetCostConfig } from '../src/routing/costConfig.js';
import { getOrder, AttemptStatus, OrderStatus } from '../src/orchestrator/orderStore.js';
import { parseIdempotencyKey } from '../src/orchestrator/idempotency.js';

describe('processPayment — pre-checkout redirect flow + cascading failover', () => {
  let redis;
  let publisher;
  let adapters;
  let logger;
  let deps;

  beforeEach(async () => {
    redis = new RedisMock();
    resetCostConfig();
    publisher = new MockEventPublisher();
    logger = { info: () => {}, warn: () => {}, error: () => {} };

    adapters = {
      gatewayA: createMockGatewayAdapter('gatewayA', { latencyMs: 1 }),
      gatewayB: createMockGatewayAdapter('gatewayB', { latencyMs: 1 }),
      gatewayC: createMockGatewayAdapter('gatewayC', { latencyMs: 1 }),
    };
    deps = { redis, adapters, publisher, logger };

    // Pre-warm all three gateways as HEALTHY so routing has real candidates.
    for (const id of Object.keys(adapters)) {
      for (let i = 0; i < 20; i += 1) {
        // eslint-disable-next-line no-await-in-loop
        await recordTransaction(redis, id, Outcome.SUCCESS);
      }
    }
  });

  afterEach(async () => {
    await redis.flushall();
  });

  const baseRequest = { channel: 'UPI', amount: 10000, currency: 'INR', userSessionPayload: { token: 'abc' } };

  test('happy path: initiate succeeds on the primary and returns a redirect, not a final outcome', async () => {
    const result = await processPayment(deps, baseRequest);

    expect(result.ok).toBe(true);
    expect(result.gatewayId).toBe('gatewayB'); // cheapest for UPI in default config
    expect(result.redirectUrl).toMatch(/^https:\/\/gatewayB\.mock-pg/);
    expect(result.idempotencyKey).toBe(`${result.orderId}_gatewayB_TRY1`);

    const order = await getOrder(redis, result.orderId);
    expect(order.status).toBe(OrderStatus.PENDING); // NOT complete — settlement is still pending
    expect(order.attempts).toHaveLength(1);
    expect(order.attempts[0].status).toBe(AttemptStatus.INITIATED);
  });

  test('idempotency keys are scoped per order + gateway + attempt number', async () => {
    adapters.gatewayB.setForceDown(true); // primary fails outright -> cascades
    const result = await processPayment(deps, baseRequest);

    const order = await getOrder(redis, result.orderId);
    expect(order.attempts[0].idempotencyKey).toBe(`${result.orderId}_gatewayB_TRY1`);
    expect(order.attempts[1].idempotencyKey).toBe(`${result.orderId}_gatewayA_TRY2`);
    // Keys must round-trip cleanly for webhook routing.
    expect(parseIdempotencyKey(order.attempts[1].idempotencyKey)).toEqual({
      orderId: result.orderId,
      gatewayId: 'gatewayA',
      attemptNumber: 2,
    });
  });

  test('reuses the same order id and session payload across cascaded attempts', async () => {
    adapters.gatewayB.setForceDown(true);
    const chargeSpy = [];
    const original = adapters.gatewayA.initiate.bind(adapters.gatewayA);
    adapters.gatewayA.initiate = async (req) => {
      chargeSpy.push(req);
      return original(req);
    };

    const result = await processPayment(deps, baseRequest);
    expect(chargeSpy).toHaveLength(1);
    expect(chargeSpy[0].orderId).toBe(result.orderId);
    expect(chargeSpy[0].userSessionPayload).toEqual(baseRequest.userSessionPayload);
  });

  test('does NOT cascade on a USER_ERROR — fails immediately without trying another gateway', async () => {
    adapters.gatewayB.setUserErrorRate(1);
    const result = await processPayment(deps, baseRequest);

    expect(result.ok).toBe(false);
    expect(result.reason).toBe('USER_ERROR');
    const order = await getOrder(redis, result.orderId);
    expect(order.attempts).toHaveLength(1);
    expect(order.status).toBe(OrderStatus.FAILED);
  });

  describe('pre-failover verification', () => {
    test('cascades to the secondary when a provider-error is genuinely NOT recoverable (status check: NOT_FOUND)', async () => {
      // gatewayB (primary) fails outright with no session ever created server-side.
      adapters.gatewayB.setFailureRate(1);
      adapters.gatewayB.setGhostAcceptOnTimeout(false);

      const result = await processPayment(deps, baseRequest);

      expect(result.ok).toBe(true);
      expect(result.gatewayId).toBe('gatewayA'); // next-cheapest healthy candidate
      const order = await getOrder(redis, result.orderId);
      expect(order.attempts).toHaveLength(2);
      expect(order.attempts[0]).toMatchObject({ gatewayId: 'gatewayB', status: AttemptStatus.VERIFIED_FAILED });
      expect(order.attempts[1]).toMatchObject({ gatewayId: 'gatewayA', status: AttemptStatus.INITIATED });
    });

    test('does NOT cascade when the status check reveals the "timed out" gateway actually succeeded', async () => {
      // The classic race: our initiate() call times out, but gatewayB (primary)
      // secretly accepted and immediately settled the session.
      adapters.gatewayB.setFailureRate(1);
      adapters.gatewayB.setGhostAcceptOnTimeout(true);
      adapters.gatewayB.setSettleDelayMs(0);
      adapters.gatewayB.setSettleOutcome('SUCCESS');

      const result = await processPayment(deps, baseRequest);

      expect(result.ok).toBe(true);
      expect(result.gatewayId).toBe('gatewayB'); // stayed on the primary — no cascade happened
      expect(result.recoveredViaVerification).toBe(true);

      const order = await getOrder(redis, result.orderId);
      expect(order.attempts).toHaveLength(1); // never touched gatewayA
      expect(order.status).toBe(OrderStatus.COMPLETE);
      expect(order.attempts[0].status).toBe(AttemptStatus.SUCCESS);
    });

    test('a PENDING status at verification time (not yet settled) is treated as not-yet-successful and cascades', async () => {
      adapters.gatewayB.setFailureRate(1);
      adapters.gatewayB.setGhostAcceptOnTimeout(true);
      adapters.gatewayB.setSettleDelayMs(60_000); // won't have settled by the time we check

      const result = await processPayment(deps, baseRequest);

      expect(result.ok).toBe(true);
      expect(result.gatewayId).toBe('gatewayA'); // cascaded despite gatewayB having a live pending session
    });

    test('publishes VERIFIED_RECOVERED (not CASCADED) when verification saves the primary attempt', async () => {
      adapters.gatewayB.setFailureRate(1);
      adapters.gatewayB.setGhostAcceptOnTimeout(true);
      adapters.gatewayB.setSettleDelayMs(0);

      await processPayment(deps, baseRequest);

      const types = publisher.getPublished().map((e) => e.type);
      expect(types).toContain(EventType.VERIFIED_RECOVERED);
      expect(types).not.toContain(EventType.CASCADED);
    });
  });

  test('cascades through multiple failures up to the configured max cascade cap', async () => {
    adapters.gatewayB.setForceDown(true);
    adapters.gatewayA.setForceDown(true);
    // Only gatewayC remains -> with maxCascades=2 (default), 3 attempts allowed total.

    const result = await processPayment(deps, baseRequest);

    expect(result.ok).toBe(true);
    expect(result.gatewayId).toBe('gatewayC');
    const order = await getOrder(redis, result.orderId);
    expect(order.attempts).toHaveLength(3);
  });

  test('returns ALL_CANDIDATES_EXHAUSTED when every candidate fails within the cascade cap', async () => {
    adapters.gatewayA.setForceDown(true);
    adapters.gatewayB.setForceDown(true);
    adapters.gatewayC.setForceDown(true);

    const result = await processPayment(deps, baseRequest);

    expect(result.ok).toBe(false);
    expect(result.reason).toBe('ALL_CANDIDATES_EXHAUSTED');
    const order = await getOrder(redis, result.orderId);
    expect(order.attempts).toHaveLength(3);
    expect(order.status).toBe(OrderStatus.FAILED);
  });

  test('publishes attempt/cascade/redirect events at every stage', async () => {
    adapters.gatewayB.setForceDown(true);
    await processPayment(deps, baseRequest);

    const types = publisher.getPublished().map((e) => e.type);
    expect(types).toContain(EventType.ATTEMPT_STARTED);
    expect(types).toContain(EventType.ATTEMPT_FAILED);
    expect(types).toContain(EventType.CASCADED);
    expect(types).toContain(EventType.REDIRECTED);
  });

  test('initiate-level provider errors feed back into the ASR sliding window for that gateway', async () => {
    adapters.gatewayB.setForceDown(true);
    await processPayment(deps, baseRequest);

    const healthB = await getGatewayHealth(redis, 'gatewayB');
    expect(healthB.counts.providerError).toBe(1);
    expect(healthB.counts.success).toBe(20); // from the pre-warm in beforeEach
  });

  test('returns a typed NO_HEALTHY_GATEWAY result without throwing when nothing is healthy', async () => {
    await redis.flushall();
    for (const id of Object.keys(adapters)) {
      for (let i = 0; i < 20; i += 1) {
        // eslint-disable-next-line no-await-in-loop
        await recordTransaction(redis, id, Outcome.PROVIDER_ERROR);
      }
    }

    const result = await processPayment(deps, baseRequest);
    expect(result.ok).toBe(false);
    expect(result.reason).toBe('NO_HEALTHY_GATEWAY');
  });
});

describe('reconciliation — double-capture detection and auto-refund', () => {
  let redis;
  let publisher;
  let adapters;
  let logger;
  let deps;

  beforeEach(async () => {
    redis = new RedisMock();
    resetCostConfig();
    publisher = new MockEventPublisher();
    logger = { info: () => {}, warn: () => {}, error: () => {} };
    adapters = {
      gatewayA: createMockGatewayAdapter('gatewayA', { latencyMs: 1 }),
      gatewayB: createMockGatewayAdapter('gatewayB', { latencyMs: 1 }),
      gatewayC: createMockGatewayAdapter('gatewayC', { latencyMs: 1 }),
    };
    deps = { redis, adapters, publisher, logger };
    for (const id of Object.keys(adapters)) {
      for (let i = 0; i < 20; i += 1) {
        // eslint-disable-next-line no-await-in-loop
        await recordTransaction(redis, id, Outcome.SUCCESS);
      }
    }
  });

  afterEach(async () => {
    await redis.flushall();
  });

  const baseRequest = { channel: 'UPI', amount: 10000, currency: 'INR', userSessionPayload: { token: 'abc' } };

  test('a late webhook confirming success on an already-cascaded-away gateway triggers an auto-refund, not a second success', async () => {
    // gatewayB (primary) "times out" from our side but secretly accepted the
    // session; it just hasn't settled by the time we do our verification
    // check, so we correctly cascade to gatewayA per policy.
    adapters.gatewayB.setFailureRate(1);
    adapters.gatewayB.setGhostAcceptOnTimeout(true);
    adapters.gatewayB.setSettleDelayMs(60_000);

    const result = await processPayment(deps, baseRequest);
    expect(result.ok).toBe(true);
    expect(result.gatewayId).toBe('gatewayA');

    const orderBefore = await getOrder(redis, result.orderId);
    const gatewayBAttempt = orderBefore.attempts.find((a) => a.gatewayId === 'gatewayB');
    expect(gatewayBAttempt.status).toBe(AttemptStatus.VERIFIED_FAILED);

    // gatewayA's normal settlement webhook arrives first.
    await handleWebhook(deps, {
      gatewayId: 'gatewayA',
      idempotencyKey: orderBefore.attempts.find((a) => a.gatewayId === 'gatewayA').idempotencyKey,
      orderId: result.orderId,
      status: 'SUCCESS',
    });

    // 30 seconds later (simulated), gatewayB's session — which really was
    // created — also settles and fires its webhook.
    adapters.gatewayB.forceSettle(gatewayBAttempt.idempotencyKey, 'SUCCESS');
    const webhookResult = await handleWebhook(deps, {
      gatewayId: 'gatewayB',
      idempotencyKey: gatewayBAttempt.idempotencyKey,
      orderId: result.orderId,
      status: 'SUCCESS',
    });
    expect(webhookResult.ok).toBe(true);

    const finalOrder = await getOrder(redis, result.orderId);
    const successes = finalOrder.attempts.filter((a) => a.status === AttemptStatus.SUCCESS);
    const refunded = finalOrder.attempts.filter((a) => a.status === AttemptStatus.REFUNDED);

    // Exactly one attempt should remain SUCCESS; the late duplicate must be refunded, not left as a second success.
    expect(successes).toHaveLength(1);
    expect(successes[0].gatewayId).toBe('gatewayA'); // first-confirmed winner
    expect(refunded).toHaveLength(1);
    expect(refunded[0].gatewayId).toBe('gatewayB');
    expect(finalOrder.status).toBe(OrderStatus.COMPLETE);
    expect(finalOrder.winningIdempotencyKey).toBe(successes[0].idempotencyKey);

    // The refund must have actually been executed against gatewayB's adapter.
    const gatewayBSession = adapters.gatewayB.getSession(gatewayBAttempt.idempotencyKey);
    expect(gatewayBSession.refunded).toBe(true);

    const eventTypes = publisher.getPublished().map((e) => e.type);
    expect(eventTypes).toContain(EventType.RECONCILED_REFUND);
  });

  test('a single normal webhook success marks the order complete with no refund action', async () => {
    const result = await processPayment(deps, baseRequest);
    await handleWebhook(deps, { gatewayId: result.gatewayId, idempotencyKey: result.idempotencyKey, orderId: result.orderId, status: 'SUCCESS' });

    const order = await getOrder(redis, result.orderId);
    expect(order.status).toBe(OrderStatus.COMPLETE);
    expect(order.attempts.filter((a) => a.status === AttemptStatus.REFUNDED)).toHaveLength(0);
    const eventTypes = publisher.getPublished().map((e) => e.type);
    expect(eventTypes).not.toContain(EventType.RECONCILED_REFUND);
  });

  test('a duplicate webhook delivery for the same attempt does not double-record ASR', async () => {
    const result = await processPayment(deps, baseRequest);
    await handleWebhook(deps, { gatewayId: result.gatewayId, idempotencyKey: result.idempotencyKey, orderId: result.orderId, status: 'SUCCESS' });
    const healthAfterFirst = await getGatewayHealth(redis, result.gatewayId);

    await handleWebhook(deps, { gatewayId: result.gatewayId, idempotencyKey: result.idempotencyKey, orderId: result.orderId, status: 'SUCCESS' });
    const healthAfterSecond = await getGatewayHealth(redis, result.gatewayId);

    expect(healthAfterSecond.counts.success).toBe(healthAfterFirst.counts.success);
  });
});
