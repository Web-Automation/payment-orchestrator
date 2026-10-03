import RedisMock from 'ioredis-mock';
import { buildApp } from '../src/api/buildApp.js';
import { createMockGatewayAdapter } from '../src/adapters/mockGatewayAdapter.js';
import { MockEventPublisher } from '../src/events/eventPublisher.js';
import { recordTransaction, Outcome } from '../src/health/asrEngine.js';
import { resetCostConfig } from '../src/routing/costConfig.js';

describe('API surface', () => {
  let app;
  let redis;
  let adapters;

  beforeEach(async () => {
    redis = new RedisMock();
    resetCostConfig();
    adapters = {
      gatewayA: createMockGatewayAdapter('gatewayA', { latencyMs: 1 }),
      gatewayB: createMockGatewayAdapter('gatewayB', { latencyMs: 1 }),
      gatewayC: createMockGatewayAdapter('gatewayC', { latencyMs: 1 }),
    };
    for (const id of Object.keys(adapters)) {
      for (let i = 0; i < 20; i += 1) {
        // eslint-disable-next-line no-await-in-loop
        await recordTransaction(redis, id, Outcome.SUCCESS);
      }
    }
    const deps = { redis, adapters, publisher: new MockEventPublisher(), logger: { info() {}, warn() {}, error() {} } };
    app = buildApp(deps);
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
    await redis.flushall();
  });

  test('POST /v1/payments returns 200 with a redirect URL, not a final payment outcome', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/payments',
      payload: { channel: 'UPI', amount: 1000, currency: 'INR', userSessionPayload: { token: 'x' } },
    });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.ok).toBe(true);
    expect(body.gatewayId).toBeDefined();
    expect(body.redirectUrl).toMatch(/^https:\/\//);
    expect(body.orderId).toBeDefined();
    expect(body.idempotencyKey).toBeDefined();
  });

  test('POST /v1/payments returns 400 for an invalid channel', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/payments',
      payload: { channel: 'BITCOIN', amount: 1000, currency: 'INR' },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().reason).toBe('INVALID_CHANNEL');
  });

  test('POST /v1/payments returns 400 for a non-positive amount', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/payments',
      payload: { channel: 'UPI', amount: -5, currency: 'INR' },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().reason).toBe('INVALID_AMOUNT');
  });

  test('GET /v1/orders/:orderId returns order + attempt metadata only — no card details, no session payload', async () => {
    const paymentRes = await app.inject({
      method: 'POST',
      url: '/v1/payments',
      payload: { channel: 'UPI', amount: 1000, currency: 'INR', userSessionPayload: { cardNumber: '4111111111111111', cvv: '123' } },
    });
    const { orderId } = paymentRes.json();

    const res = await app.inject({ method: 'GET', url: `/v1/orders/${orderId}` });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.orderId).toBe(orderId);
    expect(body.attempts.length).toBeGreaterThan(0);

    const serialized = JSON.stringify(body);
    expect(serialized).not.toContain('4111111111111111');
    expect(serialized).not.toContain('cvv');
  });

  test('GET /v1/orders/:orderId returns 404 for an unknown order', async () => {
    const res = await app.inject({ method: 'GET', url: '/v1/orders/ORD_does_not_exist' });
    expect(res.statusCode).toBe(404);
  });

  test('POST /v1/webhooks/:gatewayId confirms settlement and marks the order complete', async () => {
    const paymentRes = await app.inject({
      method: 'POST',
      url: '/v1/payments',
      payload: { channel: 'UPI', amount: 1000, currency: 'INR', userSessionPayload: { token: 'x' } },
    });
    const { orderId, gatewayId, idempotencyKey } = paymentRes.json();

    const webhookRes = await app.inject({
      method: 'POST',
      url: `/v1/webhooks/${gatewayId}`,
      payload: { idempotencyKey, status: 'SUCCESS' },
    });
    expect(webhookRes.statusCode).toBe(200);

    const orderRes = await app.inject({ method: 'GET', url: `/v1/orders/${orderId}` });
    expect(orderRes.json().status).toBe('COMPLETE');
  });

  test('POST /v1/webhooks/:gatewayId returns 400 for a malformed payload', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/webhooks/gatewayA',
      payload: { status: 'SUCCESS' }, // missing idempotencyKey
    });
    expect(res.statusCode).toBe(400);
  });

  test('GET /v1/gateways/health returns health/status/ASR for all configured gateways', async () => {
    const res = await app.inject({ method: 'GET', url: '/v1/gateways/health' });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(Array.isArray(body.gateways)).toBe(true);
    expect(body.gateways.length).toBeGreaterThan(0);
    expect(body.gateways[0]).toHaveProperty('status');
    expect(body.gateways[0]).toHaveProperty('asr');
  });

  test('POST /v1/gateways/config updates routing cost table without redeploy', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/gateways/config',
      payload: { gatewayA: { UPI: { type: 'flat', value: 0.01 } } },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json().config.gatewayA.UPI).toEqual({ type: 'flat', value: 0.01 });

    const getRes = await app.inject({ method: 'GET', url: '/v1/gateways/config' });
    expect(getRes.json().config.gatewayA.UPI.value).toBe(0.01);
  });

  test('GET /healthz returns ok', async () => {
    const res = await app.inject({ method: 'GET', url: '/healthz' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ status: 'ok' });
  });
});
