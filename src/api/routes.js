import { processPayment } from '../orchestrator/processPayment.js';
import { handleWebhook } from '../orchestrator/reconciliation.js';
import { getOrder } from '../orchestrator/orderStore.js';
import { parseIdempotencyKey } from '../orchestrator/idempotency.js';
import { getAllGatewaysHealth } from '../health/asrEngine.js';
import { getCostConfig, updateCostConfig } from '../routing/costConfig.js';
import { env } from '../config/env.js';

const VALID_CHANNELS = new Set(['UPI', 'CARD', 'NETBANKING']);

/**
 * Registers all API routes onto a Fastify instance. `deps` carries the
 * shared redis client, adapter map, event publisher, and logger so routes
 * stay thin and testable (see test/api.test.js, using the same shape via
 * light-my-request injection).
 */
export function registerRoutes(app, deps) {
  // Pre-checkout metadata routing + redirect creation. This does NOT
  // confirm the payment succeeded — it confirms a checkout session was
  // created on the chosen gateway and hands back the URL to redirect the
  // user to. Final settlement is confirmed asynchronously via
  // POST /v1/webhooks/:gatewayId below.
  app.post('/v1/payments', async (request, reply) => {
    const { channel, amount, currency, userSessionPayload, orderId } = request.body ?? {};

    if (!channel || !VALID_CHANNELS.has(channel)) {
      reply.code(400);
      return { ok: false, reason: 'INVALID_CHANNEL', message: `channel must be one of ${[...VALID_CHANNELS].join(', ')}` };
    }
    if (typeof amount !== 'number' || amount <= 0) {
      reply.code(400);
      return { ok: false, reason: 'INVALID_AMOUNT', message: 'amount must be a positive number' };
    }
    if (!currency) {
      reply.code(400);
      return { ok: false, reason: 'INVALID_CURRENCY', message: 'currency is required' };
    }

    const result = await processPayment(deps, { orderId, channel, amount, currency, userSessionPayload });
    reply.code(result.ok ? 200 : 402);
    return result;
  });

  // Async settlement confirmation from a gateway. Real gateways would sign
  // this payload; signature verification is omitted here since it's
  // provider-specific, but a production deployment MUST verify it before
  // trusting the body.
  app.post('/v1/webhooks/:gatewayId', async (request, reply) => {
    const { gatewayId } = request.params;
    const { idempotencyKey, status, reason } = request.body ?? {};

    if (!idempotencyKey || !['SUCCESS', 'FAILED'].includes(status)) {
      reply.code(400);
      return { ok: false, reason: 'INVALID_WEBHOOK_PAYLOAD' };
    }
    const parsed = parseIdempotencyKey(idempotencyKey);
    if (!parsed) {
      reply.code(400);
      return { ok: false, reason: 'INVALID_IDEMPOTENCY_KEY' };
    }

    deps.publisher.publish({
      type: 'WEBHOOK_RECEIVED',
      timestamp: new Date().toISOString(),
      gatewayId,
      idempotencyKey,
      orderId: parsed.orderId,
      status,
    });

    const result = await handleWebhook(deps, { gatewayId, idempotencyKey, orderId: parsed.orderId, status, reason });
    reply.code(result.ok ? 200 : 404);
    return result;
  });

  app.get('/v1/orders/:orderId', async (request, reply) => {
    const order = await getOrder(deps.redis, request.params.orderId);
    if (!order) {
      reply.code(404);
      return { ok: false, reason: 'ORDER_NOT_FOUND' };
    }
    return order;
  });

  app.get('/v1/gateways/health', async () => {
    const health = await getAllGatewaysHealth(deps.redis, env.gateways);
    return { gateways: health };
  });

  app.post('/v1/gateways/config', async (request, reply) => {
    const body = request.body ?? {};
    if (typeof body !== 'object' || Array.isArray(body)) {
      reply.code(400);
      return { ok: false, reason: 'INVALID_CONFIG', message: 'body must be an object keyed by gatewayId' };
    }
    const merge = request.query?.merge !== 'false';
    const updated = updateCostConfig(body, { merge });
    return { ok: true, config: updated };
  });

  app.get('/v1/gateways/config', async () => ({ config: getCostConfig() }));

  app.get('/healthz', async () => ({ status: 'ok' }));
}
