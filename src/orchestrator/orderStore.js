/**
 * Stores parent-order + per-gateway-attempt state in Redis.
 *
 * IMPORTANT: this store only ever holds orchestration metadata — order id,
 * channel, amount, currency, gateway id, idempotency key, and attempt
 * status. It never stores card numbers, UPI PINs, OTPs, or the raw
 * userSessionPayload handed to a gateway. That payload is used in-memory
 * for the duration of a single request/attempt and discarded — see
 * processPayment.js, which never passes it into this module.
 */

export const AttemptStatus = Object.freeze({
  INITIATED: 'INITIATED', // gateway accepted the session, user redirected, awaiting settlement
  PROVIDER_ERROR: 'PROVIDER_ERROR', // initiate() call itself failed/timed out
  VERIFIED_FAILED: 'VERIFIED_FAILED', // pre-failover status check confirmed it did NOT go through
  SUCCESS: 'SUCCESS', // confirmed success, either via verification check or webhook
  FAILED: 'FAILED', // confirmed failure via webhook
  REFUNDED: 'REFUNDED', // reconciliation auto-refunded this attempt (double-capture cleanup)
});

export const OrderStatus = Object.freeze({
  PENDING: 'PENDING',
  COMPLETE: 'COMPLETE',
  FAILED: 'FAILED',
});

const orderKey = (orderId) => `order:${orderId}`;

async function readOrder(redis, orderId) {
  const raw = await redis.get(orderKey(orderId));
  return raw ? JSON.parse(raw) : null;
}

async function writeOrder(redis, order) {
  await redis.set(orderKey(order.orderId), JSON.stringify(order));
  return order;
}

export async function createOrder(redis, { orderId, channel, amount, currency }) {
  const existing = await readOrder(redis, orderId);
  if (existing) return existing; // idempotent: re-using an existing order id just returns it
  const order = {
    orderId,
    channel,
    amount,
    currency,
    status: OrderStatus.PENDING,
    attempts: [],
    winningIdempotencyKey: null,
    createdAt: new Date().toISOString(),
  };
  return writeOrder(redis, order);
}

export async function getOrder(redis, orderId) {
  return readOrder(redis, orderId);
}

export async function addAttempt(redis, orderId, { gatewayId, idempotencyKey, attemptNumber, cost, status }) {
  const order = await readOrder(redis, orderId);
  if (!order) throw new Error(`addAttempt: unknown order ${orderId}`);
  order.attempts.push({
    gatewayId,
    idempotencyKey,
    attemptNumber,
    cost,
    status,
    createdAt: new Date().toISOString(),
  });
  return writeOrder(redis, order);
}

export async function updateAttemptStatus(redis, orderId, idempotencyKey, status) {
  const order = await readOrder(redis, orderId);
  if (!order) throw new Error(`updateAttemptStatus: unknown order ${orderId}`);
  const attempt = order.attempts.find((a) => a.idempotencyKey === idempotencyKey);
  if (!attempt) throw new Error(`updateAttemptStatus: unknown attempt ${idempotencyKey}`);
  attempt.status = status;
  attempt.updatedAt = new Date().toISOString();
  return writeOrder(redis, order);
}

export async function markOrderComplete(redis, orderId, winningIdempotencyKey) {
  const order = await readOrder(redis, orderId);
  if (!order) throw new Error(`markOrderComplete: unknown order ${orderId}`);
  order.status = OrderStatus.COMPLETE;
  order.winningIdempotencyKey = winningIdempotencyKey;
  return writeOrder(redis, order);
}

export async function markOrderFailed(redis, orderId) {
  const order = await readOrder(redis, orderId);
  if (!order) throw new Error(`markOrderFailed: unknown order ${orderId}`);
  order.status = OrderStatus.FAILED;
  return writeOrder(redis, order);
}

/** All attempts currently recorded as SUCCESS for an order — used by reconciliation. */
export function successfulAttempts(order) {
  return order.attempts.filter((a) => a.status === AttemptStatus.SUCCESS);
}
