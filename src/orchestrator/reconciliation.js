import { withLock, orderLockKey } from './distributedLock.js';
import { getOrder, updateAttemptStatus, markOrderComplete, successfulAttempts, AttemptStatus } from './orderStore.js';
import { recordTransaction, Outcome } from '../health/asrEngine.js';
import { EventType, buildEvent } from '../events/eventPublisher.js';
import { env } from '../config/env.js';

/**
 * The safety net for the case pre-failover verification can't fully rule
 * out: gateway A times out, we verify-and-cascade to gateway B, B succeeds
 * — but 30 seconds later A turns out to have secretly settled the payment
 * too. Both gateways fire a webhook. This worker detects that a single
 * order now has more than one successful capture and automatically refunds
 * every successful attempt except the first-confirmed winner, logging an
 * audit trail event for each refund.
 *
 * Runs under the same per-order distributed lock used by pre-failover
 * verification, so a webhook arriving mid-cascade can't race the
 * orchestrator's own read-modify-write of the order.
 */
export async function reconcileOrder(deps, orderId) {
  const { redis, adapters, publisher, logger } = deps;
  const lockTtlMs = deps.lockTtlMs ?? env.failover.lockTtlMs;

  const { acquired, result } = await withLock(redis, orderLockKey(orderId), lockTtlMs, async () => {
    const order = await getOrder(redis, orderId);
    if (!order) return { refunded: [] };

    const successes = successfulAttempts(order);
    if (successes.length <= 1) {
      // Nothing to reconcile — 0 or 1 confirmed success is the normal case.
      if (successes.length === 1 && order.status !== 'COMPLETE') {
        await markOrderComplete(redis, orderId, successes[0].idempotencyKey);
      }
      return { refunded: [] };
    }

    // More than one successful capture for the same order: keep the
    // earliest-confirmed attempt (or the order's already-recorded winner,
    // if pre-failover verification already picked one) and refund the rest.
    const winner =
      successes.find((a) => a.idempotencyKey === order.winningIdempotencyKey) ??
      [...successes].sort((a, b) => a.attemptNumber - b.attemptNumber)[0];

    if (!order.winningIdempotencyKey) {
      await markOrderComplete(redis, orderId, winner.idempotencyKey);
    }

    const refunded = [];
    for (const attempt of successes) {
      if (attempt.idempotencyKey === winner.idempotencyKey) continue;
      const adapter = adapters[attempt.gatewayId];
      // eslint-disable-next-line no-await-in-loop
      const refundResult = await adapter.refund(attempt.idempotencyKey, order.amount);
      // eslint-disable-next-line no-await-in-loop
      await updateAttemptStatus(redis, orderId, attempt.idempotencyKey, AttemptStatus.REFUNDED);
      // eslint-disable-next-line no-await-in-loop
      await publisher.publish(
        buildEvent(EventType.RECONCILED_REFUND, {
          orderId,
          gatewayId: attempt.gatewayId,
          idempotencyKey: attempt.idempotencyKey,
          winningIdempotencyKey: winner.idempotencyKey,
          refundOk: refundResult.ok,
          refundId: refundResult.refundId,
        })
      );
      logger?.warn(
        { orderId, gatewayId: attempt.gatewayId, idempotencyKey: attempt.idempotencyKey, winningIdempotencyKey: winner.idempotencyKey },
        'double-capture detected — auto-refunded late-settling gateway'
      );
      refunded.push({ gatewayId: attempt.gatewayId, idempotencyKey: attempt.idempotencyKey, ...refundResult });
    }

    return { refunded, winner: winner.idempotencyKey };
  });

  if (!acquired) {
    logger?.warn({ orderId }, 'could not acquire order lock for reconciliation; will retry on next webhook');
    return { refunded: [] };
  }
  return result;
}

/**
 * Applies an incoming webhook's settlement status to the matching attempt,
 * feeds the outcome back into that gateway's ASR window (skipping
 * already-terminal attempts so duplicate webhook deliveries don't double
 * count), then runs reconciliation to catch any double-capture.
 */
export async function handleWebhook(deps, { gatewayId, idempotencyKey, orderId, status, reason }) {
  const { redis, logger } = deps;

  const order = await getOrder(redis, orderId);
  if (!order) {
    logger?.warn({ orderId, idempotencyKey }, 'webhook for unknown order');
    return { ok: false, reason: 'UNKNOWN_ORDER' };
  }
  const attempt = order.attempts.find((a) => a.idempotencyKey === idempotencyKey);
  if (!attempt) {
    logger?.warn({ orderId, idempotencyKey }, 'webhook for unknown attempt');
    return { ok: false, reason: 'UNKNOWN_ATTEMPT' };
  }

  const alreadyTerminal = [AttemptStatus.SUCCESS, AttemptStatus.FAILED, AttemptStatus.REFUNDED].includes(attempt.status);

  const newStatus = status === 'SUCCESS' ? AttemptStatus.SUCCESS : AttemptStatus.FAILED;
  await updateAttemptStatus(redis, orderId, idempotencyKey, newStatus);

  if (!alreadyTerminal) {
    const outcome = status === 'SUCCESS' ? Outcome.SUCCESS : reason === 'USER_ERROR' ? Outcome.USER_ERROR : Outcome.PROVIDER_ERROR;
    await recordTransaction(redis, gatewayId, outcome);
  }

  await reconcileOrder(deps, orderId);
  return { ok: true };
}
