import { env } from '../config/env.js';
import { selectGateway } from '../routing/selectGateway.js';
import { incrementTps } from '../routing/capacity.js';
import { recordTransaction, Outcome } from '../health/asrEngine.js';
import { EventType, buildEvent } from '../events/eventPublisher.js';
import { buildIdempotencyKey } from './idempotency.js';
import { withLock, orderLockKey } from './distributedLock.js';
import {
  createOrder,
  addAttempt,
  updateAttemptStatus,
  markOrderComplete,
  markOrderFailed,
  AttemptStatus,
} from './orderStore.js';

function withTimeout(promise, ms, onTimeoutValue) {
  let timer;
  const timeout = new Promise((resolve) => {
    timer = setTimeout(() => resolve(onTimeoutValue), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * Orchestrates a single payment as a pre-checkout metadata router:
 *
 *   1. selectGateway() picks the ranked candidate list — cheapest healthy
 *      gateway first, with capacity-aware overflow.
 *   2. initiate() is called on the top candidate. Success here means the
 *      gateway accepted the session and returned a redirect URL — NOT that
 *      the payment has settled. The caller (checkout) redirects the user to
 *      that URL; actual settlement is confirmed later via webhook
 *      (see api/routes.js `/v1/webhooks/:gatewayId` + reconciliation.js).
 *   3. If initiate() fails with a provider-side error (timeout/5xx), the
 *      orchestrator does NOT blindly cascade. It acquires a distributed
 *      lock on the order and performs a synchronous out-of-band status
 *      check against the same gateway first — because in a redirect flow, a
 *      "timeout" on our end doesn't mean the gateway didn't process it. If
 *      that check confirms SUCCESS, the order is completed with that
 *      gateway and failover is cancelled. Only if it's confirmed NOT
 *      successful does the orchestrator cascade to the next candidate, each
 *      attempt carrying its own gateway-and-attempt-scoped idempotency key
 *      so a late-recovering earlier attempt can never collide with a later
 *      one.
 *   4. USER_ERROR (e.g. invalid instrument) never cascades — a different
 *      gateway can't fix bad input.
 *
 * This module never stores card details, OTPs, or the raw
 * userSessionPayload — those are used in-memory for this call only. Only
 * orchestration metadata (order id, gateway id, idempotency key, attempt
 * status, cost) is persisted, via orderStore.js.
 */
export async function processPayment(deps, transactionRequest) {
  const { redis, adapters, publisher, logger } = deps;
  const { channel, amount, currency, userSessionPayload } = transactionRequest;
  const orderId = transactionRequest.orderId ?? `ORD_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const maxCascades = deps.maxCascades ?? env.failover.maxCascades;
  const attemptTimeoutMs = deps.attemptTimeoutMs ?? env.failover.attemptTimeoutMs;
  const statusCheckTimeoutMs = deps.statusCheckTimeoutMs ?? env.failover.statusCheckTimeoutMs;
  const lockTtlMs = deps.lockTtlMs ?? env.failover.lockTtlMs;

  const orchestrationStart = performance.now();

  await createOrder(redis, { orderId, channel, amount, currency });

  const routingStart = performance.now();
  const routing = await selectGateway(redis, channel, amount, deps.routingOpts);
  const routingDecisionMs = Number((performance.now() - routingStart).toFixed(2));
  logger?.info({ orderId, routingDecisionMs }, 'routing decision latency'); // ASR read + cost ranking only, excludes PG network calls

  if (!routing.ok) {
    await markOrderFailed(redis, orderId);
    await publisher.publish(
      buildEvent(EventType.FINAL_OUTCOME, { orderId, channel, amount, currency, success: false, reason: routing.reason })
    );
    logger?.warn({ orderId, channel, reason: routing.reason }, 'no healthy gateway for channel');
    return { ok: false, orderId, reason: routing.reason, message: routing.message };
  }

  const candidates = routing.candidates.slice(0, maxCascades + 1);

  for (let i = 0; i < candidates.length; i += 1) {
    const candidate = candidates[i];
    const adapter = adapters[candidate.gatewayId];
    const attemptNumber = i + 1;
    const idempotencyKey = buildIdempotencyKey(orderId, candidate.gatewayId, attemptNumber);
    const isLastCandidate = i === candidates.length - 1;

    // eslint-disable-next-line no-await-in-loop
    await addAttempt(redis, orderId, {
      gatewayId: candidate.gatewayId,
      idempotencyKey,
      attemptNumber,
      cost: candidate.cost,
      status: AttemptStatus.INITIATED,
    });
    // eslint-disable-next-line no-await-in-loop
    await incrementTps(redis, candidate.gatewayId);

    // eslint-disable-next-line no-await-in-loop
    await publisher.publish(
      buildEvent(EventType.ATTEMPT_STARTED, {
        orderId,
        idempotencyKey,
        gatewayId: candidate.gatewayId,
        attemptNumber,
        cost: candidate.cost,
        asrAtDecision: candidate.asr,
      })
    );

    // eslint-disable-next-line no-await-in-loop
    const initResult = await withTimeout(
      adapter.initiate({ orderId, idempotencyKey, channel, amount, currency, userSessionPayload }),
      attemptTimeoutMs,
      { status: 'PROVIDER_ERROR', gatewayId: candidate.gatewayId, raw: { error: 'attempt_timeout' } }
    );

    if (initResult.status === 'INITIATED') {
      // eslint-disable-next-line no-await-in-loop
      await publisher.publish(
        buildEvent(EventType.REDIRECTED, { orderId, idempotencyKey, gatewayId: candidate.gatewayId, redirectUrl: initResult.redirectUrl })
      );
      logOverhead(logger, orderId, orchestrationStart);
      return {
        ok: true,
        orderId,
        gatewayId: candidate.gatewayId,
        idempotencyKey,
        redirectUrl: initResult.redirectUrl,
        cost: candidate.cost,
        // Final payment success/failure is confirmed asynchronously via
        // webhook — this response only confirms a checkout session was
        // created on the chosen gateway.
      };
    }

    if (initResult.status === 'USER_ERROR') {
      // eslint-disable-next-line no-await-in-loop
      await recordTransaction(redis, candidate.gatewayId, Outcome.USER_ERROR);
      // eslint-disable-next-line no-await-in-loop
      await updateAttemptStatus(redis, orderId, idempotencyKey, AttemptStatus.FAILED);
      // eslint-disable-next-line no-await-in-loop
      await markOrderFailed(redis, orderId);
      // eslint-disable-next-line no-await-in-loop
      await publisher.publish(
        buildEvent(EventType.ATTEMPT_FAILED, { orderId, idempotencyKey, gatewayId: candidate.gatewayId, reason: 'USER_ERROR', raw: initResult.raw })
      );
      // eslint-disable-next-line no-await-in-loop
      await publisher.publish(
        buildEvent(EventType.FINAL_OUTCOME, { orderId, gatewayId: candidate.gatewayId, success: false, reason: 'USER_ERROR' })
      );
      logOverhead(logger, orderId, orchestrationStart);
      return { ok: false, orderId, reason: 'USER_ERROR', gatewayId: candidate.gatewayId };
    }

    // --- PROVIDER_ERROR: don't blindly cascade — verify first ---
    // eslint-disable-next-line no-await-in-loop
    await recordTransaction(redis, candidate.gatewayId, Outcome.PROVIDER_ERROR);
    // eslint-disable-next-line no-await-in-loop
    await updateAttemptStatus(redis, orderId, idempotencyKey, AttemptStatus.PROVIDER_ERROR);
    // eslint-disable-next-line no-await-in-loop
    await publisher.publish(
      buildEvent(EventType.ATTEMPT_FAILED, { orderId, idempotencyKey, gatewayId: candidate.gatewayId, reason: 'PROVIDER_ERROR', raw: initResult.raw })
    );

    // eslint-disable-next-line no-await-in-loop
    const { acquired, result: verification } = await withLock(redis, orderLockKey(orderId), lockTtlMs, () =>
      withTimeout(adapter.checkStatus(idempotencyKey), statusCheckTimeoutMs, { status: 'NOT_FOUND' })
    );

    if (!acquired) {
      logger?.warn({ orderId, idempotencyKey }, 'could not acquire order lock for pre-failover verification; proceeding to cascade');
    }

    const verifiedSuccess = acquired && verification?.status === 'SUCCESS';

    if (verifiedSuccess) {
      // eslint-disable-next-line no-await-in-loop
      await recordTransaction(redis, candidate.gatewayId, Outcome.SUCCESS);
      // eslint-disable-next-line no-await-in-loop
      await updateAttemptStatus(redis, orderId, idempotencyKey, AttemptStatus.SUCCESS);
      // eslint-disable-next-line no-await-in-loop
      await markOrderComplete(redis, orderId, idempotencyKey);
      // eslint-disable-next-line no-await-in-loop
      await publisher.publish(
        buildEvent(EventType.VERIFIED_RECOVERED, { orderId, idempotencyKey, gatewayId: candidate.gatewayId })
      );
      // eslint-disable-next-line no-await-in-loop
      await publisher.publish(
        buildEvent(EventType.FINAL_OUTCOME, { orderId, gatewayId: candidate.gatewayId, success: true, recoveredViaVerification: true })
      );
      logOverhead(logger, orderId, orchestrationStart);
      return { ok: true, orderId, gatewayId: candidate.gatewayId, idempotencyKey, recoveredViaVerification: true };
    }

    // eslint-disable-next-line no-await-in-loop
    await updateAttemptStatus(redis, orderId, idempotencyKey, AttemptStatus.VERIFIED_FAILED);

    if (!isLastCandidate) {
      // eslint-disable-next-line no-await-in-loop
      await publisher.publish(
        buildEvent(EventType.CASCADED, {
          orderId,
          fromGatewayId: candidate.gatewayId,
          toGatewayId: candidates[i + 1].gatewayId,
          attemptNumber,
        })
      );
    }
  }

  await markOrderFailed(redis, orderId);
  await publisher.publish(
    buildEvent(EventType.FINAL_OUTCOME, { orderId, success: false, reason: 'ALL_CANDIDATES_EXHAUSTED' })
  );
  logOverhead(logger, orderId, orchestrationStart);
  return { ok: false, orderId, reason: 'ALL_CANDIDATES_EXHAUSTED' };
}

function logOverhead(logger, orderId, orchestrationStart) {
  const totalMs = Number((performance.now() - orchestrationStart).toFixed(2));
  logger?.info({ orderId, orchestrationOverheadMs: totalMs }, 'orchestration overhead');
}
