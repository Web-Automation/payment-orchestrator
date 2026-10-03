/**
 * Deterministic, gateway-scoped idempotency keys.
 *
 * The orchestrator keeps one parent Order ID (e.g. ORD_1001) but generates a
 * distinct key per gateway attempt: ORD_1001_gatewayA_TRY1, then
 * ORD_1001_gatewayB_TRY2 on cascade, etc. Scoping the key to both the
 * gateway AND the attempt number means that if gatewayA recovers later and
 * actually processes ORD_1001_gatewayA_TRY1 after we've already cascaded,
 * it can never collide with or re-trigger gatewayB's attempt — each key
 * identifies exactly one (order, gateway, attempt) tuple.
 */
export function buildIdempotencyKey(orderId, gatewayId, attemptNumber) {
  return `${orderId}_${gatewayId}_TRY${attemptNumber}`;
}

/** Parses a key back into its parts — used by webhook handlers to find the parent order. */
export function parseIdempotencyKey(key) {
  const match = /^(.+)_([^_]+)_TRY(\d+)$/.exec(key);
  if (!match) return null;
  const [, orderId, gatewayId, attemptNumber] = match;
  return { orderId, gatewayId, attemptNumber: Number(attemptNumber) };
}
