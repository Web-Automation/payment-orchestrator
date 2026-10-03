import { buildIdempotencyKey, parseIdempotencyKey } from '../src/orchestrator/idempotency.js';

describe('idempotency keys', () => {
  test('builds gateway- and attempt-scoped keys', () => {
    expect(buildIdempotencyKey('ORD_1001', 'gatewayA', 1)).toBe('ORD_1001_gatewayA_TRY1');
    expect(buildIdempotencyKey('ORD_1001', 'gatewayB', 2)).toBe('ORD_1001_gatewayB_TRY2');
  });

  test('a recovered first attempt can never collide with a later cascaded attempt', () => {
    const try1 = buildIdempotencyKey('ORD_1001', 'gatewayA', 1);
    const try2 = buildIdempotencyKey('ORD_1001', 'gatewayB', 2);
    expect(try1).not.toBe(try2);
  });

  test('parses a key back into its order/gateway/attempt parts, even when the order id contains underscores', () => {
    const key = buildIdempotencyKey('ORD_1001', 'gatewayA', 1);
    expect(parseIdempotencyKey(key)).toEqual({ orderId: 'ORD_1001', gatewayId: 'gatewayA', attemptNumber: 1 });
  });

  test('returns null for a malformed key', () => {
    expect(parseIdempotencyKey('not-a-valid-key')).toBeNull();
  });
});
