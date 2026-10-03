/**
 * Provider-agnostic adapter interface every gateway integration must
 * implement, modeling a redirect/hosted-checkout-page flow rather than a
 * synchronous server-to-server charge:
 *   - id: string
 *   - channels: string[]
 *   - async initiate(request) -> { status: 'INITIATED'|'PROVIDER_ERROR'|'USER_ERROR', redirectUrl?, raw }
 *       'INITIATED' means the gateway accepted the session and handed back a
 *       redirect URL — NOT that the payment has settled. The user completes
 *       payment on the gateway's own page; final settlement arrives later
 *       via webhook.
 *   - async checkStatus(idempotencyKey) -> { status: 'SUCCESS'|'FAILED'|'PENDING'|'NOT_FOUND' }
 *       Out-of-band status query, used for pre-failover verification: before
 *       cascading away from a gateway that just timed out, ask it directly
 *       whether it actually processed the attempt.
 *   - async refund(idempotencyKey, amount) -> { ok, refundId }
 *       Used by the reconciliation worker to reverse a late-settling
 *       duplicate capture.
 *   - async probe() -> void (throws if the health endpoint is down)
 *
 * This in-process mock is used for local dev / tests without needing the
 * standalone HTTP mock-servers from docker-compose. Every failure mode
 * described in the real architecture is reachable via the setters below:
 *   - setForceDown(true)              -> initiate()/probe() always fail outright, no session created
 *   - setFailureRate(rate)            -> fraction of initiate() calls return PROVIDER_ERROR
 *   - setGhostAcceptOnTimeout(true)   -> a "failed" initiate() call still secretly creates a session
 *                                        server-side (simulates "the gateway actually got it, the
 *                                        response to us just got lost") — this is what pre-failover
 *                                        verification exists to catch
 *   - setSettleDelayMs(ms)            -> how long after initiate() a session takes to settle
 *   - setSettleOutcome(outcome)       -> what a session settles to ('SUCCESS' | 'FAILED')
 */
export function createMockGatewayAdapter(id, options = {}) {
  const state = {
    channels: options.channels ?? ['UPI', 'CARD', 'NETBANKING'],
    latencyMs: options.latencyMs ?? 5,
    failureRate: options.failureRate ?? 0, // 0..1 chance initiate() returns PROVIDER_ERROR
    userErrorRate: options.userErrorRate ?? 0, // 0..1 chance initiate() returns USER_ERROR
    forceDown: options.forceDown ?? false,
    ghostAcceptOnTimeout: options.ghostAcceptOnTimeout ?? false,
    settleDelayMs: options.settleDelayMs ?? 0,
    settleOutcome: options.settleOutcome ?? 'SUCCESS',
    rng: options.rng ?? Math.random,
  };

  /** idempotencyKey -> { status, createdAt, settleAtMs, refunded } */
  const sessions = new Map();

  async function simulateLatency() {
    if (state.latencyMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, state.latencyMs));
    }
  }

  function createSession(idempotencyKey) {
    const now = Date.now();
    sessions.set(idempotencyKey, {
      status: state.settleDelayMs > 0 ? 'PENDING' : state.settleOutcome,
      createdAt: now,
      settleAtMs: now + state.settleDelayMs,
      settleOutcome: state.settleOutcome,
      refunded: false,
    });
  }

  return {
    id,
    get channels() {
      return state.channels;
    },

    // --- test/ops control hooks ---
    setForceDown(value) { state.forceDown = value; },
    setFailureRate(value) { state.failureRate = value; },
    setUserErrorRate(value) { state.userErrorRate = value; },
    setLatencyMs(value) { state.latencyMs = value; },
    setGhostAcceptOnTimeout(value) { state.ghostAcceptOnTimeout = value; },
    setSettleDelayMs(value) { state.settleDelayMs = value; },
    setSettleOutcome(value) { state.settleOutcome = value; },
    /** Test hook: directly flip a session's settlement outcome (simulates an async webhook trigger source). */
    forceSettle(idempotencyKey, outcome) {
      const session = sessions.get(idempotencyKey);
      if (session) {
        session.status = outcome;
        session.settleAtMs = Date.now();
      }
    },
    getSession(idempotencyKey) {
      return sessions.get(idempotencyKey) ?? null;
    },

    async initiate(request) {
      await simulateLatency();
      const { idempotencyKey } = request;

      if (state.forceDown) {
        return { status: 'PROVIDER_ERROR', gatewayId: id, raw: { error: 'gateway_down' } };
      }

      const roll = state.rng();
      if (roll < state.failureRate) {
        if (state.ghostAcceptOnTimeout) {
          // The gateway actually accepted it — the response back to us was
          // what got lost. This is precisely the scenario pre-failover
          // verification exists to catch before blindly cascading.
          createSession(idempotencyKey);
        }
        return { status: 'PROVIDER_ERROR', gatewayId: id, raw: { error: 'timeout_or_5xx' } };
      }
      if (roll < state.failureRate + state.userErrorRate) {
        return { status: 'USER_ERROR', gatewayId: id, raw: { error: 'invalid_instrument' } };
      }

      createSession(idempotencyKey);
      return {
        status: 'INITIATED',
        gatewayId: id,
        redirectUrl: `https://${id}.mock-pg.example/checkout/${idempotencyKey}`,
        raw: { idempotencyKey },
      };
    },

    async checkStatus(idempotencyKey) {
      await simulateLatency();
      const session = sessions.get(idempotencyKey);
      if (!session) return { status: 'NOT_FOUND' };
      if (session.status === 'PENDING' && Date.now() >= session.settleAtMs) {
        session.status = session.settleOutcome;
      }
      return { status: session.status };
    },

    async refund(idempotencyKey) {
      const session = sessions.get(idempotencyKey);
      if (!session || session.status !== 'SUCCESS') {
        return { ok: false, reason: 'nothing_to_refund' };
      }
      session.refunded = true;
      return { ok: true, refundId: `refund_${idempotencyKey}` };
    },

    async probe() {
      await simulateLatency();
      if (state.forceDown) {
        throw new Error(`${id} probe: gateway unreachable`);
      }
    },
  };
}
