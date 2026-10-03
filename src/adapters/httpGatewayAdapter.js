/**
 * HTTP-based adapter for talking to a real (or standalone mock) PG server
 * over the network — same initiate/checkStatus/refund/probe interface as
 * mockGatewayAdapter.js so the orchestrator never needs to know which
 * implementation it's holding.
 */
export function createHttpGatewayAdapter(id, { baseUrl, channels, timeoutMs = 5000 }) {
  async function post(path, body) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(`${baseUrl}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      clearTimeout(timer);
      return res;
    } catch (err) {
      clearTimeout(timer);
      throw err;
    }
  }

  return {
    id,
    channels,
    healthCheckUrl: `${baseUrl}/health`,

    async initiate(request) {
      try {
        const res = await post('/initiate', request);
        if (res.status >= 500) {
          return { status: 'PROVIDER_ERROR', gatewayId: id, raw: { httpStatus: res.status } };
        }
        const body = await res.json();
        return { status: body.status ?? 'PROVIDER_ERROR', gatewayId: id, redirectUrl: body.redirectUrl, raw: body };
      } catch (err) {
        // Network error / timeout / abort -> always a provider error, never surfaced raw to checkout.
        return { status: 'PROVIDER_ERROR', gatewayId: id, raw: { error: err.message } };
      }
    },

    async checkStatus(idempotencyKey) {
      try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        const res = await fetch(`${baseUrl}/status/${encodeURIComponent(idempotencyKey)}`, {
          signal: controller.signal,
        });
        clearTimeout(timer);
        if (!res.ok) return { status: 'NOT_FOUND' };
        const body = await res.json();
        return { status: body.status ?? 'NOT_FOUND' };
      } catch {
        return { status: 'NOT_FOUND' };
      }
    },

    async refund(idempotencyKey, amount) {
      try {
        const res = await post('/refund', { idempotencyKey, amount });
        if (!res.ok) return { ok: false };
        const body = await res.json();
        return { ok: true, refundId: body.refundId };
      } catch (err) {
        return { ok: false, reason: err.message };
      }
    },

    async probe() {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const res = await fetch(`${baseUrl}/health`, { signal: controller.signal });
        clearTimeout(timer);
        if (!res.ok) throw new Error(`health endpoint returned ${res.status}`);
      } catch (err) {
        clearTimeout(timer);
        throw err;
      }
    },
  };
}
