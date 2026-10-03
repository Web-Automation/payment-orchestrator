import http from 'node:http';

/**
 * Standalone mock Payment Gateway HTTP server, used by docker-compose to
 * give the orchestrator real network calls to fail over across (instead of
 * the in-process mock adapter used by default in `npm start`/tests).
 * Implements the same redirect-flow contract as adapters/httpGatewayAdapter.js:
 *   POST /initiate         -> { status: 'INITIATED'|'PROVIDER_ERROR'|'USER_ERROR', redirectUrl? }
 *   GET  /status/:idemKey  -> { status: 'SUCCESS'|'FAILED'|'PENDING'|'NOT_FOUND' }
 *   POST /refund           -> { refundId }
 *   GET  /health           -> 200/503
 *
 * Configurable via env vars so docker-compose can spin up several instances
 * in different simulated states (healthy / degraded / down):
 *   PORT, GATEWAY_ID
 *   FAILURE_RATE          - 0..1 chance /initiate returns PROVIDER_ERROR
 *   USER_ERROR_RATE       - 0..1 chance /initiate returns USER_ERROR
 *   GHOST_ACCEPT_ON_TIMEOUT - "true": a "failed" initiate still creates a session server-side
 *   SETTLE_DELAY_MS       - how long after initiate a session settles (default 0)
 *   SETTLE_OUTCOME        - what it settles to: SUCCESS | FAILED (default SUCCESS)
 *   LATENCY_MS, FORCE_DOWN
 *
 * A live control endpoint lets tests/demos flip a running container's
 * behavior without restarting it: POST /control { ...same fields... }
 */

const state = {
  gatewayId: process.env.GATEWAY_ID || 'mock-pg',
  failureRate: Number(process.env.FAILURE_RATE ?? 0),
  userErrorRate: Number(process.env.USER_ERROR_RATE ?? 0),
  ghostAcceptOnTimeout: process.env.GHOST_ACCEPT_ON_TIMEOUT === 'true',
  settleDelayMs: Number(process.env.SETTLE_DELAY_MS ?? 0),
  settleOutcome: process.env.SETTLE_OUTCOME || 'SUCCESS',
  latencyMs: Number(process.env.LATENCY_MS ?? 20),
  forceDown: process.env.FORCE_DOWN === 'true',
};

const sessions = new Map(); // idempotencyKey -> { status, settleAtMs, settleOutcome, refunded }
const port = Number(process.env.PORT || 4000);

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => { data += chunk; });
    req.on('end', () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res, status, body) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

function createSession(idempotencyKey) {
  const now = Date.now();
  sessions.set(idempotencyKey, {
    status: state.settleDelayMs > 0 ? 'PENDING' : state.settleOutcome,
    settleAtMs: now + state.settleDelayMs,
    settleOutcome: state.settleOutcome,
    refunded: false,
  });
}

function resolveSession(idempotencyKey) {
  const session = sessions.get(idempotencyKey);
  if (!session) return null;
  if (session.status === 'PENDING' && Date.now() >= session.settleAtMs) {
    session.status = session.settleOutcome;
  }
  return session;
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);

    if (req.method === 'GET' && url.pathname === '/health') {
      await delay(state.latencyMs);
      if (state.forceDown) return sendJson(res, 503, { status: 'down', gatewayId: state.gatewayId });
      return sendJson(res, 200, { status: 'ok', gatewayId: state.gatewayId, latencyMs: state.latencyMs });
    }

    if (req.method === 'POST' && url.pathname === '/initiate') {
      await delay(state.latencyMs);
      const body = await readBody(req);
      const { idempotencyKey } = body;

      if (state.forceDown) {
        return sendJson(res, 503, { status: 'PROVIDER_ERROR', gatewayId: state.gatewayId });
      }
      const roll = Math.random();
      if (roll < state.failureRate) {
        if (state.ghostAcceptOnTimeout) createSession(idempotencyKey);
        return sendJson(res, 200, { status: 'PROVIDER_ERROR', gatewayId: state.gatewayId });
      }
      if (roll < state.failureRate + state.userErrorRate) {
        return sendJson(res, 200, { status: 'USER_ERROR', gatewayId: state.gatewayId });
      }
      createSession(idempotencyKey);
      return sendJson(res, 200, {
        status: 'INITIATED',
        gatewayId: state.gatewayId,
        redirectUrl: `https://${state.gatewayId}.mock-pg.local/checkout/${idempotencyKey}`,
      });
    }

    if (req.method === 'GET' && url.pathname.startsWith('/status/')) {
      const idempotencyKey = decodeURIComponent(url.pathname.slice('/status/'.length));
      const session = resolveSession(idempotencyKey);
      if (!session) return sendJson(res, 200, { status: 'NOT_FOUND' });
      return sendJson(res, 200, { status: session.status });
    }

    if (req.method === 'POST' && url.pathname === '/refund') {
      const body = await readBody(req);
      const session = sessions.get(body.idempotencyKey);
      if (!session || session.status !== 'SUCCESS') {
        return sendJson(res, 200, { ok: false, reason: 'nothing_to_refund' });
      }
      session.refunded = true;
      return sendJson(res, 200, { ok: true, refundId: `refund_${body.idempotencyKey}` });
    }

    if (req.method === 'POST' && url.pathname === '/control') {
      const body = await readBody(req);
      const { forceSettleKey, forceSettleOutcome, ...rest } = body;
      Object.assign(state, rest);
      if (forceSettleKey) {
        const session = sessions.get(forceSettleKey);
        if (session) {
          session.status = forceSettleOutcome || session.settleOutcome;
          session.settleAtMs = Date.now();
        }
      }
      return sendJson(res, 200, { ok: true, state });
    }
    if (req.method === 'GET' && url.pathname === '/control') {
      return sendJson(res, 200, { state, sessionCount: sessions.size });
    }

    sendJson(res, 404, { error: 'not_found' });
  } catch (err) {
    sendJson(res, 400, { error: err.message });
  }
});

server.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`[mock-pg:${state.gatewayId}] listening on :${port} (failureRate=${state.failureRate}, latencyMs=${state.latencyMs})`);
});
