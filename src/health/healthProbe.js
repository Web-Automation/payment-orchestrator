import { env } from '../config/env.js';
import { recordProbeResult } from './asrEngine.js';

/**
 * Pings a single gateway's health/status endpoint and records latency + uptime.
 * Adapters expose a `healthCheckUrl` (or a `probe()` function for pure-mock
 * adapters that don't have a real HTTP endpoint).
 */
export async function probeGateway(redis, adapter, logger) {
  const start = performance.now();
  let up = false;
  let latencyMs = null;

  try {
    if (typeof adapter.probe === 'function') {
      await withTimeout(adapter.probe(), env.health.probeTimeoutMs);
    } else if (adapter.healthCheckUrl) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), env.health.probeTimeoutMs);
      const res = await fetch(adapter.healthCheckUrl, { signal: controller.signal });
      clearTimeout(timer);
      if (!res.ok) throw new Error(`probe returned HTTP ${res.status}`);
    }
    up = true;
    latencyMs = Math.round(performance.now() - start);
  } catch (err) {
    up = false;
    latencyMs = Math.round(performance.now() - start);
    logger?.warn({ gatewayId: adapter.id, err: err.message }, 'health probe failed');
  }

  await recordProbeResult(redis, adapter.id, { up, latencyMs });
  return { gatewayId: adapter.id, up, latencyMs };
}

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('probe timeout')), ms)),
  ]);
}

/**
 * Starts a background interval that probes every configured gateway adapter.
 * Returns a stop() function so callers (and tests) can clean it up.
 */
export function startHealthProbeLoop(redis, adapters, logger, intervalMs = env.health.probeIntervalMs) {
  const tick = async () => {
    await Promise.all(adapters.map((adapter) => probeGateway(redis, adapter, logger)));
  };

  // Fire once immediately so health state is populated before the first request.
  tick().catch((err) => logger?.error({ err: err.message }, 'initial health probe failed'));

  const handle = setInterval(() => {
    tick().catch((err) => logger?.error({ err: err.message }, 'health probe tick failed'));
  }, intervalMs);
  handle.unref?.();

  return function stop() {
    clearInterval(handle);
  };
}
