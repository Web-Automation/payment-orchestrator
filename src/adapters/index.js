import { env } from '../config/env.js';
import { createMockGatewayAdapter } from './mockGatewayAdapter.js';
import { createHttpGatewayAdapter } from './httpGatewayAdapter.js';

/**
 * Builds the gatewayId -> adapter map used by the orchestrator.
 * For each configured gateway id, if a `GATEWAY_<ID>_URL` env var is set
 * (e.g. GATEWAY_GATEWAYA_URL=http://mock-pg-a:4001, as set by
 * docker-compose), an HTTP adapter is wired up against the standalone mock
 * PG server. Otherwise an in-process mock adapter is used, which is enough
 * to run `npm start` and exercise the full flow with zero extra services.
 */
export function buildAdapters(gatewayIds = env.gateways) {
  const adapters = {};
  for (const id of gatewayIds) {
    const envKey = `GATEWAY_${id.toUpperCase()}_URL`;
    const baseUrl = process.env[envKey];
    adapters[id] = baseUrl
      ? createHttpGatewayAdapter(id, { baseUrl, channels: ['UPI', 'CARD', 'NETBANKING'] })
      : createMockGatewayAdapter(id, { channels: ['UPI', 'CARD', 'NETBANKING'], latencyMs: 20 });
  }
  return adapters;
}
