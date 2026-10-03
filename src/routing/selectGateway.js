import { env } from '../config/env.js';
import { getAllGatewaysHealth, HealthStatus } from '../health/asrEngine.js';
import { gatewaysSupportingChannel, computeCost, getCostConfig } from './costConfig.js';
import { getCurrentTps, getMaxTps, isAtCapacity } from './capacity.js';

/** Typed result so callers never have to catch a routing exception. */
export function noHealthyGatewayResult(channel) {
  return {
    ok: false,
    reason: 'NO_HEALTHY_GATEWAY',
    message: `No healthy gateway available for channel ${channel}`,
    channel,
    candidates: [],
  };
}

/**
 * Selects gateways for a channel + amount:
 *   1. filter to gateways that support the channel
 *   2. filter to gateways with ASR >= threshold and a reachable live probe
 *   3. rank remaining candidates by lowest computed cost
 *   4. push any candidate currently at/over its configured TPS capacity
 *      cap to the back of the list — still a valid fallback, just not the
 *      first choice, so a 0%-MDR primary gets a safe throughput buffer
 *      instead of taking traffic that would collapse it
 *
 * Returns a typed { ok, candidates } result — never throws for "no healthy
 * gateway" scenarios, so the caller (orchestrator) can handle it gracefully.
 * `candidates` is the full fallback-ordered list (primary + ranked
 * alternates) that feeds directly into the failover cascade.
 */
export async function selectGateway(redis, channel, amount, opts = {}) {
  const config = opts.costConfig ?? getCostConfig();
  const gatewayIds = opts.gatewayIds ?? Object.keys(config);
  const maxTpsByGateway = opts.maxTpsByGateway ?? env.routing.maxTpsByGateway;

  const channelGateways = gatewaysSupportingChannel(channel, config).filter((id) =>
    gatewayIds.includes(id)
  );

  if (channelGateways.length === 0) {
    return noHealthyGatewayResult(channel);
  }

  const healthResults = await getAllGatewaysHealth(redis, channelGateways);
  const healthById = Object.fromEntries(healthResults.map((h) => [h.gatewayId, h]));

  const eligibleIds = channelGateways.filter((id) => healthById[id].status === HealthStatus.HEALTHY);

  if (eligibleIds.length === 0) {
    return noHealthyGatewayResult(channel);
  }

  const withCost = eligibleIds
    .map((id) => ({
      gatewayId: id,
      cost: computeCost(id, channel, amount, config),
      status: healthById[id].status,
      asr: healthById[id].asr,
    }))
    .sort((a, b) => a.cost - b.cost);

  // Capacity check happens after cost sorting, on the current TPS snapshot —
  // read-only here; the orchestrator increments the counter when it actually
  // dispatches an attempt (see orchestrator/processPayment.js).
  const capacityFlags = await Promise.all(
    withCost.map(async (c) => {
      const maxTps = getMaxTps(c.gatewayId, maxTpsByGateway);
      const currentTps = maxTps > 0 ? await getCurrentTps(redis, c.gatewayId) : 0;
      return { ...c, maxTps, currentTps, atCapacity: isAtCapacity(currentTps, maxTps) };
    })
  );

  const underCapacity = capacityFlags.filter((c) => !c.atCapacity);
  const overCapacity = capacityFlags.filter((c) => c.atCapacity);
  const candidates = [...underCapacity, ...overCapacity];

  return {
    ok: true,
    channel,
    amount,
    candidates, // primary = candidates[0], ranked alternates follow
  };
}
