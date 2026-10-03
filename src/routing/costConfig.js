/**
 * Routing/cost config: maps each (gateway, channel) pair to an MDR
 * (percentage) or flat fee. This is intentionally a plain in-memory object
 * with load/update functions so it can be swapped for a DB-backed store
 * later without touching selectGateway(). POST /v1/gateways/config updates
 * this at runtime — no redeploy required.
 */

export const Channel = Object.freeze({
  UPI: 'UPI',
  CARD: 'CARD',
  NETBANKING: 'NETBANKING',
});

const defaultConfig = {
  gatewayA: {
    UPI: { type: 'percentage', value: 0.002 }, // 0.2%
    CARD: { type: 'percentage', value: 0.019 }, // 1.9%
    NETBANKING: { type: 'flat', value: 8 },
  },
  gatewayB: {
    UPI: { type: 'percentage', value: 0.0015 },
    CARD: { type: 'percentage', value: 0.021 },
    NETBANKING: { type: 'flat', value: 10 },
  },
  gatewayC: {
    UPI: { type: 'percentage', value: 0.0025 },
    CARD: { type: 'percentage', value: 0.017 },
    NETBANKING: { type: 'flat', value: 6 },
  },
};

let currentConfig = structuredClone(defaultConfig);

export function getCostConfig() {
  return currentConfig;
}

/** Replaces the whole config, or merges a partial update per gateway. */
export function updateCostConfig(partialConfig, { merge = true } = {}) {
  if (!merge) {
    currentConfig = structuredClone(partialConfig);
    return currentConfig;
  }
  for (const [gatewayId, channelRates] of Object.entries(partialConfig)) {
    currentConfig[gatewayId] = { ...(currentConfig[gatewayId] || {}), ...channelRates };
  }
  return currentConfig;
}

export function resetCostConfig() {
  currentConfig = structuredClone(defaultConfig);
  return currentConfig;
}

/** Gateways that have a rate configured for the given channel. */
export function gatewaysSupportingChannel(channel, config = currentConfig) {
  return Object.entries(config)
    .filter(([, channels]) => channels[channel] !== undefined)
    .map(([gatewayId]) => gatewayId);
}

/** Computes the fee for a given gateway/channel/amount using the cost config. */
export function computeCost(gatewayId, channel, amount, config = currentConfig) {
  const rate = config[gatewayId]?.[channel];
  if (!rate) return null;
  if (rate.type === 'percentage') return Number((amount * rate.value).toFixed(4));
  if (rate.type === 'flat') return rate.value;
  throw new Error(`Unknown rate type: ${rate.type}`);
}
