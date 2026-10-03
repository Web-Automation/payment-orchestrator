import { randomUUID } from 'node:crypto';

/**
 * Minimal Redis distributed lock (single-instance SET NX PX pattern).
 * Used to guard the critical sections where two code paths could otherwise
 * race on the same order: pre-failover verification (checking whether a
 * "timed out" gateway actually succeeded before cascading) and webhook
 * reconciliation (detecting a double-capture). A single-instance lock is
 * sufficient here — this isn't guarding a bank transfer, it's serializing
 * two internal workers so they don't both act on the same order at once.
 * Swap for Redlock across a Redis cluster if that guarantee is ever needed.
 */

const RELEASE_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
`;

/**
 * Attempts to acquire the lock, returning a token to release it with, or
 * null if someone else already holds it.
 */
export async function acquireLock(redis, key, ttlMs) {
  const token = randomUUID();
  const result = await redis.set(key, token, 'PX', ttlMs, 'NX');
  return result === 'OK' ? token : null;
}

/** Releases the lock only if this caller still holds it (compare-and-delete). */
export async function releaseLock(redis, key, token) {
  const result = await redis.eval(RELEASE_SCRIPT, 1, key, token);
  return result === 1;
}

/**
 * Runs `fn` while holding the lock on `key`, always releasing afterward.
 * Returns { acquired: false } immediately (without running fn) if the lock
 * couldn't be acquired within the retry budget — callers decide what that
 * means for them (e.g. treat as "someone else is already handling this").
 */
export async function withLock(redis, key, ttlMs, fn, { retries = 3, retryDelayMs = 50 } = {}) {
  let token = null;
  for (let attempt = 0; attempt <= retries && !token; attempt += 1) {
    // eslint-disable-next-line no-await-in-loop
    token = await acquireLock(redis, key, ttlMs);
    if (!token && attempt < retries) {
      // eslint-disable-next-line no-await-in-loop
      await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
    }
  }
  if (!token) {
    return { acquired: false, result: undefined };
  }
  try {
    const result = await fn();
    return { acquired: true, result };
  } finally {
    await releaseLock(redis, key, token);
  }
}

export const orderLockKey = (orderId) => `lock:order:${orderId}`;
