import RedisMock from 'ioredis-mock';
import { acquireLock, releaseLock, withLock, orderLockKey } from '../src/orchestrator/distributedLock.js';

describe('distributed lock', () => {
  let redis;

  beforeEach(() => {
    redis = new RedisMock();
  });

  afterEach(async () => {
    await redis.flushall();
  });

  test('acquireLock succeeds when no one holds the lock, and returns a token', async () => {
    const token = await acquireLock(redis, 'lock:order:ORD_1', 5000);
    expect(typeof token).toBe('string');
  });

  test('a second acquireLock call fails while the first holder still has it', async () => {
    const token1 = await acquireLock(redis, 'lock:order:ORD_1', 5000);
    const token2 = await acquireLock(redis, 'lock:order:ORD_1', 5000);
    expect(token1).not.toBeNull();
    expect(token2).toBeNull();
  });

  test('releaseLock only releases if the caller still holds the matching token (compare-and-delete)', async () => {
    const token = await acquireLock(redis, 'lock:order:ORD_1', 5000);
    const releasedWithWrongToken = await releaseLock(redis, 'lock:order:ORD_1', 'not-the-real-token');
    expect(releasedWithWrongToken).toBe(false);

    const releasedWithRealToken = await releaseLock(redis, 'lock:order:ORD_1', token);
    expect(releasedWithRealToken).toBe(true);

    // Now that it's released, someone else can acquire it.
    const token2 = await acquireLock(redis, 'lock:order:ORD_1', 5000);
    expect(token2).not.toBeNull();
  });

  test('withLock runs fn while holding the lock and always releases it afterward, even on success', async () => {
    const { acquired, result } = await withLock(redis, 'lock:order:ORD_1', 5000, async () => 'done');
    expect(acquired).toBe(true);
    expect(result).toBe('done');

    // Lock should be free again immediately after.
    const token = await acquireLock(redis, 'lock:order:ORD_1', 5000);
    expect(token).not.toBeNull();
  });

  test('withLock releases the lock even if fn throws', async () => {
    await expect(
      withLock(redis, 'lock:order:ORD_1', 5000, async () => {
        throw new Error('boom');
      })
    ).rejects.toThrow('boom');

    const token = await acquireLock(redis, 'lock:order:ORD_1', 5000);
    expect(token).not.toBeNull(); // proves the lock was released despite the throw
  });

  test('withLock reports acquired:false without running fn when the lock cannot be obtained', async () => {
    await acquireLock(redis, 'lock:order:ORD_1', 5000); // someone else holds it
    let callCount = 0;
    const fn = async () => { callCount += 1; };
    const { acquired, result } = await withLock(redis, 'lock:order:ORD_1', 5000, fn, { retries: 0 });
    expect(acquired).toBe(false);
    expect(result).toBeUndefined();
    expect(callCount).toBe(0);
  });

  test('orderLockKey produces a consistent, order-scoped key', () => {
    expect(orderLockKey('ORD_1001')).toBe('lock:order:ORD_1001');
  });
});
