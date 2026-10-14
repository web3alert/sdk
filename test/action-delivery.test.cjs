const test = require('node:test');
const assert = require('node:assert/strict');
const { ActionDeliveryFence, actionDeliveryErrorKind } = require('../dist');

function store() {
  const records = new Map();
  return { records, afterWrite: async () => {},
    get: async key => structuredClone(records.get(key)?.value),
    async mutate(key, callback) {
      for (;;) {
        const before = records.get(key), revision = before?.revision ?? 0;
        let conflict = false;
        try {
          return await callback(structuredClone(before?.value), async next => {
            if ((records.get(key)?.revision ?? 0) !== revision) { conflict = true; throw Error('conflict'); }
            records.set(key, { revision: revision + 1, value: structuredClone(next) });
            await this.afterWrite(next);
            return structuredClone(next);
          });
        } catch (error) { if (!conflict) throw error; }
      }
    } };
}
const deferred = () => { let resolve; const promise = new Promise(r => resolve = r); return { resolve, promise }; };
const cancelled = error => actionDeliveryErrorKind(error) === 'cancelled';
const unavailable = error => actionDeliveryErrorKind(error) === 'unavailable';

test('cancel stops queued sends; HTTP already started is not recalled and cannot retry', async () => {
  const db = store(), fence = new ActionDeliveryFence(db), ref = await fence.open('sub');
  const issued = deferred(), response = deferred(); let calls = 0;
  const sending = fence.start(ref, () => { calls++; issued.resolve(); return response.promise; });
  await issued.promise;
  // Release acknowledgement is asynchronous; cancellation may briefly be pending.
  for (;;) { try { await fence.cancel(ref); break; } catch (error) { assert.ok(unavailable(error)); await new Promise(setImmediate); } }
  await assert.rejects(fence.start(ref, async () => { calls++; }), cancelled);
  response.resolve('done'); assert.equal(await sending, 'done');
  await assert.rejects(fence.start(ref, async () => { calls++; }), cancelled);
  assert.equal(calls, 1);
});

test('cancel cannot confirm across an admitted request whose HTTP has not started yet', async () => {
  const db = store(), fence = new ActionDeliveryFence(db), ref = await fence.open('sub');
  const admitted = deferred(), ack = deferred(); let calls = 0;
  db.afterWrite = async value => { if (value.starting && !value.cancelled) { admitted.resolve(); await ack.promise; } };
  const sending = fence.start(ref, async () => { calls++; });
  const rejected = assert.rejects(sending, cancelled);
  await admitted.promise;
  await assert.rejects(fence.cancel(ref), unavailable);
  assert.equal(calls, 0, 'cancellation is pending, NOT falsely acknowledged');
  ack.resolve(); await rejected;
  await fence.cancel(ref);
  await assert.rejects(fence.start(ref, async () => { calls++; }), cancelled);
  assert.equal(calls, 0);
});

test('generation survives restart; old cancellation cannot revoke a new generation or another subscription', async () => {
  const db = store(), a = new ActionDeliveryFence(db), old = await a.open('one');
  const restarted = new ActionDeliveryFence(db);
  assert.deepEqual(await restarted.open('one'), old);
  await a.cancel(old);
  const fresh = await restarted.open('one'), other = await restarted.open('two');
  assert.notEqual(fresh.generation, old.generation);
  await a.cancel(old);
  await assert.rejects(a.start(old, async () => {}), cancelled);
  await restarted.start(fresh, async () => {});
  await restarted.start(other, async () => {});
  assert.equal(db.records.size, 2, 'bounded to one control record per subscription');
});

test('missing/failed control does not mean cancelled and cannot issue HTTP', async () => {
  let calls = 0;
  const db = store(), fence = new ActionDeliveryFence(db);
  await assert.rejects(fence.start({ key: 'unknown', generation: 'x' }, async () => { calls++; }), unavailable);
  db.mutate = async () => { throw Error('offline'); };
  await assert.rejects(fence.start({ key: 'unknown', generation: 'x' }, async () => { calls++; }), unavailable);
  assert.equal(calls, 0);
});

test('lost admission/release ACKs cannot orphan a token or repeat an HTTP', async () => {
  const db = store(), fence = new ActionDeliveryFence(db), ref = await fence.open('sub');
  let lose = 'admission', calls = 0;
  db.afterWrite = async state => {
    if ((lose === 'admission' && state.starting) || (lose === 'release' && !state.starting)) {
      lose = ''; throw Error('ACK lost after durable write');
    }
  };
  await assert.rejects(fence.start(ref, async () => { calls++; }), unavailable);
  assert.equal((await db.get(ref.key)).starting, undefined);
  assert.equal(calls, 0);
  lose = 'release';
  await fence.start(ref, async () => { calls++; });
  await fence.cancel(ref);
  assert.equal(calls, 1);
  assert.equal((await db.get(ref.key)).starting, undefined);
});
