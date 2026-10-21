const assert = require('node:assert/strict');
const test = require('node:test');
const { StreamSubscription } = require('../dist/stream');
const { SubscriptionImpl } = require('../dist/client/subscription/subscription');
const { TriggerImpl } = require('../dist/client/trigger/trigger');
const { hashOf } = require('../dist/utils');

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
function telemetry() {
  const noop = () => {};
  return { wrap: (_, fn) => fn, child: telemetry, gauge: () => ({ set: noop }),
    warn: noop, error: noop, debug: noop, info: noop, destroy: noop };
}

test('expired task retention checks exact durable subscriber and subject, failing closed on broker errors',async()=>{
  let consumers=[];
  const trigger=new TriggerImpl({telemetry:telemetry(),name:'trigger',core:{jsm:{consumers:{list:()=>({
    async *[Symbol.asyncIterator](){for(const value of consumers){if(value instanceof Error)throw value;yield value}}
  })}}},runner:()=>{},tester:()=>{}});
  trigger._stream={ref:{stream:'stream',subject:'trigger.events'}};
  const c=(name,filter='trigger.events.key')=>({config:{durable_name:name,filter_subject:filter}});
  consumers=[c('reader_key')];assert.equal(await trigger._hasDurableSubscriber('key','reader'),true);
  assert.equal(await trigger._hasDurableSubscriber('key','other'),false);
  consumers=[c('reader_key','trigger.events.other')];assert.equal(await trigger._hasDurableSubscriber('key','reader'),false);
  consumers=[c('reader_key','trigger.events.*')];assert.equal(await trigger._hasDurableSubscriber('key','reader'),true);
  consumers=[c('reader_key','trigger.>')];assert.equal(await trigger._hasDurableSubscriber('key','reader'),true);
  consumers=[{config:{durable_name:'reader_key',filter_subjects:['unrelated','trigger.events.key']}}];
  assert.equal(await trigger._hasDurableSubscriber('key','reader'),true);
  consumers=[];assert.equal(await trigger._hasDurableSubscriber('key','reader'),false);
  consumers=[new Error('broker unavailable')];await assert.rejects(trigger._hasDurableSubscriber('key','reader'),/broker unavailable/);
});

test('shutdown consumes the NATS stop sentinel behind buffered items without dispatching them', { timeout: 2000 }, async () => {
  const stopped = deferred(), closed = deferred(), first = deferred();
  let dispatched = 0;
  const messages = {
    // Real nats 2.x stop queues completion in its iterator, rather than
    // resolving closed immediately. Returning early strands that completion.
    stop() { stopped.resolve(); },
    closed: () => closed.promise,
    async *[Symbol.asyncIterator]() {
      yield {};
      await stopped.promise;
      yield {}; // already buffered when suspension starts
      closed.resolve();
    },
  };
  const sub = new StreamSubscription({ telemetry: telemetry(), core: {
    jsm: { consumers: { add: async () => ({ stream_name: 'stream', name: 'consumer' }) } },
    js: { consumers: { get: async () => ({ fetch: async () => messages }) } },
    uncaughtException: error => { throw error; },
  }, name: 'consumer', ref: { stream: 'stream', subject: 'events' }, callback: async () => {} });
  sub._handleMessage = async () => { dispatched++; first.resolve(); };
  await sub.init(); await first.promise;
  await sub.suspend();
  assert.equal(dispatched, 1, 'buffered tail must remain unacknowledged for redelivery');
});

test('shutdown stops an iterator whose fetch resolves after shutdown began', { timeout: 2000 }, async () => {
  const fetched = deferred(), started = deferred(), closed = deferred();
  let stopped = 0, deleted = 0;
  const messages = {
    stop() { stopped++; closed.resolve(); },
    closed: () => closed.promise,
    async *[Symbol.asyncIterator]() { await closed.promise; },
  };
  const core = {
    jsm: { consumers: { add: async () => ({ stream_name: 'stream', name: 'consumer' }),
      delete: async () => { deleted++; } } },
    js: { consumers: { get: async () => ({ fetch: async () => {
      started.resolve(); return await fetched.promise;
    } }) } },
    uncaughtException: err => { throw err; },
  };
  const sub = new StreamSubscription({ telemetry: telemetry(), core, name: 'consumer',
    ref: { stream: 'stream', subject: 'events' }, callback: async () => {} });
  await sub.init(); await started.promise;
  const closing = sub.suspend();
  fetched.resolve(messages);
  await closing; await sub.destroy(); await sub.suspend();
  assert.equal(stopped, 1);
  assert.equal(deleted, 0, 'container teardown must not delete a suspended consumer');
});

test('subscription suspension is final/idempotent; default removal still unsubscribes', async () => {
  for (const retain of [true, false]) {
    const counts = { service: 0, timer: 0, unsubscribe: 0, suspend: 0, destroy: 0 };
    const sub = new SubscriptionImpl({ telemetry: telemetry(), core: {
      call: async () => { counts.unsubscribe++; },
    }, name: 'consumer', trigger: { name: 'trigger' }, params: {}, callback: async () => {} });
    sub._service = { destroy: async () => { counts.service++; } };
    sub._timer = { destroy: async () => { counts.timer++; } };
    sub._subscriptionKey = 'key';
    sub._subscription = { suspend: async () => { counts.suspend++; }, destroy: async () => { counts.destroy++; } };
    await Promise.all([retain ? sub.suspend() : sub.destroy(), sub.destroy()]);
    assert.deepEqual(counts, { service: 1, timer: 1, unsubscribe: retain ? 0 : 1,
      suspend: retain ? 1 : 0, destroy: retain ? 0 : 1 });
  }
});

test('failed durable consumer removal remains retryable', async () => {
  let calls = 0;
  const sub = new StreamSubscription({ telemetry: telemetry(), name: 'consumer',
    ref: { stream: 'stream', subject: 'events' }, callback: async () => {}, core: {
      jsm: { consumers: { delete: async () => { if (++calls === 1) throw Error('temporary API failure'); } } },
    } });
  sub._info = { stream_name: 'stream', name: 'consumer' };
  await assert.rejects(sub.destroy(), /temporary API failure/);
  await sub.destroy(); await sub.destroy();
  assert.equal(calls, 2);
});

test('stream restoration retains the old durable consumer instead of deleting pending work', async () => {
  const streams = [];
  let callbacks;
  const sub = new SubscriptionImpl({ telemetry: telemetry(), name: 'consumer',
    trigger: { name: 'trigger' }, params: {}, callback: async () => {}, core: {
      options: { heartbeatInterval: 1000 },
      call: async () => ({ stream: { stream: 'stream', subject: 'events' }, key: 'same-key' }),
      stream: { subscribe: async (_telemetry, _name, _ref, _callback, options) => {
        const stream = { suspended: 0, deleted: 0,
          async suspend() { this.suspended++; }, async destroy() { this.deleted++; } };
        streams.push(stream); callbacks = options;
        return stream;
      } },
      localTimer: async () => ({ destroy: async () => {} }),
      service: async () => ({ method: async () => {}, destroy: async () => {} }),
    } });
  await sub.init();
  callbacks.error(Error('iterator interrupted'));
  await sub._restore;
  assert.equal(streams.length, 2);
  assert.equal(streams[0].suspended, 1);
  assert.equal(streams[0].deleted, 0);
  assert.equal(sub._subscriptionBroken, false);
  await sub.destroy();
  assert.equal(streams[1].deleted, 1);
});

test('explicit parameter dispatch deduplicates subjects and propagates publish failure', async () => {
  const published = [], states = new Map();
  let fail = true;
  const trigger = new TriggerImpl({ telemetry: telemetry(), name: 'trigger', core: {
    mutex: () => ({ lock: async callback => callback() }),
    registry: { cell: key => ({ mutate: async callback => callback(states.get(key), async value => { states.set(key, value); }) }) },
  }, runner: async ({ params, reduce, publish }) => async input => {
    await reduce(async (previous = 0, write) => { await write(previous + 1); });
    await publish({ input, params });
  }, tester: async value => value });
  trigger._stream = { publishToSubject: async (key, value) => {
    if (fail) throw Error('publish rejected');
    published.push({ key, value });
  } };
  const params = [{ wallet: 'a' }, { wallet: 'a' }, { wallet: 'b' }];
  await assert.rejects(trigger.executeForParams(1, params), /publish rejected/);
  fail = false;
  await trigger.executeForParams(1, params);
  assert.deepEqual(published.map(item => item.key).sort(), [hashOf(params[0]), hashOf(params[2])].sort());
  assert.equal(states.size, 2);
  await trigger.executeForParams(2, []);
  assert.equal(published.length, 2);
});
