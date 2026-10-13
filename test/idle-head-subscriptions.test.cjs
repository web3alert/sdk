const assert = require('node:assert/strict');
const test = require('node:test');
const { Blockchain } = require('../dist/client/local/blockchain/blockchain');

function telemetry() {
  const noop = () => {};
  return { child: telemetry, gauge: () => ({ set: noop }), debug: noop, destroy: noop };
}
async function harness(active = true) {
  const counts = { subscribe: 0, unsubscribe: 0, reads: 0, destroy: 0 };
  const background = [];
  let callback;
  let lastCallback;
  const backend = {
    init: async () => {},
    destroy: async () => { counts.destroy++; },
    subscribe: async cb => {
      counts.subscribe++;
      callback = cb;
      lastCallback = cb;
      return async () => { counts.unsubscribe++; callback = undefined; };
    },
    rpc: async () => { counts.reads++; return 'on-demand'; },
    setHeadSubscriptionActive: async active => { background.push(active); },
  };
  const blockchain = new Blockchain({ telemetry: telemetry(), specs: [{ name: 'test', spec: {} }],
    factory: async () => backend, options: { headSubscriptionActive: active, upstreamAcquireTimeout: 0 } });
  await blockchain.init();
  // Upstream initializes through Immortal's background spawn, without network.
  for (let i = 0; i < 10 && blockchain._upstreams.get('test').backend !== backend; i++) {
    await new Promise(resolve => setImmediate(resolve));
  }
  const seen = [];
  const listener = await blockchain.listen(async value => { seen.push(value); });
  return { blockchain, backend, counts, seen, background, tick: value => callback?.(value),
    lateTick: value => lastCallback?.(value), listener };
}

test('idle startup opens no head subscription and keeps on-demand backend available', async () => {
  const h = await harness(false);
  try {
    assert.equal(h.counts.subscribe, 0);
    assert.deepEqual(h.background, [false]);
    assert.equal(await (await h.blockchain.upstream()).rpc(), 'on-demand');
    h.tick(10);
    assert.deepEqual(h.seen, []);
    await h.blockchain.setHeadSubscriptionActive(true);
    h.tick(11);
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(h.seen, [11]);
    assert.equal(h.counts.subscribe, 1);
    assert.equal(h.background.at(-1), true);
  } finally { await h.listener.destroy(); await h.blockchain.destroy(); }
  assert.equal(h.counts.unsubscribe, 1);
  assert.equal(h.counts.destroy, 1);
});

test('pause/wake is idempotent, suppresses late heads, and does not destroy provider methods', async () => {
  const h = await harness();
  try {
    h.tick(10);
    await new Promise(resolve => setImmediate(resolve));
    await h.blockchain.setHeadSubscriptionActive(false);
    await h.blockchain.setHeadSubscriptionActive(false);
    assert.equal(h.counts.unsubscribe, 1);
    assert.equal(h.background.at(-1), false);
    assert.equal(h.counts.destroy, 0);
    h.lateTick(999);
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(h.seen, [10]);
    // Stale head timestamp must not prevent provider-only usage while paused.
    h.blockchain._upstreams.get('test')._timestamp = 0;
    assert.equal(await (await h.blockchain.upstream()).rpc(), 'on-demand');
    await Promise.all([h.blockchain.setHeadSubscriptionActive(true), h.blockchain.setHeadSubscriptionActive(true)]);
    assert.equal(h.counts.subscribe, 2);
    h.tick(11);
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(h.seen, [10, 11]);
  } finally { await h.listener.destroy(); await h.blockchain.destroy(); }
  assert.equal(h.counts.unsubscribe, 2);
});
