const assert = require('node:assert/strict');
const test = require('node:test');
const { Cron } = require('../dist/client/cron');
const at = Date.parse('2026-10-19T00:00:00.000Z');
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
function fixture(callback, catchUp = true, expression = '* * * * * *') {
  let state = { next: new Date(at).toISOString() };
  const warnings = [], delays = [];
  const cell = { mutate: async run => run(structuredClone(state), async value => { state = structuredClone(value); }) };
  const cron = new Cron({ name: 'fixture', expression, catchUp,
    telemetry: { warn: value => warnings.push(value) }, core: { mutex: () => ({ lock: run => run() }), registry: { cell: () => cell } }, callback });
  cron._state = cell; cron._mutex = { lock: run => run() };
  return { cron, warnings, delays, state: () => state, tick: now => cron._tick(now, delay => delays.push(delay)) };
}
test('catch-up commits each exact due identity, including downtime', async () => {
  const seen = [], f = fixture(async expected => { seen.push(expected); });
  for (let i = 0; i < 5; i++) await f.tick(at + 4500);
  assert.deepEqual(seen, [0, 1000, 2000, 3000, 4000].map(offset => at + offset));
  assert.equal(Date.parse(f.state().next), at + 5000);
});
test('legacy coalescing remains the default for other Cron callers', async () => {
  const seen = [], f = fixture(async expected => { seen.push(expected); }, false);
  await f.tick(at + 4500);
  assert.deepEqual(seen, [at]); assert.equal(Date.parse(f.state().next), at + 5000);
});
test('failed callback keeps its cursor and retries the same timestamp', async () => {
  let failed = true; const seen = [];
  const f = fixture(async expected => { seen.push(expected); if (failed) throw Error('maintenance timeout'); });
  await f.tick(at + 1000);
  assert.equal(Date.parse(f.state().next), at); assert.deepEqual(f.delays, [1000]);
  failed = false; await f.tick(at + 2000);
  assert.deepEqual(seen, [at, at]); assert.equal(Date.parse(f.state().next), at + 1000);
});
test('ordinary failure keeps a two-hour timer input without one-second RPC retries', async () => {
  let attempts = 0;
  const f = fixture(async () => { attempts++; throw Error('permanent provider failure'); }, true, '0 0 */2 * * *');
  await f.tick(at + 1000);
  assert.equal(attempts, 1); assert.equal(Date.parse(f.state().next), at);
  assert.deepEqual(f.delays, [2 * 60 * 60 * 1000]);
});
test('explicit transport retry delay does not commit the held timestamp', async () => {
  const f = fixture(async () => { throw Object.assign(Error('maintenance still active'), { redeliveryDelayMs: 1000 }); }, true, '0 0 */2 * * *');
  await f.tick(at + 1000);
  assert.equal(Date.parse(f.state().next), at); assert.deepEqual(f.delays, [1000]);
});
test('destroy aborts and waits for admitted callback without committing it', async () => {
  const entered = deferred(), release = deferred(); let callbackSignal;
  const f = fixture(async (_, signal) => { callbackSignal = signal; entered.resolve(); await release.promise; });
  await f.cron.init(); await entered.promise;
  let stopped = false; const stopping = f.cron.destroy().then(() => { stopped = true; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(callbackSignal.aborted, true); assert.equal(stopped, false);
  release.resolve(); await stopping;
  assert.equal(Date.parse(f.state().next), at); assert.equal(f.warnings.length, 0);
});
test('a replacement Cron resumes the original cursor after suspension', async () => {
  const seen = [], f = fixture(async expected => seen.push(expected));
  f.cron._closing = true; await f.tick(at + 3000);
  assert.deepEqual(seen, []); assert.equal(Date.parse(f.state().next), at);
  const next = fixture(async expected => seen.push(expected));
  next.cron._state = f.cron._state;
  for (let i = 0; i < 4; i++) await next.tick(at + 3000);
  assert.deepEqual(seen, [0, 1000, 2000, 3000].map(offset => at + offset));
});
