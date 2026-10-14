// Native, isolated JetStream. No Docker, external endpoints or production state.
const assert = require('node:assert/strict');
const test = require('node:test');
const { spawn } = require('node:child_process');
const { mkdtemp, rm } = require('node:fs/promises');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { connect, createTelemetry } = require('../dist');
const { hashOf } = require('../dist/utils');

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(read, accept, label, timeout = 10000) {
  const deadline = Date.now() + timeout;
  let value;
  while (Date.now() < deadline) {
    value = await read();
    if (accept(value)) return value;
    await sleep(20);
  }
  throw Error(`${label}: ${JSON.stringify(value)}`);
}
async function bounded(promise, label) {
  let timer;
  try { return await Promise.race([promise, new Promise((_, reject) => {
    timer = setTimeout(() => reject(Error(`${label} timed out`)), 10000);
  })]); } finally { clearTimeout(timer); }
}
async function fixture() {
  const socket = net.createServer();
  await new Promise((resolve, reject) => { socket.once('error', reject); socket.listen(0, '127.0.0.1', resolve); });
  const port = socket.address().port;
  await new Promise(resolve => socket.close(resolve));
  const directory = await mkdtemp(path.join(os.tmpdir(), 'w3a-sdk-retain-'));
  const server = spawn('nats-server', ['-js', '-a', '127.0.0.1', '-p', String(port), '-sd', directory], { stdio: ['ignore', 'pipe', 'pipe'] });
  let logs = '', spawnError;
  server.stderr.on('data', chunk => { logs += chunk; });
  server.on('error', error => { spawnError = error; });
  const exited = new Promise(resolve => server.on('close', resolve));
  const close = async () => { server.kill('SIGTERM'); await bounded(exited, 'NATS exit'); await rm(directory, { recursive: true }); };
  try { await until(() => { if (spawnError) throw spawnError; return logs; }, value => value.includes('Server is ready'), 'NATS startup'); }
  catch (error) { await close(); throw error; }
  const errors = [];
  const open = async () => connect({ servers: `nats://127.0.0.1:${port}`, workspaceName: 'test', appName: 'retention',
    telemetry: createTelemetry({ logLevel: 'error', logFormat: 'json' }).telemetry,
    options: { heartbeatInterval: 1000 }, uncaughtException: error => { errors.push(error); } });
  return { open, close, errors };
}

test('durable pending and backlog survive whole-client restart; actual unsubscribe removes them', { timeout: 60000 }, async () => {
  const f = await fixture(); let app, publisher, consumer;
  try {
    app = await f.open(); publisher = await app.client('source', {}, { detach: true });
    const triggerArgs = { name: 'events', tester: async input => input,
      runner: async ({ params, publish }) => async input => publish({ input, wallet: params.wallet }) };
    let trigger = await publisher.trigger(triggerArgs);
    const params = { wallet: 'a' }, key = hashOf(params), name = 'test_reader_events_' + key;
    const stream = 'trigger_test_source_events_events';
    const info = () => publisher.core.jsm.consumers.info(stream, name);
    let attempts = 0;
    consumer = await app.client('reader', {}, { detach: true });
    const subscription = await consumer.subscribe({ name: 'events', trigger: trigger.ref, params,
      callback: async () => { attempts++; throw Object.assign(Error('intentional pending delivery'), { redeliveryDelayMs: 2000 }); } });
    await trigger.executeForParams(1, [params]);
    await until(() => attempts, count => count > 0, 'first attempt');
    const original = await info();
    const statuses = publisher.core.nats.status();
    const reconnected = (async () => {
      for await (const status of statuses) if (status.type === 'reconnect') return;
      throw Error('connection closed without reconnect');
    })();
    await publisher.core.nats.reconnect();
    await bounded(reconnected, 'real reconnect');
    await publisher.core.nats.flush();
    assert.equal((await info()).created, original.created, 'reconnect must keep consumer identity');
    await subscription.suspend(); await bounded(consumer.destroy(), 'suspended client teardown'); consumer = undefined;
    const before = await info();
    assert.equal(before.num_ack_pending, 1);
    await trigger.executeForParams(2, [params]);
    await bounded(publisher.destroy(), 'publisher teardown'); publisher = undefined;
    await bounded(app.destroy(), 'app teardown'); app = undefined;

    app = await f.open(); publisher = await app.client('source', {}, { detach: true });
    trigger = await publisher.trigger(triggerArgs);
    const kept = await info();
    assert.equal(kept.created, before.created);
    assert.equal(kept.num_ack_pending, 1);
    assert.equal(kept.num_pending, 1);
    const received = [];
    consumer = await app.client('reader', {}, { detach: true });
    await consumer.subscribe({ name: 'events', trigger: trigger.ref, params, callback: async value => { received.push(value.input); } });
    await until(() => received, values => values.includes(1) && values.includes(2), 'retained pending/backlog redelivery');
    assert.equal((await info()).created, before.created);
    assert.equal((await publisher.core.jsm.consumers.list(stream).next()).length, 1);
    await until(info, value => value.num_ack_pending === 0 && value.num_pending === 0, 'all ACKed');
    await bounded(consumer.destroy(), 'actual unsubscribe'); consumer = undefined;
    await assert.rejects(info(), error => error.code === '404');
    assert.equal(await publisher.core.registry.get(`${trigger.ref.name}.tasks.${key}`), undefined);
    // A forced disconnect rejects in-flight heartbeat requests by contract.
    // Only that explicit transient error is allowed; delivery is checked above.
    for (const error of f.errors) {
      let root = error;
      while (root.cause) root = root.cause;
      assert.equal(root.code, 'TIMEOUT', `unexpected reconnect error: ${error}`);
    }
  } finally {
    try { if (consumer) await bounded(consumer.destroy(), 'consumer cleanup');
      if (publisher) await bounded(publisher.destroy(), 'publisher cleanup');
      if (app) await bounded(app.destroy(), 'app cleanup'); }
    finally { await f.close(); }
  }
});

test('repeated suspend/resume and immediate disconnect do not leak durable consumers', { timeout: 60000 }, async () => {
  const f = await fixture(); let app, publisher, consumer;
  try {
    app = await f.open(); publisher = await app.client('source', {}, { detach: true });
    const trigger = await publisher.trigger({ name: 'events', tester: async input => input,
      runner: async ({ publish }) => async input => publish({ input }) });
    const stream = 'trigger_test_source_events_events';
    let created;
    for (let cycle = 0; cycle < 20; cycle++) {
      consumer = await app.client('reader', {}, { detach: true });
      const sub = await consumer.subscribe({ name: 'events', trigger: trigger.ref, params: {}, callback: async () => {} });
      const infos = await publisher.core.jsm.consumers.list(stream).next();
      assert.equal(infos.length, 1);
      if (created) assert.equal(infos[0].created, created);
      created = infos[0].created;
      if (cycle < 19) await bounded(sub.suspend(), 'immediate suspension');
      await bounded(consumer.destroy(), 'immediate disconnect'); consumer = undefined;
    }
    assert.equal((await publisher.core.jsm.consumers.list(stream).next()).length, 0);
    assert.deepEqual(await publisher.core.registry.slice(`${trigger.ref.name}.tasks`).keys(), []);
    assert.deepEqual(f.errors, []);
  } finally {
    try { if (consumer) await bounded(consumer.destroy(), 'consumer cleanup');
      if (publisher) await bounded(publisher.destroy(), 'publisher cleanup');
      if (app) await bounded(app.destroy(), 'app cleanup'); }
    finally { await f.close(); }
  }
});

test('accepted parameter dispatch works after unsubscribe without creating demand', { timeout: 45000 }, async () => {
  const f = await fixture(); let app, publisher, consumer;
  try {
    app = await f.open(); publisher = await app.client('source', {}, { detach: true });
    const params = { wallet: 'a' }, key = hashOf(params);
    const trigger = await publisher.trigger({ name: 'events', tester: async input => input,
      runner: async ({ params, publish }) => async input => publish({ input, wallet: params.wallet }) });
    consumer = await app.client('reader', {}, { detach: true });
    await consumer.subscribe({ name: 'events', trigger: trigger.ref, params, callback: async () => {} });
    await consumer.destroy(); consumer = undefined;
    const tasks = publisher.core.registry.slice(`${trigger.ref.name}.tasks`);
    await until(() => tasks.keys(), keys => keys.length === 0, 'unsubscribed task');
    const published = [];
    const observer = publisher.core.nats.subscribe(`trigger.test.source.events.events.${key}`, {
      callback: (error, message) => { assert.ifError(error); published.push(publisher.core.decode(message.data)); },
    });
    await publisher.core.nats.flush();
    try { await trigger.executeForParams(7, [params, { ...params }]);
      await until(() => published, messages => messages.length > 0, 'accepted publish to original subject'); }
    finally { observer.unsubscribe(); }
    const stream = 'trigger_test_source_events_events';
    assert.deepEqual(published, [{ input: 7, wallet: 'a' }]);
    assert.deepEqual(await tasks.keys(), [], 'accepted dispatch must not reactivate demand');
    // Interest retention intentionally releases a message after actual removal
    // of the last durable consumer. Completing the send must not resurrect it.
    assert.equal((await publisher.core.jsm.streams.info(stream)).state.last_seq, 1);
    assert.equal((await publisher.core.jsm.consumers.list(stream).next()).length, 0);
    assert.deepEqual(f.errors, []);
  } finally {
    try { if (consumer) await bounded(consumer.destroy(), 'consumer cleanup');
      if (publisher) await bounded(publisher.destroy(), 'publisher cleanup');
      if (app) await bounded(app.destroy(), 'app cleanup'); }
    finally { await f.close(); }
  }
});
