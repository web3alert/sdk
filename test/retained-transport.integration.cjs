// Native, isolated JetStream. No Docker, external endpoints or production state.
const assert = require('node:assert/strict');
const test = require('node:test');
const { spawn } = require('node:child_process');
const { mkdtemp, rm } = require('node:fs/promises');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { connect, createTelemetry } = require(process.env.W3A_TASK_BASELINE_ROOT || '../dist');
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

test('expired heartbeat retains source tasks while their durable consumer survives', { timeout: 45000 }, async()=>{
  const f=await fixture();let app,publisher,reader;
  try {
    app=await f.open();publisher=await app.client('source',{}, {detach:true});
    const trigger=await publisher.trigger({name:'lease-events',tester:async x=>x,
      runner:async({publish})=>async input=>publish({input})});
    reader=await app.client('reader',{}, {detach:true});
    const sub=await reader.subscribe({name:'lease',trigger:trigger.ref,params:{},callback:async()=>{}});
    await sub.suspend();await reader.destroy();reader=undefined;
    // More than two cleanup windows and the real four-heartbeat expiry.
    // No subscriber heartbeat, no fake demand, no manual renewal or ACK.
    for(let i=0;i<9;i++){await trigger.execute(i);await sleep(1000);}
    const task=await publisher.core.registry.get(`${trigger.ref.name}.tasks.${hashOf({})}`);
    assert.ok(task,'heartbeat expiry deleted a still-owned task');
    const received=[];reader=await app.client('reader',{}, {detach:true});
    await reader.subscribe({name:'lease',trigger:trigger.ref,params:{},callback:async value=>received.push(value.input)});
    await until(()=>received,x=>x.length===9,'all inputs survive delayed recovery');
    assert.deepEqual(received,[0,1,2,3,4,5,6,7,8]);
    await reader.destroy();reader=undefined;
    assert.equal(await publisher.core.registry.get(`${trigger.ref.name}.tasks.${hashOf({})}`),undefined,'real unsubscribe still removes demand');
  }finally{try{await reader?.destroy();await publisher?.destroy();await app?.destroy()}finally{await f.close()}}
});

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

for (const retain of [true, false]) test(`${retain ? 'suspension' : 'removal'} with a full fetch buffer completes without dispatching its tail`, { timeout: 45000 }, async () => {
  const f = await fixture(); let app, publisher, consumer, unblock;
  const held = new Promise(resolve => { unblock = resolve; });
  try {
    app = await f.open(); publisher = await app.client('source', {}, { detach: true });
    const trigger = await publisher.trigger({ name: 'events', tester: async input => input,
      runner: async ({ publish }) => async input => publish({ input }) });
    consumer = await app.client('reader', {}, { detach: true });
    const received = [];
    const sub = await consumer.subscribe({ name: 'events', trigger: trigger.ref, params: {}, options: { concurrency: 1 },
      callback: async value => { received.push(value.input); await held; } });
    for (let input = 0; input < 300; input++) await trigger.executeForParams(input, [{}]);
    await until(() => received.length, count => count === 1, 'blocked first callback');
    const stream = 'trigger_test_source_events_events';
    const name = 'test_reader_events_' + hashOf({});
    const original = await until(() => publisher.core.jsm.consumers.info(stream, name), info => info.num_ack_pending === 101, 'real backpressured fetch');
    const closing = retain ? sub.suspend() : sub.destroy();
    unblock();
    await bounded(closing, 'suspend buffered iterator');
    await bounded(consumer.destroy(), 'suspended client teardown'); consumer = undefined;
    if (!retain) {
      await assert.rejects(publisher.core.jsm.consumers.info(stream, name), error => error.code === '404');
      assert.deepEqual(f.errors, []);
      return;
    }
    const retained = await publisher.core.jsm.consumers.info(stream, name);
    assert.equal(retained.created, original.created);
    assert.ok(retained.num_ack_pending + retained.num_pending > 0, 'unhandled tail must remain pending');
    assert.ok(received.length < 300, 'test must actually suspend before consuming the tail');
    consumer = await app.client('reader', {}, { detach: true });
    await consumer.subscribe({ name: 'events', trigger: trigger.ref, params: {}, callback: async value => { received.push(value.input); } });
    // Buffered, never-dispatched records were not ACKed. Request immediate
    // redelivery using the existing durable consumer's normal ACK timeout.
    await publisher.core.jsm.consumers.update(stream, name, { ack_wait: 200_000_000 });
    await until(() => new Set(received).size, size => size === 300, 'complete retained tail', 15000);
    assert.deepEqual([...new Set(received)].sort((a, b) => a - b), Array.from({ length: 300 }, (_, i) => i));
    assert.deepEqual(f.errors, []);
  } finally {
    unblock();
    try { if (consumer) await bounded(consumer.destroy(), 'consumer cleanup');
      if (publisher) await bounded(publisher.destroy(), 'publisher cleanup');
      if (app) await bounded(app.destroy(), 'app cleanup'); }
    finally { await f.close(); }
  }
});

test('stop during a real buffered JetStream fetch reaches closed and redelivers the unhandled records', { timeout: 45000 }, async () => {
  const f = await fixture(); let app, publisher, transport;
  try {
    app = await f.open(); publisher = await app.client('source', {}, { detach: true });
    const trigger = await publisher.trigger({ name: 'events', tester: async input => input,
      runner: async ({ publish }) => async input => publish({ input }) });
    const ref = { stream: 'trigger_test_source_events_events', subject: 'trigger.test.source.events.events' };
    const name = 'buffered_reader';
    await publisher.core.jsm.consumers.add(ref.stream, {
      durable_name: name, deliver_policy: 'new', ack_policy: 'explicit',
    });
    for (let input = 0; input < 100; input++) await trigger.executeForParams(input, [{}]);
    const before = await publisher.core.jsm.consumers.info(ref.stream, name);
    assert.equal(before.num_pending, 100);
    const received = []; let closing;
    transport = await publisher.core.stream.subscribe(publisher.telemetry.child('first-buffered-reader'), name, ref, async message => {
      received.push(message.data.input);
      closing ??= transport.suspend(); // abort inside the first batch, no await cycle
    }, { concurrency: 1, batchSize: 100 });
    await until(() => Boolean(closing), Boolean, 'first buffered callback');
    await bounded(closing, 'iterator stop sentinel');
    const after = await publisher.core.jsm.consumers.info(ref.stream, name);
    assert.equal(after.created, before.created);
    assert.ok(after.num_ack_pending > 0, 'already fetched but unhandled records remain unACKed');
    assert.ok(received.length < 100);
    transport = await publisher.core.stream.subscribe(publisher.telemetry.child('resumed-buffered-reader'), name, ref, async message => {
      received.push(message.data.input);
    }, { concurrency: 1 });
    await publisher.core.jsm.consumers.update(ref.stream, name, { ack_wait: 200_000_000 });
    await until(() => new Set(received).size, size => size === 100, 'all unACKed buffered records redelivered');
    assert.equal((await publisher.core.jsm.consumers.info(ref.stream, name)).created, before.created);
    assert.deepEqual(f.errors, []);
  } finally {
    try { if (transport) await bounded(transport.destroy(), 'transport cleanup');
      if (publisher) await bounded(publisher.destroy(), 'publisher cleanup');
      if (app) await bounded(app.destroy(), 'app cleanup'); }
    finally { await f.close(); }
  }
});

test('per-trigger retention survives restart, rejects overflow and never evicts pending messages', { timeout: 45000 }, async () => {
  const f = await fixture(); let app, publisher, consumer;
  try {
    app = await f.open(); publisher = await app.client('source', {}, { detach: true });
    const args = { name: 'retained', streamOptions: { maxAge: 0, maxMessages: 2 },
      tester: async input => input, runner: async ({ publish }) => async input => publish({ input }) };
    let trigger = await publisher.trigger(args);
    const stream = 'trigger_test_source_retained_events';
    const ordinary = await publisher.trigger({ ...args, name: 'ordinary', streamOptions: undefined });
    assert.equal((await publisher.core.jsm.streams.info('trigger_test_source_ordinary_events')).config.max_age, 2 * 60 * 60 * 1e9);
    consumer = await app.client('reader', {}, { detach: true });
    const sub = await consumer.subscribe({ name: 'events', trigger: trigger.ref, params: {}, callback: async () => {} });
    await sub.suspend(); await consumer.destroy(); consumer = undefined;
    await trigger.executeForParams(1, [{}]); await trigger.executeForParams(2, [{}]);
    await assert.rejects(trigger.executeForParams(3, [{}]), error => error.api_error?.err_code === 10077);
    await publisher.destroy(); publisher = undefined;
    publisher = await app.client('source', {}, { detach: true });
    trigger = await publisher.trigger(args);
    const info = await publisher.core.jsm.streams.info(stream);
    assert.equal(info.config.max_age, 0);
    assert.equal(info.config.discard, 'new');
    assert.equal(info.config.max_msgs, 2);
    assert.ok(info.config.max_bytes > 0, 'retention must remain size-bounded');
    assert.equal(info.state.messages, 2);
    const received = [];
    consumer = await app.client('reader', {}, { detach: true });
    await consumer.subscribe({ name: 'events', trigger: trigger.ref, params: {}, callback: async value => { received.push(value.input); } });
    await until(() => received, values => values.length === 2, 'original retained payloads');
    assert.deepEqual(received, [1, 2]);
    await until(() => publisher.core.jsm.streams.info(stream), value => value.state.messages === 0, 'ACK frees capacity');
    await trigger.executeForParams(3, [{}]);
    await until(() => received, values => values.length === 3, 'publication resumes after ACK');
    void ordinary;
    assert.deepEqual(f.errors, []);
  } finally {
    try { if (consumer) await bounded(consumer.destroy(), 'consumer cleanup');
      if (publisher) await bounded(publisher.destroy(), 'publisher cleanup');
      if (app) await bounded(app.destroy(), 'app cleanup'); }
    finally { await f.close(); }
  }
});
