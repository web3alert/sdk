const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { mkdtemp, rm, writeFile } = require('node:fs/promises');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const { setTimeout: sleep } = require('node:timers/promises');
const { connect: rawConnect, ErrorCode, StringCodec } = require('nats');
const sdkPath = process.env.W3A_TASK_BASELINE_ROOT || path.resolve(__dirname, '../dist');
const { connect, createTelemetry } = require(sdkPath);
const codec = StringCodec();

async function until(read, label, timeout = 10_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await read()) return;
    await sleep(20);
  }
  assert.fail(label);
}

async function fixture(t, config = '') {
  const socket = net.createServer().listen(0, '127.0.0.1');
  await once(socket, 'listening');
  const { port } = socket.address();
  await new Promise(resolve => socket.close(resolve));
  const directory = await mkdtemp(path.join(os.tmpdir(), 'w3a-sdk-nats-'));
  const configPath = path.join(directory, 'nats.conf');
  const servers = `nats://127.0.0.1:${port}`;
  const connections = [];
  let broker;
  let stopped;

  async function start() {
    broker = spawn('nats-server', ['-c', configPath, '-js', '-a', '127.0.0.1', '-p', String(port),
      '-sd', directory], { stdio: ['ignore', 'ignore', 'pipe'] });
    let logs = '';
    let spawnError;
    broker.on('error', error => { spawnError = error; });
    broker.stderr.on('data', data => { logs += data; });
    stopped = new Promise(resolve => broker.once('close', resolve));
    await until(() => {
      if (spawnError) throw spawnError;
      assert.equal(broker.exitCode, null, logs);
      return logs.includes('Server is ready');
    }, 'NATS did not start');
  }

  async function stop() {
    if (!broker) return;
    broker.kill('SIGTERM');
    assert.equal(await stopped, 0);
    broker = undefined;
  }

  t.after(async () => {
    try {
      await Promise.all(connections.map(nc => nc.close()));
      await stop();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  await writeFile(configPath, config);
  await start();

  const lost = [];
  const errors = [];
  async function open(options = {}) {
    const app = await connect({
      servers, workspaceName: 'test', appName: 'transport',
      telemetry: createTelemetry({ logLevel: 'error' }).telemetry,
      onConnectionLost: error => { lost.push(error); },
      uncaughtException: error => { errors.push(error); },
      ...options,
    });
    const client = await app.client('app');
    connections.push(client.core.nats);
    return { app, client, core: client.core };
  }

  async function openRaw(options = {}) {
    const nc = await rawConnect({ servers, ...options });
    connections.push(nc);
    return nc;
  }

  return {
    servers, start, stop, open, openRaw, lost, errors,
    setConfig: config => writeFile(configPath, config),
  };
}

function statuses(nc) {
  const received = [];
  const feed = nc.status();
  void (async () => {
    for await (const status of feed) received.push(status);
  })();
  return received;
}

test('the same app resumes its service, subscription, durable and registry after a 30s outage',
    { timeout: 65_000 }, async t => {
  const f = await fixture(t);
  const { app, client, core } = await f.open();
  const nc = core.nats;
  const events = statuses(nc);
  const service = await client.container.use(() => core.service('app.test.app.echo'));
  await service.method('echo', async ({ req, res }) => { res.data = req.data; });

  const published = [];
  const subscription = await core.subscribe('app.test.app.events', async ({ data }) => { published.push(data); });
  const watched = [];
  await core.registry.watch(async update => { watched.push(update); }, { filter: 'reconnect' });

  const delivered = [];
  const stream = await core.stream(client.telemetry.child('stream'), 'recovery');
  const reader = await core.stream.subscribe(client.telemetry.child('reader'), 'durable', stream.ref,
    async ({ data }) => { delivered.push(data); });
  const info = () => core.jsm.consumers.info(stream.ref.stream, 'durable');
  const before = await info();

  async function exercise(value) {
    assert.equal(await core.call('app.test.app.echo.echo', value), value);
    await core.publish('app.test.app.events', value);
    await stream.publish(value);
    await core.registry.put('reconnect', value);
    assert.equal(await core.registry.get('reconnect'), value);
    await until(() => published.includes(value), 'core subscription did not receive the message');
    await until(() => delivered.includes(value), 'durable did not receive the message');
    await until(() => watched.some(update => update.value === value), 'registry watcher stalled');
  }

  try {
    await exercise('before');
    await until(async () => (await info()).ack_floor.stream_seq === 1, 'first message was not ACKed');

    // The nats.js default allows ten attempts about two seconds apart.
    await f.stop();
    await until(() => events.some(event => event.type === 'disconnect'), 'disconnect not reported');
    await sleep(30_000);
    assert.equal(nc.isClosed(), false, 'connection exhausted reconnect attempts during the outage');
    await f.start();
    await until(() => events.some(event => event.type === 'reconnect'), 'reconnect not reported');

    assert.equal(await core.registry.get('reconnect'), 'before');
    await exercise('after');
    await until(async () => (await info()).ack_floor.stream_seq === 2, 'second message was not ACKed');
    assert.equal((await info()).created, before.created);
    assert.deepEqual(delivered, ['before', 'after']);
    assert.deepEqual(f.lost, []);
    assert.deepEqual(f.errors, []);
  } finally {
    if (!nc.isClosed() && events.some(event => event.type === 'reconnect')) {
      await reader.destroy();
      await subscription.destroy();
    }
  }
  await app.destroy();
});

test('app destroy, explicit close and explicit drain are graceful', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  for (const shutdown of [
    ({ app }) => app.destroy(),
    ({ core }) => core.nats.close(),
    ({ core }) => core.nats.drain(),
  ]) {
    const connection = await f.open();
    const events = statuses(connection.core.nats);
    await shutdown(connection);
    assert.equal(await connection.core.nats.closed(), undefined);
    assert.deepEqual(events, []);
    assert.deepEqual(f.lost, []);
  }
});

test('an unexpected errorless close invokes the hook once', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const { core } = await f.open({ reconnectOptions: { reconnect: false } });
  const events = statuses(core.nats);
  await f.stop();
  assert.equal(await core.nats.closed(), undefined);
  await until(() => f.lost.length === 1, 'connection-lost hook was not called');
  assert.match(f.lost[0].message, /closed unexpectedly/);
  assert.deepEqual(events.map(event => event.type), ['disconnect']);
});

test('exhausted attempts exit the process nonzero even with active timers', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const child = spawn(process.execPath, ['-e', `
    const { connect, createTelemetry } = require(process.argv[1]);
    connect({
      servers: process.argv[2], workspaceName: 'test', appName: 'transport',
      telemetry: createTelemetry().telemetry,
      reconnectOptions: { maxReconnectAttempts: 2, reconnectTimeWait: 50, reconnectJitter: 0 },
    }).then(() => {
      setInterval(() => {}, 1000);
      process.send('ready');
    });
  `, sdkPath, f.servers], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  const exited = once(child, 'close');
  t.after(async () => {
    child.kill('SIGKILL');
    await exited;
  });
  const ready = await Promise.race([once(child, 'message'), exited]);
  assert.equal(ready[0], 'ready', stderr);
  await f.stop();
  assert.deepEqual(await exited, [1, null], stderr);
  assert.match(stderr, /NATS connection lost/);
  assert.match(stderr, /CONNECTION_REFUSED/);
});

test('authorization errors on reconnect become terminal, while initial auth rejects connect',
    { timeout: 15_000 }, async t => {
  const f = await fixture(t);
  const { core } = await f.open({
    reconnectOptions: { reconnectTimeWait: 50, reconnectJitter: 0 },
  });
  const events = statuses(core.nats);
  await f.stop();
  await f.setConfig('authorization { users: [{ user: allowed, password: fixture }] }');
  await f.start();
  const error = await core.nats.closed();
  assert.equal(error.code, ErrorCode.AuthorizationViolation);
  await until(() => f.lost.length === 1, 'auth close did not notify the hook');
  assert.equal(f.lost[0], error);
  assert.ok(events.some(event => event.type === 'error' && event.data === ErrorCode.AuthorizationViolation));
  await assert.rejects(f.open(), { code: ErrorCode.AuthorizationViolation });
  assert.equal(f.lost.length, 1);
});

const restrictedConfig = `
authorization {
  users: [
    { user: admin, password: fixture },
    {
      user: sandbox, password: fixture,
      permissions: {
        subscribe: { allow: ["_INBOX_sandbox.>"] }
        publish: { allow: [
          "app.test.app.echo.echo",
          "$JS.API.INFO",
          "$JS.API.STREAM.INFO.KV_registry",
          "$JS.API.STREAM.INFO.KV_marketplace_sandbox_lock",
          "$JS.API.DIRECT.GET.KV_registry.$KV.registry.sample",
          "$JS.API.DIRECT.GET.KV_marketplace_sandbox_lock.$KV.marketplace_sandbox_lock.app.test.app.mutex",
          "$KV.marketplace_sandbox_lock.app.test.app.mutex"
        ] }
      }
    }
  ]
}
`;

test('custom inbox and a preprovisioned lock work through a narrow broker ACL', { timeout: 15_000 }, async t => {
  const f = await fixture(t, restrictedConfig);
  const admin = await f.open({ username: 'admin', password: 'fixture' });
  const lockBucketName = 'marketplace_sandbox_lock';
  const lock = await admin.core.js.views.kv(lockBucketName, { storage: 'memory', ttl: 10_000, replicas: 1 });
  await admin.core.registry.put('sample', 'stored value');
  const service = await admin.client.container.use(() => admin.core.service('app.test.app.echo'));
  await service.method('echo', async ({ req, res }) => { res.data = req.data; });

  const restricted = { username: 'sandbox', password: 'fixture', options: { lockBucketName } };
  await assert.rejects(f.open(restricted), { code: ErrorCode.PermissionsViolation });

  const { app, client, core } = await f.open({ ...restricted, inboxPrefix: '_INBOX_sandbox' });
  const events = statuses(core.nats);
  assert.equal(await core.call('app.test.app.echo.echo', 'hello'), 'hello');
  assert.equal(await core.registry.get('sample'), 'stored value');
  const key = 'app.test.app.mutex';
  const sharedLock = await admin.core.js.views.kv('lock');
  for (let i = 0; i < 2; i++) {
    // A second acquisition reads the DEL marker via the direct GET endpoint.
    await core.mutex(key).lock(async () => {
      assert.equal((await lock.get(key)).operation, 'PUT');
      assert.equal(await sharedLock.get(key), null);
    });
    assert.equal((await lock.get(key)).operation, 'DEL');
  }
  await core.nats.flush();
  assert.deepEqual(events.filter(event => event.type === 'error'), []);
  assert.deepEqual(f.lost, []);
  await app.destroy();
  await admin.app.destroy();
});

test('invalid inbox prefixes fail before dialing NATS', async () => {
  for (const inboxPrefix of ['', '_INBOX_', '_INBOX_other.', '_INBOX_other\n', 'test', '_INBOX_*', '_INBOX_>']) {
    await assert.rejects(
      connect({ inboxPrefix, workspaceName: 'test', appName: 'transport' }),
      /inboxPrefix must be _INBOX/,
    );
  }
});

test('service and function guard replies while accepting both inbox forms', { timeout: 15_000 }, async t => {
  const f = await fixture(t);
  const telemetry = createTelemetry({ logLevel: 'warn', logFormat: 'json' });
  const warnings = [];
  telemetry.telemetry.warn = (details, message) => warnings.push({ details, message });
  const { app, client } = await f.open({ telemetry: telemetry.telemetry });
  const service = await client.container.use(() => client.core.service('app.test.app.echo'));
  const calls = [];
  await service.method('echo', async ({ req, res }) => {
    calls.push(req.data);
    res.data = req.data;
  });
  const fun = await client.fun('echo').params().result().callback(async input => {
    calls.push(input);
    return input;
  });
  const subscription = await client.core.subscribe('app.test.app.custom', async ({ data, respond }) => { respond(data); });
  const requester = await f.openRaw();
  const custom = await f.openRaw({ inboxPrefix: '_INBOX_sandbox' });
  await client.core.nats.flush();

  const methods = ['app.test.app.echo.echo', fun.ref.name, 'app.test.app.custom'];
  const reply = requester.subscribe('test.invalid-reply');
  for (const method of methods) {
    requester.publish(method, codec.encode('"drop"'), { reply: reply.getSubject() });
  }
  await until(() => warnings.length === methods.length, 'expected one warning per rejected response');
  await client.core.nats.flush();
  await requester.flush();
  assert.equal(reply.getReceived(), 0);
  assert.deepEqual(calls, []);
  assert.match(await telemetry.metrics.collect(), /nats_invalid_reply_subject_total 3(?:\n|$)/);

  for (const method of methods) {
    for (const [nc, value] of [[requester, 'default'], [custom, 'custom']]) {
      const result = await nc.request(method, codec.encode(JSON.stringify(value)), { timeout: 1000 });
      const payload = JSON.parse(codec.decode(result.data));
      assert.equal(method === 'app.test.app.custom' ? payload : payload.result, value);
    }
  }
  assert.deepEqual(calls, ['default', 'custom', 'default', 'custom']);
  await subscription.destroy();
  await app.destroy();
});
