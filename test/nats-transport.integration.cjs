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
  let logs;

  async function start() {
    broker = spawn('nats-server', ['-c', configPath, '-js', '-a', '127.0.0.1', '-p', String(port),
      '-sd', directory], { stdio: ['ignore', 'ignore', 'pipe'] });
    logs = '';
    let spawnError;
    broker.on('error', error => { spawnError = error; });
    broker.stderr.on('data', data => { logs += data; });
    stopped = new Promise(resolve => broker.once('close', (code, signal) => resolve({ code, signal })));
    await until(() => {
      if (spawnError) throw spawnError;
      assert.equal(broker.exitCode, null, logs);
      return logs.includes('Server is ready');
    }, 'NATS did not start');
  }

  async function stop() {
    if (!broker) return;
    const sent = broker.exitCode === null && broker.signalCode === null && broker.kill('SIGINT');
    const { code, signal } = await stopped;
    broker = undefined;
    assert.ok(sent, `Could not send SIGINT to the running NATS broker\n${logs}`);
    assert.deepEqual({ code, signal }, { code: 0, signal: null }, `NATS did not exit cleanly after SIGINT\n${logs}`);
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

async function holdMutex(t, mutex) {
  let entered, resume;
  const holding = new Promise(resolve => { entered = resolve; });
  const blocked = new Promise(resolve => { resume = resolve; });
  const finished = mutex.lock(async () => {
    entered();
    await blocked;
  });
  t.after(async () => {
    resume();
    await finished.catch(() => {});
  });
  await Promise.race([holding, finished.then(() => assert.fail('mutex exited before entering its callback'))]);
  return async () => {
    resume();
    await finished;
  };
}

const quickReconnect = { reconnectTimeWait: 50, reconnectJitter: 0 };

test('the same app resumes its service, subscription, durable, registry and mutexes after a 30s outage',
    { timeout: 65_000 }, async t => {
  const f = await fixture(t);
  const admin = await f.openRaw();
  // Exercise recovery from a legacy memory bucket provisioned before the SDK connects.
  await admin.jetstream().views.kv('lock', { storage: 'memory', ttl: 10_000 });
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
  const createdAtSetup = (await info()).created;
  assert.equal((await core.jsm.streams.info('KV_lock')).config.storage, 'memory');
  const lock = core.mutex('recover');
  const peer = await f.open();
  const peerEvents = statuses(peer.core.nats);
  const peerLock = peer.core.mutex('recover');
  let active = 0;
  const acquire = mutex => mutex.lock(async () => {
    assert.equal(++active, 1, 'recovered locks must still provide mutual exclusion');
    await sleep(20);
    active--;
  });

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
    await acquire(lock);
    await exercise('before');
    await until(async () => (await info()).ack_floor.stream_seq === 1, 'first message was not ACKed');
    const before = await info();
    assert.equal(before.created, createdAtSetup, 'durable was replaced before its first ACK');
    assert.equal(before.ack_floor.consumer_seq, 1);

    // The nats.js default allows ten attempts about two seconds apart.
    await f.stop();
    await until(() => events.some(event => event.type === 'disconnect'), 'disconnect not reported');
    await sleep(30_000);
    assert.equal(nc.isClosed(), false, 'connection exhausted reconnect attempts during the outage');
    await f.start();
    await until(() => events.some(event => event.type === 'reconnect'), 'reconnect not reported');

    // A recreated durable starts with a fresh consumer sequence, even if its
    // name and stream position are the same. 2.10 can restamp created on restart.
    const resumed = await info();
    for (const field of ['delivered', 'ack_floor']) {
      assert.equal(resumed[field].consumer_seq, before[field].consumer_seq, `${field} consumer sequence survived`);
      assert.equal(resumed[field].stream_seq, before[field].stream_seq, `${field} stream sequence survived`);
    }

    assert.equal(await core.registry.get('reconnect'), 'before');
    await until(() => peerEvents.some(event => event.type === 'reconnect'), 'peer reconnect not reported');
    await assert.rejects(core.jsm.streams.info('KV_lock'), error => error.api_error?.err_code === 10059);
    await Promise.all([acquire(lock), acquire(core.mutex('recover')), acquire(peerLock)]);
    const recovered = await core.jsm.streams.info('KV_lock');
    assert.equal(recovered.config.storage, 'file');
    assert.equal(recovered.config.max_age, 10_000_000_000);
    await exercise('after');
    await until(async () => (await info()).ack_floor.stream_seq === 2, 'second message was not ACKed');
    assert.equal((await info()).ack_floor.consumer_seq, before.ack_floor.consumer_seq + 1);
    assert.deepEqual(delivered, ['before', 'after']);
    assert.deepEqual(f.lost, []);
    assert.deepEqual(f.errors, []);
  } finally {
    if (!nc.isClosed() && events.some(event => event.type === 'reconnect')) {
      await reader.destroy();
      await subscription.destroy();
    }
  }
  await peer.app.destroy();
  await app.destroy();
});

test('a held lock releases after reconnect to the same stream', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const admin = await f.openRaw();
  const kv = await admin.jetstream().views.kv('lock', { storage: 'file', ttl: 60_000 });
  const { app, core } = await f.open({ reconnectOptions: quickReconnect });
  const peer = await f.open();
  const events = statuses(core.nats);
  const before = await core.jsm.streams.info('KV_lock');
  const release = await holdMutex(t, core.mutex('reconnect'));
  assert.equal((await kv.get('reconnect')).operation, 'PUT');

  await core.nats.reconnect();
  await until(() => events.some(event => event.type === 'reconnect'), 'lock holder did not reconnect');
  assert.equal((await core.jsm.streams.info('KV_lock')).created, before.created);
  await release();
  assert.equal((await kv.get('reconnect')).operation, 'DEL');
  await peer.core.mutex('reconnect').lock(async () => {});
  await peer.app.destroy();
  await app.destroy();
});

test('a stale release leaves a recreated stream\'s reused revision untouched', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const { app, core } = await f.open();
  const admin = await f.openRaw();
  const provision = () => admin.jetstream().views.kv('lock', { storage: 'file', ttl: 60_000 });
  const kv = await provision();
  const before = await core.jsm.streams.info('KV_lock');
  const release = await holdMutex(t, core.mutex('reused'));
  const original = await kv.get('reused');

  // Replace the stream while the holder's NATS connection stays up.
  await kv.destroy();
  const replacement = await provision();
  assert.notEqual((await core.jsm.streams.info('KV_lock')).created, before.created);
  const revision = await replacement.create('reused', codec.encode('new holder'));
  assert.equal(revision, original.revision, 'the replacement must reuse the old revision');
  await release();
  const entry = await replacement.get('reused');
  assert.equal(entry.operation, 'PUT');
  assert.equal(entry.revision, revision);
  assert.equal(codec.decode(entry.value), 'new holder');
  await app.destroy();
});

test('a custom lock bucket must be provisioned before connect', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const admin = await f.openRaw();
  const jsm = await admin.jetstreamManager();
  const options = { lockBucketName: 'deployment_lock' };
  await assert.rejects(f.open({ options }), error => error.api_error?.err_code === 10059);
  await assert.rejects(jsm.streams.info('KV_deployment_lock'), error => error.api_error?.err_code === 10059);
});

test('recovery of a missing custom lock waits for deployment even with create permission',
    { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const admin = await f.openRaw();
  const lockBucketName = 'deployment_lock';
  const provision = () => admin.jetstream().views.kv(lockBucketName, { storage: 'file', ttl: 60_000 });
  const kv = await provision();
  const { telemetry } = createTelemetry({ logLevel: 'error' });
  let reportRetry;
  const retrying = new Promise(resolve => { reportRetry = resolve; });
  telemetry.warn = ({ error }) => reportRetry(error);
  const { app, core } = await f.open({ options: { lockBucketName }, telemetry });
  await kv.destroy();

  const acquired = core.mutex('recover-custom').lock(async () => {});
  const outcome = await Promise.race([retrying, acquired.then(() => assert.fail('SDK recreated a custom lock bucket'))]);
  assert.equal(outcome.api_error?.err_code, 10059);
  await assert.rejects(core.jsm.streams.info('KV_deployment_lock'), error => error.api_error?.err_code === 10059);
  const restored = await provision();
  await acquired;
  assert.equal((await restored.get('recover-custom')).operation, 'DEL');
  const { config } = await core.jsm.streams.info('KV_deployment_lock');
  assert.equal(config.storage, 'file');
  assert.equal(config.max_age, 60_000_000_000);
  await app.destroy();
});

test('a newly created default lock bucket keeps held keys across a broker restart',
    { timeout: 15_000 }, async t => {
  const f = await fixture(t);
  const { app, core } = await f.open({ reconnectOptions: quickReconnect });
  const events = statuses(core.nats);
  const before = await core.jsm.streams.info('KV_lock');
  assert.equal(before.config.storage, 'file');
  assert.equal(before.config.max_age, 10_000_000_000);
  const kv = await core.js.views.kv('lock', { bindOnly: true, allow_direct: before.config.allow_direct });
  const release = await holdMutex(t, core.mutex('restart-held'));
  const held = await kv.get('restart-held');

  await f.stop();
  await f.start();
  await until(() => events.some(event => event.type === 'reconnect'), 'file lock holder did not reconnect');
  assert.equal((await core.jsm.streams.info('KV_lock')).created, before.created);
  const survived = await kv.get('restart-held');
  assert.equal(survived.operation, 'PUT');
  assert.equal(survived.revision, held.revision);
  await release();
  assert.equal((await kv.get('restart-held')).operation, 'DEL');
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

test('app destroy waits for the in-flight connection drain', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const { app, core } = await f.open();
  const nc = core.nats;
  let unblock;
  const blocked = new Promise(resolve => { unblock = resolve; });
  const flush = nc.protocol.flush.bind(nc.protocol);
  t.mock.method(nc.protocol, 'flush', async (...args) => {
    await blocked;
    return flush(...args);
  });

  const drain = nc.drain();
  const shutdown = app.destroy();
  try {
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(nc.isDraining(), true);
    assert.equal(nc.isClosed(), false);
  } finally {
    unblock();
    await Promise.all([drain, shutdown]);
  }
  assert.equal(nc.isClosed(), true);
  assert.deepEqual(f.lost, []);
});

test('app destroy closes the connection when the drain stalls', { timeout: 10_000 }, async t => {
  const f = await fixture(t);
  const { app, core } = await f.open();
  let unblock;
  const blocked = new Promise((_, reject) => { unblock = reject; });
  t.mock.method(core.nats.protocol, 'flush', () => blocked);
  const drain = core.nats.drain();
  try {
    await app.destroy();
    assert.equal(core.nats.isClosed(), true);
    assert.deepEqual(f.lost, []);
  } finally {
    unblock(new Error('fixture cleanup'));
    await drain;
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

test('exhausted attempts exit the process nonzero even with active timers', async t => {
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
    }, error => {
      console.error(error);
      process.exit(1);
    });
  `, sdkPath, f.servers], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  const exited = once(child, 'close');
  t.after(async () => {
    child.kill('SIGKILL');
    await exited;
  });
  // A cold Node worker can spend seconds loading the SDK under the full suite.
  // Give setup its own deadline and start timing reconnect exhaustion at readiness.
  const ready = await Promise.race([
    once(child, 'message', { signal: AbortSignal.timeout(30_000) }).catch(error => {
      throw new Error('SDK worker did not report ready', { cause: error });
    }),
    exited,
  ]);
  assert.equal(ready[0], 'ready', stderr);
  await t.test('the ready worker exits after the broker stops', { timeout: 10_000 }, async () => {
    await f.stop();
    assert.deepEqual(await exited, [1, null], stderr);
    assert.match(stderr, /NATS connection lost/);
    assert.match(stderr, /CONNECTION_REFUSED/);
  });
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
  const provision = () => admin.core.js.views.kv(lockBucketName, { storage: 'file', ttl: 10_000, replicas: 1 });
  const lock = await provision();
  await admin.core.registry.put('sample', 'stored value');
  const service = await admin.client.container.use(() => admin.core.service('app.test.app.echo'));
  await service.method('echo', async ({ req, res }) => { res.data = req.data; });

  const restricted = { username: 'sandbox', password: 'fixture', options: { lockBucketName } };
  await assert.rejects(f.open(restricted), { code: ErrorCode.PermissionsViolation });

  const { telemetry } = createTelemetry({ logLevel: 'error' });
  const recoveryWarnings = [];
  telemetry.warn = details => recoveryWarnings.push(details);
  const { app, core } = await f.open({ ...restricted, inboxPrefix: '_INBOX_sandbox', telemetry });
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
  await admin.core.jsm.streams.delete(`KV_${lockBucketName}`);
  let ran = false;
  const acquired = core.mutex(key).lock(async () => { ran = true; });
  void acquired.catch(() => {});
  await until(() => recoveryWarnings.length >= 2, 'restricted client did not retry binding the missing lock bucket');
  assert.equal(ran, false);
  assert.equal(recoveryWarnings[0].error.api_error?.err_code, 10059);
  assert.equal(recoveryWarnings[1].error.api_error?.err_code, 10059);
  assert.deepEqual(events.filter(event => event.type === 'error'), []);
  const restored = await provision();
  await acquired;
  assert.equal(ran, true);
  assert.equal((await restored.get(key)).operation, 'DEL');
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

test('SDK service answers a real NATS account service import', { timeout: 10_000 }, async t => {
  const f = await fixture(t, `
system_account: SYS
accounts {
  SYS {}
  SOURCE {
    jetstream: enabled
    users: [{ user: "source", password: "fixture" }]
    exports: [
      { service: "test.echo" },
      { service: "test.tracked", latency: { sampling: 100, subject: "test.latency" } }
    ]
  }
  IMPORTER {
    users: [{ user: "importer", password: "fixture" }]
    imports: [
      { service: { account: SOURCE, subject: "test.echo" }, to: "test.import" },
      { service: { account: SOURCE, subject: "test.tracked" }, to: "test.import_tracked" }
    ]
  }
}
`);
  const { app, client, core } = await f.open({ username: 'source', password: 'fixture' });
  const service = await client.container.use(() => core.service('test'));
  const imported = await f.openRaw({ user: 'importer', pass: 'fixture' });
  const exporter = await f.openRaw({ user: 'source', pass: 'fixture' });
  for (const [method, importedSubject, pattern] of [
    ['echo', 'test.import', /^_R_\.[A-Za-z0-9]{6}\.[A-Za-z0-9]{6}$/],
    ['tracked', 'test.import_tracked', /^_R_\.[A-Za-z0-9]{6}\.[A-Za-z0-9]{6}\.[A-Za-z0-9]{8}\.T$/],
  ]) {
    await service.method(method, async ({ req, res }) => { res.data = req.data; });
    const observer = exporter.subscribe(`test.${method}`, { max: 1 });
    const observation = (async () => {
      for await (const msg of observer) return msg.reply;
    })();
    await Promise.all([core.nats.flush(), exporter.flush()]);

    const [reply, result] = await Promise.all([
      observation,
      imported.request(importedSubject, codec.encode('"mapped reply"'), { timeout: 1500 }),
    ]);
    assert.match(reply, pattern);
    assert.deepEqual(JSON.parse(codec.decode(result.data)), { result: 'mapped reply' });
  }
  await app.destroy();
});
