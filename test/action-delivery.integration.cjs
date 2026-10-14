// Owned loopback NATS and HTTP only. No production services or external sends.
const test = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { mkdtemp, rm } = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const http = require('node:http');
const { connect, createTelemetry, actionDeliveryFence, actionDeliveryErrorKind } = require('../dist');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const deferred = () => { let resolve; const promise = new Promise(r => resolve = r); return { resolve, promise }; };
async function until(read, accept) {
  for (let i = 0; i < 500; i++) { const value = await read(); if (accept(value)) return value; await sleep(10); }
  throw Error('fixture condition timed out');
}

test('actual SDK/JetStream cancels queued HTTP, retains issued HTTP and isolates generations', { timeout: 30000 }, async () => {
  const socket = net.createServer(); await new Promise(resolve => socket.listen(0, '127.0.0.1', resolve));
  const port = socket.address().port; await new Promise(resolve => socket.close(resolve));
  const directory = await mkdtemp(path.join(os.tmpdir(), 'w3a-action-fence-'));
  const server = spawn(process.env.NATS_SERVER_BINARY || 'nats-server', ['-js', '-a', '127.0.0.1', '-p', String(port), '-sd', directory],
    { stdio: ['ignore', 'ignore', 'pipe'] });
  let logs = '', spawnError; server.stderr.on('data', bytes => { logs += bytes; }); server.on('error', error => { spawnError = error; });
  const exited = new Promise(resolve => server.once('close', resolve));
  let senderApp, engineApp, sink, heldResponse;
  const sockets = new Set(), requests = [];
  const queued = deferred(), continueQueue = deferred(), issued = deferred();
  try {
    await until(() => { if (spawnError) throw spawnError; return logs; }, value => value.includes('Server is ready'));
    sink = http.createServer((req, res) => {
      requests.push(req.url); req.resume();
      if (req.url === '/held') { heldResponse = res; issued.resolve(); }
      else { res.end('ok'); }
    });
    sink.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
    await new Promise(resolve => sink.listen(0, '127.0.0.1', resolve));
    const base = `http://127.0.0.1:${sink.address().port}`;
    const open = appName => connect({ servers: `nats://127.0.0.1:${port}`, workspaceName: 'fixture', appName,
      telemetry: createTelemetry({ logLevel: 'error', logFormat: 'json' }).telemetry });
    engineApp = await open('engine'); senderApp = await open('sender');
    const engine = await engineApp.client('engine'), sender = await senderApp.client('sender');
    const fence = await actionDeliveryFence(engine.core);
    const action = await sender.action('post').params().callback(async (input, delivery) => {
      if (input.params.path === '/queued') { queued.resolve(); await continueQueue.promise; }
      const response = await delivery.start(() => fetch(base + input.params.path, { method: 'POST', redirect: 'manual' }));
      await response.text();
    }, { guardedDelivery: true });
    const input = (delivery, endpoint) => ({ delivery, params: { path: endpoint } });
    const old = await fence.open('subscription-one');
    const waiting = engine.execute(action.ref, input(old, '/queued'));
    const rejected = assert.rejects(waiting, error => actionDeliveryErrorKind(error) === 'cancelled');
    await queued.promise; await fence.cancel(old); continueQueue.resolve(); await rejected;
    assert.deepEqual(requests, []);
    const active = await fence.open('subscription-one');
    let completed = false;
    const sending = engine.execute(action.ref, input(active, '/held')).then(() => { completed = true; });
    await issued.promise;
    await until(async () => { try { await fence.cancel(active); return true; } catch (error) {
      assert.equal(actionDeliveryErrorKind(error), 'unavailable'); return false;
    } }, Boolean);
    assert.equal(completed, false, 'cancel did not wait for HTTP response');
    heldResponse.end('ok'); heldResponse = undefined; await sending;
    await assert.rejects(engine.execute(action.ref, input(active, '/retry')), error => actionDeliveryErrorKind(error) === 'cancelled');
    const fresh = await fence.open('subscription-one'), other = await fence.open('subscription-two');
    await fence.cancel(active); // late cleanup cannot revoke fresh
    await engine.execute(action.ref, input(fresh, '/fresh'));
    await engine.execute(action.ref, input(other, '/other'));
    await senderApp.destroy(); senderApp = await open('replacement');
    const replacement = await senderApp.client('replacement');
    assert.deepEqual(await (await actionDeliveryFence(replacement.core)).open('subscription-one'), fresh);
    let legacyCalls = 0;
    const legacy = await replacement.action('legacy').params().callback(async () => { legacyCalls++; });
    await assert.rejects(engine.execute(legacy.ref, input(fresh, '/unsupported')));
    assert.equal(legacyCalls, 0, 'guarded commands cannot fall back to an unguarded sender');
    assert.deepEqual(requests, ['/held', '/fresh', '/other']);
  } finally {
    continueQueue.resolve(); heldResponse?.end('fixture cleanup');
    await senderApp?.destroy(); await engineApp?.destroy();
    for (const socket of sockets) socket.destroy();
    if (sink) await new Promise(resolve => sink.close(resolve));
    server.kill('SIGTERM'); await exited;
    await rm(directory, { recursive: true });
  }
});
