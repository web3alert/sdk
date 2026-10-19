const assert = require('node:assert/strict');
const test = require('node:test');
const { spawn } = require('node:child_process');
const { mkdtemp, rm } = require('node:fs/promises');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { connect, createTelemetry } = require('../dist');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(read, accept, label) {
  const end = Date.now() + 15000;
  while (Date.now() < end) { const value = await read(); if (accept(value)) return value; await sleep(20); }
  throw Error(`${label} timed out`);
}

test('real registry resumes an aborted timestamp and every tick during downtime', { timeout: 45000 }, async () => {
  const socket = net.createServer();
  await new Promise((resolve, reject) => { socket.once('error', reject); socket.listen(0, '127.0.0.1', resolve); });
  const port = socket.address().port; await new Promise(resolve => socket.close(resolve));
  const directory = await mkdtemp(path.join(os.tmpdir(), 'w3a-cron-continuity-'));
  const server = spawn('nats-server', ['-js', '-a', '127.0.0.1', '-p', String(port), '-sd', directory], { stdio: ['ignore', 'ignore', 'pipe'] });
  let logs = '', app, client, held;
  const done = new Promise(resolve => server.once('close', resolve));
  server.stderr.on('data', value => { logs += value; });
  const open = () => connect({ servers: [`nats://127.0.0.1:${port}`], workspaceName: 'test', appName: 'cron',
    telemetry: createTelemetry({ logLevel: 'error', logFormat: 'json' }).telemetry });
  try {
    await until(() => logs, value => value.includes('Server is ready'), 'NATS');
    app = await open(); client = await app.client('timer');
    const name = 'test.timer.durable.state';
    const delivered = [];
    let block = true;
    await client.cron({ name: 'durable', expression: '* * * * * *', catchUp: true, callback: async (expected, signal) => {
      if (block) {
        held = expected;
        await new Promise((_, reject) => { signal.addEventListener('abort', () => reject(signal.reason), { once: true }); });
      }
      delivered.push(expected);
    } });
    await until(() => held, Number.isSafeInteger, 'held timer input');
    const before = await client.core.registry.get(name);
    assert.equal(Date.parse(before.next), held);
    await app.destroy(); app = undefined; client = undefined;
    // Wall-clock oracle, independent of Cron's cursor/parse implementation.
    await sleep(2200);
    const through = Math.floor(Date.now() / 1000) * 1000;
    const expected = Array.from({ length: (through - held) / 1000 + 1 }, (_, i) => held + i * 1000);
    block = false; app = await open(); client = await app.client('timer');
    assert.deepEqual(await client.core.registry.get(name), before);
    await client.cron({ name: 'durable', expression: '* * * * * *', catchUp: true, callback: async timestamp => { delivered.push(timestamp); } });
    await until(() => delivered, values => expected.every(value => values.includes(value)), 'all due timestamps');
    assert.deepEqual(delivered.filter(value => value <= through), expected);
    assert.ok(Date.parse((await client.core.registry.get(name)).next) > through);
  } finally {
    try { await app?.destroy(); }
    finally { server.kill('SIGTERM'); await done; await rm(directory, { recursive: true, force: true }); }
  }
});
