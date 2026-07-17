const assert = require('node:assert/strict');
const Module = require('node:module');
const path = require('node:path');
const test = require('node:test');

const entrypoint = path.resolve(__dirname, '../dist/index.js');

test('public entrypoint does not load or expose legacy AMQP APIs', () => {
  const loadedRequests = [];
  const originalLoad = Module._load;

  Module._load = function(request, parent, isMain) {
    loadedRequests.push(request);
    return originalLoad.call(this, request, parent, isMain);
  };

  let sdk;
  try {
    delete require.cache[entrypoint];
    sdk = require(entrypoint);
  } finally {
    Module._load = originalLoad;
  }

  assert.equal(
    loadedRequests.some(request => /amqp|rabbit/i.test(request)),
    false,
    `unexpected legacy transport load: ${loadedRequests.join(', ')}`,
  );

  for (const exportName of [
    'BusClient',
    'SourceBusClient',
    'SourceApiClient',
    'SubscriptionsCache',
    'System',
    'PollRunner',
    'LegacySource',
  ]) {
    assert.equal(Object.hasOwn(sdk, exportName), false, `${exportName} is still exported`);
  }

  assert.equal(typeof sdk.HttpAgent, 'function');
  assert.equal(typeof sdk.HttpAgentError, 'function');
  assert.equal(typeof sdk.HttpAgentClientError, 'function');
});

test('HttpAgent preserves request and response behavior', async t => {
  const sdk = require(entrypoint);
  const requests = [];

  t.mock.method(global, 'fetch', async (url, init) => {
    requests.push({ url, init });
    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      statusText: 'OK',
      headers: { 'content-type': 'application/json' },
    });
  });

  const agent = new sdk.HttpAgent({
    url: 'https://api.example.test',
    token: 'secret',
  });
  const result = await agent.request({
    path: '/resource',
    method: 'post',
    body: { enabled: true },
  });

  assert.deepEqual(result, { ok: true });
  assert.deepEqual(requests, [{
    url: 'https://api.example.test/resource',
    init: {
      method: 'POST',
      headers: {
        authorization: 'Bearer secret',
        'content-type': 'application/json',
      },
      body: JSON.stringify({ enabled: true }),
    },
  }]);
});

test('HttpAgent returns undefined for an empty successful response', async t => {
  const { HttpAgent } = require(entrypoint);
  t.mock.method(global, 'fetch', async () => new Response(null, { status: 204 }));

  const agent = new HttpAgent({ url: 'https://api.example.test', token: 'secret' });
  const result = await agent.request({ path: '/resource', method: 'delete' });

  assert.equal(result, undefined);
});

test('HttpAgent maps structured API errors to HttpAgentClientError', async t => {
  const { HttpAgent, HttpAgentClientError } = require(entrypoint);
  t.mock.method(global, 'fetch', async () => new Response(JSON.stringify({
    error: {
      message: 'Invalid resource',
      code: 'invalid_resource',
      payload: { field: 'target' },
    },
  }), { status: 400, statusText: 'Bad Request' }));

  const agent = new HttpAgent({ url: 'https://api.example.test', token: 'secret' });

  await assert.rejects(
    agent.request({ path: '/resource', method: 'post', body: {} }),
    error => {
      assert.ok(error instanceof HttpAgentClientError);
      assert.equal(error.message, 'Invalid resource');
      assert.equal(error.code, 'invalid_resource');
      assert.deepEqual(error.payload, { field: 'target' });
      return true;
    },
  );
});

test('HttpAgent preserves generic HTTP error details', async t => {
  const { HttpAgent, HttpAgentError } = require(entrypoint);
  const responseBody = JSON.stringify({ message: 'upstream unavailable' });
  t.mock.method(global, 'fetch', async () => new Response(responseBody, {
    status: 503,
    statusText: 'Service Unavailable',
  }));

  const agent = new HttpAgent({ url: 'https://api.example.test', token: 'secret' });

  await assert.rejects(
    agent.request({ path: '/resource', method: 'get' }),
    error => {
      assert.ok(error instanceof HttpAgentError);
      assert.equal(error.status, 503);
      assert.equal(error.statusMessage, 'Service Unavailable');
      assert.equal(error.body, responseBody);
      return true;
    },
  );
});
