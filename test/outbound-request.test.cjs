const assert = require('node:assert/strict');
const { createSocket } = require('node:dgram');
const dns = require('node:dns/promises');
const { once } = require('node:events');
const { createServer } = require('node:http');
const net = require('node:net');
const test = require('node:test');
const { createServer: createTlsServer } = require('node:tls');
const { inspect } = require('node:util');
const {
  OutboundRequestError,
  createOutboundLookup,
  guardedRequest,
  isOutboundAddressAllowed,
} = require('../dist');

const loopback = '127.0.0.1';

async function listen(t, server) {
  const sockets = new Set();
  server.on('connection', socket => {
    sockets.add(socket);
    socket.once('close', () => sockets.delete(socket));
  });
  server.listen(0, loopback);
  await once(server, 'listening');
  t.after(() => {
    for (const socket of sockets) socket.destroy();
    return new Promise(resolve => server.close(resolve));
  });
  return server.address().port;
}

function localPolicy(...ports) {
  return { allowedPrivateCidrs: [`${loopback}/32`], allowedPorts: { 'http:': ports, 'https:': ports, 'ws:': ports } };
}

function localUrl(port, path = '/') {
  return `http://${loopback}:${port}${path}`;
}

function hasCode(code) {
  return error => error instanceof OutboundRequestError && error.code === code;
}

async function listenDns(t) {
  const socket = createSocket('udp4');
  socket.bind(0, loopback);
  await once(socket, 'listening');
  t.after(() => new Promise(resolve => socket.close(resolve)));
  return socket;
}

function respondToDns(socket, query, peer) {
  let offset = 12;
  while (query[offset]) offset += query[offset] + 1;
  offset++;
  const type = query.readUInt16BE(offset);
  const address = type === 1 ? Buffer.from([127, 0, 0, 1]) : Buffer.from('00000000000000000000000000000001', 'hex');
  const header = Buffer.from('000081800001000100000000', 'hex');
  query.copy(header, 0, 0, 2);
  const answer = Buffer.alloc(12);
  answer.writeUInt16BE(0xc00c, 0); // Name points to the original question.
  answer.writeUInt16BE(type, 2);
  answer.writeUInt16BE(1, 4); // IN class.
  answer.writeUInt16BE(address.length, 10);
  socket.send(Buffer.concat([header, query.subarray(12, offset + 4), answer, address]), peer.port, peer.address);
}

function observeDns(t, serverFor) {
  const { Resolver } = dns;
  const queries = [];
  // Keep this test on loopback even when run against the previous implementation.
  t.mock.method(dns, 'lookup', () => assert.fail('DNS must avoid the threadpool'));
  const configured = new WeakSet();
  for (const method of ['resolve4', 'resolve6']) {
    const resolve = Resolver.prototype[method];
    t.mock.method(Resolver.prototype, method, function (hostname) {
      if (!configured.has(this)) {
        this.setServers([`${loopback}:${serverFor(hostname).address().port}`]);
        configured.add(this);
      }
      const promise = resolve.call(this, hostname);
      queries.push({
        hostname,
        method,
        outcome: promise.then(() => 'answered', error => error.code),
      });
      return promise;
    });
  }
  return queries;
}

test('the IPv4 range table denies special-use networks and matches their boundaries', () => {
  const blocked = [
    '0.0.0.0', '0.255.255.255', '10.3.0.1', '100.64.0.0', '100.127.255.255',
    '127.0.0.1', '127.255.255.255', '168.63.129.16', '169.254.1.1', '172.16.0.0', '172.31.255.255',
    '192.0.0.9', '192.0.2.1', '192.31.196.1', '192.52.193.1', '192.88.99.1',
    '192.168.1.1', '192.175.48.1', '198.18.0.0', '198.19.255.255', '198.51.100.1', '203.0.113.1',
    '224.0.0.0', '239.255.255.255', '240.0.0.0', '255.255.255.255',
  ];
  const allowed = [
    '1.0.0.0', '8.8.8.8', '100.63.255.255', '100.128.0.0', '168.63.129.17', '172.15.255.255',
    '172.32.0.0', '198.17.255.255', '198.20.0.0', '223.255.255.255',
  ];
  for (const ip of blocked) assert.equal(isOutboundAddressAllowed(ip), false, ip);
  for (const ip of allowed) assert.equal(isOutboundAddressAllowed(ip), true, ip);
  for (const ip of [...blocked, ...allowed]) {
    assert.equal(isOutboundAddressAllowed(`[::ffff:${ip}]`), isOutboundAddressAllowed(ip), ip);
  }
});

test('the IPv6 range table permits global unicast outside special-use ranges', () => {
  for (const ip of [
    '::', '::1', '::127.0.0.1', '64:ff9b::7f00:1', '64:ff9b::808:808', '64:ff9b:1::1', '100::1',
    '2001::1', '2001:2::1', '2001:20::1', '2001:db8::1', '2002:7f00:1::1', '2620:4f:8000::1',
    '3fff:fff::1', '5f00::1', 'fc00::1', 'fd00:ec2::254', 'fe80::1', 'fec0::1', 'ff02::1', 'fe80::1%en0',
    '[::ffff:7f00:1]', 'missing.test', '[::1',
  ]) {
    assert.equal(isOutboundAddressAllowed(ip), false, ip);
  }
  for (const ip of ['[2001:4860:4860::8888]', '2001:200::1', '2606:4700::1111', '3fff:1000::1', '::ffff:808:808']) {
    assert.equal(isOutboundAddressAllowed(ip), true, ip);
  }
});

test('private CIDR exceptions open only the configured subnet', () => {
  const exceptions = ['127.0.0.1/32', '192.168.42.0/24', 'fd12:3456::/32', '::1/128'];
  for (const ip of ['127.0.0.1', '::ffff:127.0.0.1', '192.168.42.10', 'fd12:3456::affe', '::1']) {
    assert.equal(isOutboundAddressAllowed(ip, exceptions), true, ip);
  }
  for (const ip of ['127.0.0.2', '192.168.41.10', 'fd12:3457::affe', '64:ff9b::7f00:1', '::']) {
    assert.equal(isOutboundAddressAllowed(ip, exceptions), false, ip);
  }
  for (const cidr of ['0.0.0.0/0', '198.18.0.0/15', '127.0.0.1/8', '::/0', '64:ff9b::/96']) {
    assert.throws(() => isOutboundAddressAllowed(loopback, [cidr]), TypeError);
  }
});

test('WHATWG normalization checks decimal, hex, octal, bracketed IPv6, mapped and NAT64 targets', async () => {
  const policy = { resolve: async () => assert.fail('literals must bypass DNS') };
  for (const host of [
    '2130706433', '0x7f000001', '0177.0000.0000.0001', '127.1', '[::1]', '[::ffff:127.0.0.1]',
    '[::ffff:7f00:0001]', '[64:ff9b::7f00:1]',
  ]) {
    await assert.rejects(guardedRequest(`http://${host}/`, { policy }), hasCode('ADDRESS_DENIED'), host);
  }
  assert.throws(() => createOutboundLookup('ws://[::ffff:127.0.0.1]/', {
    policy: { ...policy, allowedProtocols: ['ws:'] },
  }), hasCode('ADDRESS_DENIED'));
});

test('protocols, ports, userinfo and exact origins are checked before DNS', async () => {
  const policy = {
    allowedOrigins: ['https://ALLOWED.test:443/'],
    resolve: async () => assert.fail('denied targets must bypass DNS'),
  };
  const denied = [
    ['http://allowed.test/', 'ORIGIN_DENIED'],
    ['https://foo.allowed.test/', 'ORIGIN_DENIED'],
    ['https://allowed.test./', 'ORIGIN_DENIED'],
    ['https://allowed.test:8443/', 'PORT_DENIED'],
    ['https://user:password@allowed.test/', 'INVALID_URL'],
    ['file:///etc/hosts', 'INVALID_URL'],
    ['ftp://allowed.test/', 'PROTOCOL_DENIED'],
  ];
  for (const [url, code] of denied) await assert.rejects(guardedRequest(url, { policy }), hasCode(code), url);
  assert.equal(typeof createOutboundLookup('https://allowed.test/path', { policy }), 'function');
});

test('direct HTTP connects using the checked address without another DNS lookup', async t => {
  const hosts = [];
  const port = await listen(t, createServer((request, response) => {
    hosts.push(request.headers.host);
    response.end('direct');
  }));
  let resolutions = 0;
  const policy = {
    ...localPolicy(port),
    resolve: async hostname => {
      assert.equal(hostname, 'rebind.test');
      ++resolutions;
      return [{ address: resolutions === 1 ? loopback : '127.0.0.2', family: 4 }];
    },
  };
  const url = `http://rebind.test:${port}/`;
  const response = await guardedRequest(url, { policy, headers: { Host: 'ignored.test' } });
  assert.equal(response.body.toString(), 'direct');
  assert.equal(resolutions, 1);
  assert.deepEqual(hosts, [`rebind.test:${port}`]);
  await assert.rejects(guardedRequest(url, { policy }), hasCode('ADDRESS_DENIED'));
  assert.equal(resolutions, 2);
  assert.equal(hosts.length, 1);
});

test('mixed public A and private AAAA answers are refused before any proxy connection', async t => {
  let connections = 0;
  const proxy = createServer();
  proxy.on('connection', () => ++connections);
  const port = await listen(t, proxy);
  const policy = { resolve: async () => [{ address: '8.8.8.8', family: 4 }, { address: 'fd00::1', family: 6 }] };
  await assert.rejects(guardedRequest('http://mixed.test', { policy, proxy: localUrl(port) }), hasCode('ADDRESS_DENIED'));
  assert.equal(connections, 0);
});

test('following redirects is opt-in and exact origins apply to every hop', async t => {
  let requests = 0;
  const port = await listen(t, createServer((_request, response) => {
    ++requests;
    response.writeHead(302, { Location: 'http://other.test/' });
    response.end('private redirect response');
  }));
  const url = localUrl(port);
  const policy = { ...localPolicy(80, port), allowedOrigins: [url] };
  await assert.rejects(guardedRequest(url, { policy }), error => hasCode('HTTP_STATUS')(error) && error.status === 302);
  assert.equal(requests, 1);
  await assert.rejects(guardedRequest(url, { policy, maxRedirects: 1 }), hasCode('ORIGIN_DENIED'));
  assert.equal(requests, 2);
});

test('redirects recheck DNS and enforce the configured hop limit', async t => {
  const requests = [];
  const port = await listen(t, createServer((request, response) => {
    requests.push(request.url);
    const location = request.url === '/dns'
      ? `http://private.test:${port}/stop`
      : '/loop';
    response.writeHead(307, { Location: location });
    response.end();
  }));
  const hostnames = [];
  const policy = { ...localPolicy(port), resolve: async hostname => {
    hostnames.push(hostname);
    return [{ address: '10.0.0.1', family: 4 }];
  } };
  await assert.rejects(guardedRequest(localUrl(port, '/dns'), { policy, maxRedirects: 2 }), hasCode('ADDRESS_DENIED'));
  assert.deepEqual(hostnames, ['private.test']);
  assert.deepEqual(requests, ['/dns']);
  await assert.rejects(guardedRequest(localUrl(port, '/loop'), { policy, maxRedirects: 1 }), hasCode('REDIRECT_LIMIT'));
  assert.deepEqual(requests, ['/dns', '/loop', '/loop']);
});

test('same-origin redirects retain credentials and cross-origin redirects strip them', async t => {
  const seen = [];
  const otherPort = await listen(t, createServer((request, response) => {
    seen.push(request);
    response.end('final');
  }));
  const port = await listen(t, createServer((request, response) => {
    seen.push(request);
    response.writeHead(302, { Location: request.url === '/start' ? '/same' : localUrl(otherPort) });
    response.end();
  }));
  const response = await guardedRequest(localUrl(port, '/start'), {
    policy: localPolicy(port, otherPort),
    maxRedirects: 2,
    method: 'POST',
    body: 'payload',
    secretHeaders: ['X-Service-Token'],
    headers: {
      Authorization: 'Bearer fake',
      Cookie: 'sid=fake',
      'Proxy-Authorization': 'Basic fake',
      'x-service-token': 'fake-token',
      'x-correlation-id': 'corr',
      'content-type': 'text/plain',
    },
  });
  assert.equal(response.body.toString(), 'final');
  assert.deepEqual(seen.map(request => request.method), ['POST', 'GET', 'GET']);
  assert.deepEqual(seen.map(request => request.headers.authorization), ['Bearer fake', 'Bearer fake', undefined]);
  assert.deepEqual(seen.map(request => request.headers.cookie), ['sid=fake', 'sid=fake', undefined]);
  assert.deepEqual(seen.map(request => request.headers['x-service-token']), ['fake-token', 'fake-token', undefined]);
  assert.deepEqual(seen.map(request => request.headers['proxy-authorization']), [undefined, undefined, undefined]);
  assert.deepEqual(seen.map(request => request.headers['x-correlation-id']), ['corr', 'corr', 'corr']);
  assert.equal(seen[0].headers['content-length'], '7');
  assert.equal(seen[1].headers['content-type'], undefined);
});

test('cross-origin redirects that preserve the body require a separate opt-in', async t => {
  const received = [];
  const target = await listen(t, createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    received.push({
      method: request.method,
      body: Buffer.concat(chunks).toString(),
      authorization: request.headers.authorization,
    });
    response.end('ok');
  }));
  const origin = await listen(t, createServer(async (request, response) => {
    if (request.url === '/same-origin') {
      response.writeHead(308, { Location: '/echo' }).end();
    } else if (request.url === '/echo') {
      for await (const chunk of request) response.write(chunk);
      response.end();
    } else {
      response.writeHead(Number(request.url.slice(1)), { Location: localUrl(target) }).end();
    }
  }));
  const options = {
    policy: localPolicy(origin, target), maxRedirects: 1, method: 'POST', body: 'fixture',
    headers: { authorization: 'Bearer fixture' },
  };
  const response = await guardedRequest(localUrl(origin, '/same-origin'), options);
  assert.equal(response.body.toString(), 'fixture');

  for (const status of [307, 308]) {
    await assert.rejects(guardedRequest(localUrl(origin, `/${status}`), options), hasCode('REDIRECT_BODY_DENIED'));
  }
  assert.deepEqual(received, []);

  for (const status of [307, 308]) {
    await guardedRequest(localUrl(origin, `/${status}`), { ...options, allowCrossOriginBodyRedirects: true });
  }
  assert.deepEqual(received, Array.from({ length: 2 }, () => ({
    method: 'POST', body: 'fixture', authorization: undefined,
  })));
});

const localhostAddresses = [
  { address: loopback, family: 4 },
  { address: '::1', family: 6 },
];
const localhostPolicy = { allowedPrivateCidrs: ['127.0.0.1/32', '::1/128'] };

function lookupAll(hostname, options) {
  const lookup = createOutboundLookup(`http://${hostname}`, options);
  return new Promise((resolve, reject) => {
    lookup(hostname, { all: true }, (error, addresses) => error ? reject(error) : resolve(addresses));
  });
}

test('the default DNS resolver handles a burst while other resolvers stall', { timeout: 5_000 }, async t => {
  const stalled = await listenDns(t);
  const responsive = await listenDns(t);
  responsive.on('message', (query, peer) => respondToDns(responsive, query, peer));
  const queries = observeDns(t, name => name.startsWith('slow-') ? stalled : responsive);
  const firstPacket = once(stalled, 'message');
  const controller = new AbortController();
  const outstanding = Promise.allSettled(['slow-one.test', 'slow-two.test'].map(hostname =>
    guardedRequest(`http://${hostname}`, { signal: controller.signal })));
  try {
    await Promise.race([
      firstPacket,
      outstanding.then(() => assert.fail('stalled lookups finished before sending a DNS packet')),
    ]);
    const hostnames = Array.from({ length: 10 }, (_, index) => `ready-${index}.test`);
    const answers = await Promise.all(hostnames.map(name => lookupAll(name, { policy: localhostPolicy })));
    assert.deepEqual(answers, hostnames.map(() => localhostAddresses));
    await assert.rejects(lookupAll('ready-mixed.test', {
      policy: { allowedPrivateCidrs: ['127.0.0.1/32'] },
    }), hasCode('ADDRESS_DENIED'));
    const stalledQueries = queries.filter(query => query.hostname.startsWith('slow-'));
    assert.equal(stalledQueries.length, 4);
    const stillPending = Symbol('pending');
    for (const { outcome } of stalledQueries) {
      assert.equal(await Promise.race([outcome, Promise.resolve(stillPending)]), stillPending);
    }
  } finally {
    controller.abort();
  }
  const outcomes = await outstanding;
  assert.ok(outcomes.every(outcome => outcome.status === 'rejected' && hasCode('ABORTED')(outcome.reason)));
  const cancelled = queries.filter(query => query.hostname.startsWith('slow-'));
  assert.deepEqual(await Promise.all(cancelled.map(query => query.outcome)), Array(4).fill('ECANCELLED'));
});

test('the request deadline cancels both native DNS queries', { timeout: 5_000 }, async t => {
  const stalled = await listenDns(t);
  const queries = observeDns(t, () => stalled);
  const firstPacket = once(stalled, 'message');
  await Promise.all([
    firstPacket,
    assert.rejects(guardedRequest('http://deadline.test', { timeoutMs: 400 }), hasCode('DEADLINE')),
  ]);
  assert.deepEqual(queries.map(query => query.method).sort(), ['resolve4', 'resolve6']);
  assert.deepEqual(await Promise.all(queries.map(query => query.outcome)), ['ECANCELLED', 'ECANCELLED']);
});

test('localhost resolves both families without consulting DNS or the hosts file', async t => {
  t.mock.method(dns, 'lookup', () => assert.fail('localhost must avoid the threadpool'));
  t.mock.method(dns, 'Resolver', () => assert.fail('localhost must avoid DNS'));
  assert.deepEqual(await lookupAll('localhost', { policy: localhostPolicy }), localhostAddresses);
  assert.deepEqual(await lookupAll('localhost.', { policy: localhostPolicy }), localhostAddresses);
  await assert.rejects(lookupAll('localhost'), hasCode('ADDRESS_DENIED'));
});

test('the overall deadline expires during DNS and late DNS results cannot open a socket', async t => {
  let connections = 0;
  const server = createServer();
  server.on('connection', () => ++connections);
  const port = await listen(t, server);
  let finishLookup;
  const policy = { ...localPolicy(port), resolve: () => new Promise(resolve => { finishLookup = resolve; }) };
  await assert.rejects(guardedRequest(`http://slow-dns.test:${port}`, { policy, timeoutMs: 100 }), hasCode('DEADLINE'));
  finishLookup([{ address: loopback, family: 4 }]);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(connections, 0);
});

test('caller cancellation interrupts DNS without exposing its reason', async () => {
  const controller = new AbortController();
  const request = guardedRequest('https://cancel.test', {
    signal: controller.signal,
    policy: { resolve: () => new Promise(() => {}) },
  });
  controller.abort(new Error('PRIVATE_ABORT_REASON'));
  await assert.rejects(request, error => hasCode('ABORTED')(error) && !inspect(error).includes('PRIVATE_ABORT_REASON'));
});

test('an absolute deadline cuts a slow drip while the server is still writing', async t => {
  let chunks = 0;
  let resolveClosed;
  const closed = new Promise(resolve => { resolveClosed = resolve; });
  const port = await listen(t, createServer((_request, response) => {
    response.write('start');
    const interval = setInterval(() => {
      ++chunks;
      response.write('.');
    }, 15);
    response.once('close', () => {
      clearInterval(interval);
      resolveClosed();
    });
  }));
  await assert.rejects(
    guardedRequest(localUrl(port), { policy: localPolicy(port), timeoutMs: 300 }),
    hasCode('DEADLINE'),
  );
  await closed;
  assert.ok(chunks > 1);
});

test('chunked responses exceeding the byte limit are cut before EOF', async t => {
  let resolveClosed;
  const closed = new Promise(resolve => { resolveClosed = resolve; });
  const port = await listen(t, createServer((_request, response) => {
    response.once('close', resolveClosed);
    response.write('1234');
    response.write('5678');
  }));
  await assert.rejects(
    guardedRequest(localUrl(port), { policy: localPolicy(port), maxResponseBytes: 7 }),
    hasCode('RESPONSE_LIMIT'),
  );
  await closed;
});

test('body limits count bytes and allow responses exactly at the limit', async t => {
  const port = await listen(t, createServer((_request, response) => response.end('🐱')));
  const options = { policy: localPolicy(port), maxResponseBytes: 4 };
  const response = await guardedRequest(localUrl(port), options);
  assert.equal(response.body.toString(), '🐱');
  await assert.rejects(guardedRequest(localUrl(port), { ...options, maxResponseBytes: 3 }), hasCode('RESPONSE_LIMIT'));
});

test('HTTP errors expose only fixed error text, code and numeric status', async t => {
  const port = await listen(t, createServer((_request, response) => {
    response.writeHead(503, 'PRIVATE_STATUS_TEXT', { 'X-Hidden': 'PRIVATE_HEADER' });
    response.end('PRIVATE_RESPONSE_BODY');
  }));
  await assert.rejects(guardedRequest(localUrl(port, '/?token=PRIVATE_URL'), { policy: localPolicy(port) }), error => {
    assert.ok(hasCode('HTTP_STATUS')(error));
    assert.deepEqual({ ...error }, { name: 'OutboundRequestError', code: 'HTTP_STATUS', status: 503 });
    for (const value of Object.getOwnPropertyNames(error).map(key => error[key])) {
      assert.doesNotMatch(inspect(value, { depth: Infinity }), /PRIVATE_/);
    }
    return true;
  });
});

test('Node net.connect uses the URL-bound lookup and revalidates on reconnect', async t => {
  let connections = 0;
  const port = await listen(t, net.createServer(socket => {
    ++connections;
    socket.end('rpc');
  }));
  let resolutions = 0;
  const lookup = createOutboundLookup(`ws://rpc.test:${port}`, { policy: {
    ...localPolicy(port),
    allowedProtocols: ['ws:'],
    resolve: async () => {
      ++resolutions;
      return [{ address: loopback, family: 4 }, ...(resolutions === 2 ? [{ address: '::1', family: 6 }] : [])];
    },
  } });
  const opts = { host: 'rpc.test', port, family: 4, lookup };
  const socket = net.connect(opts);
  socket.setEncoding('utf8');
  let data = '';
  for await (const chunk of socket) data += chunk;
  assert.equal(data, 'rpc');
  const [error] = await once(net.connect(opts), 'error');
  assert.ok(hasCode('ADDRESS_DENIED')(error));
  assert.equal(connections, 1);
  const [mismatch] = await once(net.connect({ ...opts, host: 'different.test' }), 'error');
  assert.ok(hasCode('ORIGIN_DENIED')(mismatch));
  assert.equal(resolutions, 2);
});

test('lookup returns Node all-answer form after validating both families', async () => {
  const addresses = [{ address: '8.8.8.8', family: 4 }, { address: '2001:4860::1', family: 6 }];
  const lookup = createOutboundLookup('wss://rpc.test', { policy: {
    allowedProtocols: ['wss:'],
    resolve: async () => addresses,
  } });
  const result = await new Promise((resolve, reject) => {
    lookup('rpc.test', { all: true }, (error, values) => error ? reject(error) : resolve(values));
  });
  assert.deepEqual(result, addresses);
});

function tunnelToLoopback(socket, port) {
  // Test-only routing. The proxy records the requested authority and dials only a local fixture.
  const upstream = net.connect({ host: loopback, port }, () => {
    socket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
    socket.pipe(upstream).pipe(socket);
  });
  socket.on('error', () => upstream.destroy());
  socket.on('close', () => upstream.destroy());
  upstream.on('error', () => socket.destroy());
  upstream.on('close', () => socket.destroy());
}

test('proxy CONNECT sees the original public DNS answer and the origin sees its own Host', async t => {
  const origins = [];
  const originPort = await listen(t, createServer((request, response) => {
    origins.push({ path: request.url, host: request.headers.host, auth: request.headers.authorization });
    response.end('tunneled');
  }));
  const connects = [];
  const proxy = createServer();
  proxy.on('connect', (request, socket) => {
    connects.push({ authority: request.url, host: request.headers.host, auth: request.headers.authorization });
    tunnelToLoopback(socket, originPort);
  });
  const proxyPort = await listen(t, proxy);
  let resolutions = 0;
  const options = {
    proxy: localUrl(proxyPort),
    policy: {
      ...localPolicy(originPort),
      resolve: async hostname => {
        assert.equal(hostname, 'public-first.test');
        ++resolutions;
        return [{ address: resolutions === 1 ? '8.8.8.8' : '10.0.0.1', family: 4 }];
      },
    },
    headers: { Authorization: 'origin-test-value' },
  };
  const url = `http://public-first.test:${originPort}/path?q=1`;
  assert.equal((await guardedRequest(url, options)).body.toString(), 'tunneled');
  assert.equal(resolutions, 1);
  assert.deepEqual(connects, [{ authority: `8.8.8.8:${originPort}`, host: `8.8.8.8:${originPort}`, auth: undefined }]);
  assert.deepEqual(origins, [{ path: '/path?q=1', host: `public-first.test:${originPort}`, auth: 'origin-test-value' }]);
  await assert.rejects(guardedRequest(url, options), hasCode('ADDRESS_DENIED'));
  assert.equal(resolutions, 2);
  assert.equal(connects.length, 1);
});

test('proxy denial never falls back to a direct connection or exposes its response', async t => {
  let originConnections = 0;
  const origin = createServer();
  origin.on('connection', () => ++originConnections);
  const originPort = await listen(t, origin);
  const proxy = createServer();
  proxy.on('connect', (_request, socket) => socket.end(
    'HTTP/1.1 403 PRIVATE_PROXY_REASON\r\nContent-Length: 18\r\n\r\nPRIVATE_PROXY_BODY',
  ));
  const proxyPort = await listen(t, proxy);
  await assert.rejects(guardedRequest(localUrl(originPort), {
    proxy: localUrl(proxyPort),
    policy: localPolicy(originPort),
  }), error => hasCode('PROXY_FAILED')(error) && !inspect(error).includes('PRIVATE_PROXY'));
  assert.equal(originConnections, 0);
});

test('the proxy receives a bracketed numeric IPv6 CONNECT authority', async t => {
  const authorities = [];
  const proxy = createServer();
  proxy.on('connect', (request, socket) => {
    authorities.push(request.url);
    socket.end('HTTP/1.1 403 Forbidden\r\n\r\n');
  });
  const port = await listen(t, proxy);
  const policy = { resolve: async () => [{ address: '2001:4860::1', family: 6 }] };
  await assert.rejects(guardedRequest('https://ipv6.test', { proxy: localUrl(port), policy }), hasCode('PROXY_FAILED'));
  assert.deepEqual(authorities, ['[2001:4860::1]:443']);
});

test('HTTPS sends the URL hostname as SNI both directly and through numeric CONNECT', async t => {
  const names = [];
  const originPort = await listen(t, createTlsServer({
    SNICallback(name, callback) {
      names.push(name);
      // Fail the handshake deliberately without using any certificate or key material.
      callback(new Error('PRIVATE_TLS_REASON'));
    },
  }));
  const authorities = [];
  const proxy = createServer();
  proxy.on('connect', (request, socket) => {
    authorities.push(request.url);
    tunnelToLoopback(socket, originPort);
  });
  const proxyPort = await listen(t, proxy);
  const policy = { ...localPolicy(originPort), resolve: async () => [{ address: loopback, family: 4 }] };
  for (const proxy of [undefined, localUrl(proxyPort)]) {
    await assert.rejects(guardedRequest(`https://tls.test:${originPort}/`, { policy, proxy }),
      error => hasCode('NETWORK_FAILED')(error) && !inspect(error).includes('PRIVATE_TLS_REASON'));
  }
  assert.deepEqual(names, ['tls.test', 'tls.test']);
  assert.deepEqual(authorities, [`${loopback}:${originPort}`]);
});
