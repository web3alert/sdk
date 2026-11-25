# @web3alert/sdk

A comprehensive TypeScript SDK for building distributed microservices and event-driven applications. Built on top of [NATS](https://nats.io/) messaging system with JetStream support, this SDK provides robust primitives for creating scalable, fault-tolerant Web3Alert platform services.

## Table of Contents

- [Installation](#installation)
- [Outbound request policy](#outbound-request-policy)
- [Version 4](#version-4)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
  - [Connection & App](#connection--app)
  - [Client](#client)
  - [Telemetry](#telemetry)
- [Features](#features)
  - [Messaging Patterns](#messaging-patterns)
  - [Data Storage](#data-storage)
  - [Scheduling](#scheduling)
  - [Distributed Primitives](#distributed-primitives)
  - [Blockchain Integration](#blockchain-integration)
- [API Reference](#api-reference)
- [Error Handling](#error-handling)
- [Utilities](#utilities)

## Installation

```bash
npm install @web3alert/sdk
```

## Outbound request policy

Use `guardedRequest(url, options)` for HTTP or HTTPS destinations supplied by
users. The default policy permits ports 80 for HTTP and 443 for HTTPS and
blocks private and special-use addresses. `allowedPorts` replaces the default
for each configured protocol. Use exact `allowedOrigins` for destinations
owned by a known integration, and reserve `allowedPrivateCidrs` and custom
`resolve` for trusted service configuration. The [range table](src/outbound-address.ts)
lists the blocked networks and the ranges eligible for private CIDR exceptions.
`isOutboundAddressAllowed(ip, allowedPrivateCidrs?)` checks a standard IP literal
without performing network I/O.

**API request**

```typescript
import { guardedRequest } from '@web3alert/sdk';

const { body } = await guardedRequest(providerUrl, {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(payload),
  timeoutMs: 3_000,
  maxResponseBytes: 256 * 1024,
});
const data = JSON.parse(body.toString('utf8'));
```

**Devourer provider through Squid**

```typescript
const { status, headers, body } = await guardedRequest(providerUrl, {
  proxy: 'http://squid:8080', // Trusted service configuration
  timeoutMs: providerTimeoutMs,
  maxResponseBytes: providerResponseLimit,
});
```

The SDK checks all target addresses, dials the first checked address, and uses
the URL hostname for HTTP Host, TLS SNI, and certificate verification. With a
proxy configured, both HTTP and HTTPS use CONNECT to the checked numeric IP and
port. The proxy is trusted to honor that authority. The SDK selects the proxy
from the explicit option and fails closed if the tunnel is refused. To support
proxied HTTP on port 80, Squid's CONNECT ACL must permit port 80 for the
appropriate clients. A configuration that allows CONNECT only on 443 supports
only port 443 through this path.

**RPC WebSocket using a client with a Node lookup option**

```typescript
import { createOutboundLookup } from '@web3alert/sdk';
import WebSocket from 'ws';

function connectRpc(endpoint: string) {
  const url = new URL(endpoint);
  const lookup = createOutboundLookup(url, {
    policy: { allowedProtocols: ['ws:', 'wss:'] },
    timeoutMs: 3_000,
  });
  return new WebSocket(url, { lookup, agent: false, followRedirects: false, handshakeTimeout: 5_000 });
}
```

Bind the hook to the same final URL that the client dials and apply it on every
reconnect. It checks literal addresses when created because Node skips DNS for
literals. For hostnames it resolves and checks both families on every connection,
including when the client asks for a single family. Use it for direct connections
with client redirects disabled. Start every handshake and reconnect on a fresh
socket. With `ws`, pass `agent: false` as above. A shared keep-alive agent can
reuse a pooled socket and bypass the lookup. The client owns the handshake and
message limits.

**Strict-origin delivery**

```typescript
await guardedRequest(slackHookUrl, {
  proxy: 'http://squid:8080',
  policy: { allowedProtocols: ['https:'], allowedOrigins: ['https://hooks.slack.com'] },
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(message),
});
```

The default deadline is 10 seconds from DNS through the last response byte,
including redirects. The default response limit is 1 MiB, checked while reading.
Success returns the status, headers, and a bounded `Buffer`. The client asks
for identity encoding and returns the bytes as received after HTTP framing.
Non-2xx statuses become `OutboundRequestError` with a fixed message, code and
numeric status. Errors omit upstream text, headers, bodies and native causes.
The default target resolver queries A and AAAA in parallel using c-ares, with a
two-second query timeout and one try per nameserver. Each lookup has its own
resolver and is cancelled on deadline or abort. The resolver uses the configured
nameservers, including Docker's embedded DNS. It checks every address before
selecting one and orders IPv4 before IPv6.

Literal IPs bypass DNS. The default resolver maps `localhost` to both loopback
addresses, which still require explicit CIDR allowances. For target names
defined only in `/etc/hosts`, supply a trusted `policy.resolve` that returns
all IPv4 and IPv6 answers. A custom resolver manages its own cancellation and
concurrency.

Set `maxRedirects` to opt in. Each hop is checked again, and origin changes drop
`Authorization`, `Cookie`, and names in `secretHeaders`. The SDK always drops
`Proxy-Authorization` and supplies the routing and framing headers. A cross-origin
redirect that would resend a body fails with `REDIRECT_BODY_DENIED`. Set
`allowCrossOriginBodyRedirects: true` only when the destination can receive the
body. Same-origin redirects can preserve the body.

## Version 4

Version 4 is NATS-only. The old RabbitMQ source runtime has been removed, including
`BusClient`, `SourceBusClient`, `System`, `PollRunner`, `LegacySource`, and the
legacy source API/cache helpers. Deprecated source services must remain pinned to
their existing SDK version; they are not compatible with version 4.

The top-level `HttpAgent` API and raw engine `Event` type remain available with
their existing contracts. Product/domain contracts must be imported directly from
`@web3alert/types` instead of relying on it as a transitive SDK dependency.

## Quick Start

```typescript
import { connect, createTelemetry, main } from '@web3alert/sdk';

main(async (use, interruption) => {
  // Create telemetry bundle (logging, metrics, tracing)
  const { telemetry } = createTelemetry({
    logLevel: 'info',
    logFormat: 'human',
  });

  // Connect to NATS and initialize the app
  const app = await use(async () => {
    return await connect({
      telemetry,
      servers: 'nats://localhost:4222',
      workspaceName: 'my-workspace',
      appName: 'my-app',
    });
  });

  // Create a client for a specific project
  const client = await app.client('my-project');

  // Define a function that can be called remotely
  const myFunction = await client.fun('greet')
    .params<{ name: string }>()
    .result<{ message: string }>()
    .callback(async (params) => {
      return { message: `Hello, ${params.name}!` };
    });

  // Wait for interruption signal (SIGINT)
  await interruption();
});
```

## Core Concepts

### Connection & App

The SDK uses a hierarchical structure: **Connection → App → Client → Components**

```typescript
import { connect } from '@web3alert/sdk';

const app = await connect({
  telemetry,                      // Telemetry instance
  servers: 'nats://localhost:4222', // NATS server(s)
  username: 'user',               // Optional: authentication
  password: 'pass',               // Optional: authentication
  token: 'secret',                // Optional: token auth
  workspaceName: 'production',    // Workspace identifier
  appName: 'api-service',         // Application name
  uncaughtException: console.error, // Error handler
});
```

Established connections reconnect indefinitely by default. Retries use a
2-second wait with up to 1 second of random jitter for both plaintext and TLS.
Use `reconnectOptions` to override nats.js settings such as
`maxReconnectAttempts`, `reconnectTimeWait`, `reconnectJitter`,
`reconnectJitterTLS`, `reconnectDelayHandler` or `reconnect`.
The initial connection and SDK initialization still reject on failure.

If an established connection closes unexpectedly, the SDK logs the error and
exits the process with status 1. Set `onConnectionLost(error)` to own that
shutdown in your application. It runs once on terminal closure and may return
a promise. A callback that throws or rejects falls back to the default exit.
`app.destroy()` and explicit NATS close or drain are graceful shutdown paths.
On teardown the SDK gives a connection drain up to five seconds to finish
before closing the socket.

For a restricted NATS user, set `inboxPrefix: '_INBOX_sandbox'` and allow
subscription to `_INBOX_sandbox.>`. The prefix must be `_INBOX` (the default)
or `_INBOX_<name>`, with letters, digits, underscores or hyphens in the name.
The SDK and nats.js use it for request replies and JetStream delivery inboxes.
The reply guard changes handler behavior. SDK services and functions drop
requests whose reply subject falls outside the accepted inbox formats or NATS
account service mappings (`_R_`). A nats.js `noMux` request with an explicit
reply subject must use an accepted inbox format. A JetStream push delivery
subject used as a reply target through `core.subscribe()` is subject to the
same guard. Use the JetStream consumer APIs for protocol acknowledgements and
flow control. The SDK logs each rejection and increments
`nats_invalid_reply_subject_total`.

Set `options: { lockBucketName: 'marketplace_sandbox_lock' }` to put that
app's mutexes in a separate KV bucket. The default is `lock`. Apps that
coordinate via a shared mutex must use the same bucket. Provision custom
buckets before startup, with file storage on the persistent JetStream volume,
one replica and a TTL matching `options.lockTimeout` (10 seconds by default).
Enable direct reads on the bucket for the ACL shown below. The existing
`registry` KV bucket must also be present. Opening these preprovisioned
buckets checks stream metadata and leaves their settings as provisioned.
The SDK creates a missing default `lock` bucket with file storage and preserves
the storage settings of an existing bucket. During recovery it creates only the
default bucket. For a missing custom bucket, the client logs the failure and
retries binding with backoff until the deploy job restores it. Mutex operations
read the stream creation timestamp so a held lock can release after reconnect
and a stale release can detect a replacement stream.

| Operation | Publish subjects for the example configuration |
| --- | --- |
| `connect()` | `$JS.API.INFO`, `$JS.API.STREAM.INFO.KV_marketplace_sandbox_lock`, `$JS.API.STREAM.INFO.KV_registry` |
| Check the lock's stream when acquiring and releasing a mutex | `$JS.API.STREAM.INFO.KV_marketplace_sandbox_lock` |
| Acquire and release a mutex | `$KV.marketplace_sandbox_lock.<key>` |
| Read after a mutex sequence conflict, including reacquisition after release | `$JS.API.DIRECT.GET.KV_marketplace_sandbox_lock.$KV.marketplace_sandbox_lock.<key>` |

The key is the NATS KV key passed to the mutex, including any application
namespace.
The client receives all these replies via `_INBOX_sandbox.>`. With direct
reads disabled, the mutex read uses
`$JS.API.STREAM.MSG.GET.KV_marketplace_sandbox_lock` instead. An ordinary
read from the direct-enabled registry uses
`$JS.API.DIRECT.GET.KV_registry.$KV.registry.<key>`; application service
requests and other bucket operations need their own permissions.

### Client

Clients provide namespaced access to SDK features within a specific project:

```typescript
const client = await app.client('alerts-service', {
  environment: 'production',
});

// Access various client features
await client.fun('my-function');        // Remote functions
await client.action('my-action');       // Actions
await client.trigger({ ... });          // Event triggers
await client.subscribe({ ... });        // Subscriptions
await client.cron({ ... });             // Cron jobs
await client.reducer({ ... });          // State reducers

// Local features (not distributed)
await client.local.timer('heartbeat', 5000, callback);
await client.local.scheduler('task', callback);
await client.local.blockchain({ ... }); // Blockchain integration
```

### Telemetry

The SDK provides a unified telemetry interface combining logging, metrics, and tracing:

```typescript
import { createTelemetry, getTelemetryParams } from '@web3alert/sdk';

// Create with explicit params
const { log, metrics, tracing, telemetry } = createTelemetry({
  logLevel: 'debug',  // 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal'
  logFormat: 'json',  // 'human' | 'json'
  trace: true,        // Enable console tracing
});

// Or use environment variables (LOG_LEVEL, LOG_FORMAT, TRACE)
const params = getTelemetryParams();
const { telemetry } = createTelemetry(params);

// Logging
telemetry.info('Service started');
telemetry.debug({ userId: 123 }, 'Processing request');
telemetry.error({ err }, 'Operation failed');

// Metrics (Prometheus-compatible)
const requestCounter = telemetry.counter({
  name: 'requests_total',
  help: 'Total number of requests',
  labelNames: ['method', 'status'],
});
requestCounter.inc({ method: 'GET', status: '200' });

const responseTime = telemetry.histogram({
  name: 'response_time_seconds',
  help: 'Response time in seconds',
  buckets: [0.1, 0.5, 1, 2, 5],
});
const stopTimer = responseTime.startTimer();
// ... do work ...
stopTimer();

// Tracing
await telemetry.span('process-order', async (span) => {
  span.trace({ orderId: 'abc123' }, 'Starting processing');
  // ... processing logic ...
  return result;
});

// Create child telemetry with additional context
const childTelemetry = telemetry.child('order-processor', {
  labels: { service: 'orders' },
});
```

## Features

### Messaging Patterns

#### Publish/Subscribe

```typescript
// Publisher
await core.publish('events.user.created', { userId: 123, email: 'user@example.com' });

// Subscriber
const subscription = await core.subscribe('events.user.>', async (message) => {
  console.log('Received:', message.data);
}, { queue: 'user-events-processor' });
```

#### Request/Response (RPC)

```typescript
// Define a remote function
const calculator = await client.fun('add')
  .params<{ a: number; b: number }>()
  .result<{ sum: number }>()
  .callback(async ({ a, b }) => ({ sum: a + b }));

// Call from another service
const result = await client.call(client.ref.fun('workspace.project.add')
  .params<{ a: number; b: number }>()
  .result<{ sum: number }>(),
  { a: 5, b: 3 }
);
// result = { sum: 8 }
```

#### Streams (JetStream)

Durable message streams with at-least-once delivery:

```typescript
// Create a stream
const stream = await core.stream(telemetry, 'orders', {
  maxAge: 2 * 60 * 60 * 1000,    // 2 hours retention
  maxSize: 100 * 1024 * 1024,    // 100 MB max size
  maxMessages: 100_000,          // Max messages
  maxMessageSize: 100 * 1024,    // 100 KB per message
});

// Publish to stream
await stream.publish({ orderId: '123', amount: 99.99 });
await stream.publishToSubject('priority', { orderId: '456', urgent: true });

// Subscribe to stream
const subscription = await core.stream.subscribe(
  telemetry,
  'orders-processor',
  stream.ref,
  async (message) => {
    console.log('Processing order:', message.data);
  },
  { concurrency: 5 }
);

// Stream pipe (combined stream + subscription)
const pipe = await core.stream.pipe(telemetry, 'tasks', async (message) => {
  await processTask(message.data);
});
await pipe.publish({ taskId: 'task-1' });
```

### Data Storage

#### Buckets (Key-Value Store)

```typescript
// Create a bucket
const users = await core.bucket<User>('users', {
  maxBytes: 10 * 1024 * 1024,  // 10 MB
  ttl: 3600000,                 // 1 hour TTL
  storage: 'file',              // 'file' | 'memory'
});

// Basic operations
await users.put('user:123', { name: 'John', email: 'john@example.com' });
const user = await users.get('user:123');
await users.delete('user:123');

// Atomic mutations with optimistic locking
await users.mutate('user:123', async (current, write) => {
  const updated = { ...current, lastLogin: new Date() };
  await write(updated);
  return updated;
});

// List keys
const allKeys = await users.keys();
const filteredKeys = await users.keys('user:*');

// Watch for changes
const watcher = await users.watch(async (update) => {
  console.log(`Key ${update.key} changed:`, update.value);
}, { filter: 'user:>' });

// Slices (namespaced views)
const premiumUsers = users.slice<PremiumUser>('premium');
await premiumUsers.put('user:456', { tier: 'gold' });

// Cells (single-key accessor)
const configCell = users.cell<Config>('config');
await configCell.put({ maxRetries: 3 });
const config = await configCell.get();
```

### Scheduling

#### Cron Jobs

```typescript
const dailyReport = await client.cron({
  name: 'daily-report',
  expression: '0 0 * * *',  // Every day at midnight
  callback: async (now) => {
    await generateDailyReport(now);
  },
});
```

#### Timers

```typescript
const heartbeat = await client.local.timer('heartbeat', 5000, async (now) => {
  await sendHeartbeat(now);
});
```

#### Schedulers

```typescript
const scheduler = await client.local.scheduler('task-scheduler', async (now, schedule) => {
  // Schedule next execution dynamically
  schedule(now + 60000); // Run again in 1 minute
  
  await performScheduledTask();
});
```

### Distributed Primitives

#### Mutex (Distributed Locking)

```typescript
// Local mutex
import { Mutex } from '@web3alert/sdk';

const mutex = new Mutex();
await mutex.lock(async () => {
  // Critical section - only one execution at a time
  await performExclusiveOperation();
});

// Distributed mutex (via Core)
const cell = core.mutex('resource:123');
await cell.lock(async () => {
  // Distributed lock across all instances
  await modifySharedResource();
});
```

#### Spawners

Dynamic resource management based on configuration changes:

```typescript
const spawner = await core.spawner<Config, Worker>(
  (a, b) => a.id === b.id,  // Compare function
  async (config) => {
    // Called when config is added/updated
    const worker = new Worker(config);
    await worker.init();
    return worker;
  }
);

// Update spawner with new configs
await spawner.set([
  { id: 'worker-1', threads: 4 },
  { id: 'worker-2', threads: 2 },
]);
```

#### Summoners

```typescript
const summoner = await core.summoner<Params>(async (params) => {
  // Called when params change
  await reconfigure(params);
});

await summoner.update({ maxConnections: 100 });
```

### Blockchain Integration

The SDK provides abstractions for blockchain connectivity with multi-upstream support:

```typescript
const blockchain = await client.local.blockchain({
  specs: [
    { name: 'primary', spec: { rpcUrl: 'https://mainnet.infura.io/v3/...' } },
    { name: 'backup', spec: { rpcUrl: 'https://eth.llamarpc.com' } },
  ],
  factory: async (spec) => {
    // Create your blockchain backend
    return new EthereumBackend(spec);
  },
  options: {
    upstreamTimeout: 30_000,  // Consider upstream offline after 30s
  },
});

// Listen for new blocks
await blockchain.listen(async (blockNumber) => {
  console.log('New block:', blockNumber);
  await processBlock(blockNumber);
});

// Get a healthy upstream for queries
const backend = await blockchain.upstream();
if (backend) {
  const balance = await backend.getBalance(address);
}
```

### Triggers & Subscriptions

Define event triggers and subscribe to them:

```typescript
// Define a trigger
const priceAlert = await client.trigger({
  name: 'price-alert',
  runner: async (params, emit) => {
    // Setup price monitoring
    return await monitorPrice(params.token, params.threshold, emit);
  },
  tester: async (params) => {
    // Test trigger configuration
    return { valid: true };
  },
});

// Subscribe to the trigger
const subscription = await client.subscribe({
  name: 'my-price-alert',
  trigger: client.ref.trigger('workspace.project.price-alert'),
  params: { token: 'ETH', threshold: 2000 },
  callback: async (event) => {
    await sendNotification(event.data);
  },
});
```

#### Restart-safe transport

`await subscription.suspend()` stops local consumption and waits for in-flight
callbacks, but retains the JetStream durable consumer, pending messages and task
binding. Recreate the subscription with the same workspace/project/name and
parameters to resume it. This closes the old handle permanently; subsequent
client teardown is idempotent and does not delete its retained consumer. Ordinary
client teardown without suspension still unsubscribes and deletes the consumer.
The owner must explicitly clean retained resources when a suspended subscription
is permanently removed. Task leases still expire normally; suspension does not
renew demand indefinitely. Stream size/age limits still apply.

For a durable outbox, pass `streamOptions: { maxAge: 0 }` to `client.trigger()`.
This disables expiry only for that trigger. The default `DiscardPolicy.New`,
100 MiB byte limit and 100,000 message limit remain: a full stream rejects the
new publication, leaving pending messages intact. Its owner must retry/backpressure
and clean up permanently retired consumers. Keep the same options on restart;
the default trigger policy still expires messages after two hours. Retention is
interest-based, so removing the last durable consumer still releases its messages.

`await trigger.executeForParams(input, acceptedParams)` dispatches previously
accepted work using the same parameter-derived subjects and reducer state as
normal execution, even if its task has since unsubscribed. Duplicate parameter
sets are dispatched once per call. It does not register subscribers or renew
leases. The caller owns the durable accepted-work record, retry policy and any
new-subscriber cutoff. Publication failures propagate; retries remain
at-least-once. With interest retention, a successful send is not a promise to
retain messages after the last durable consumer has actually been removed.

Run `npm test` for unit/type checks and `npm run test:transport` with a native
`nats-server` on PATH for isolated JetStream restart, reconnect, pending/backlog,
unsubscribe and repeated teardown checks. Transport test files run serially to
keep the short lease-renewal deadlines isolated from the other broker tests.
Each fixture creates and removes its own temporary server/data directory.

### Actions

Define reusable actions:

```typescript
const sendEmail = await client.action('send-email')
  .params<{ to: string; subject: string; body: string }>()
  .callback(async (params) => {
    await emailService.send(params);
  });

// Execute action from another service
await client.execute(
  client.ref.action('workspace.project.send-email'),
  { to: 'user@example.com', subject: 'Alert', body: 'Price threshold reached!' }
);
```

### Reducers

Aggregate state over time:

```typescript
const orderStats = await client.reducer<Stats>({
  name: 'order-stats',
  init: async () => ({ totalOrders: 0, totalRevenue: 0 }),
  execute: async (state, event) => {
    return {
      totalOrders: state.totalOrders + 1,
      totalRevenue: state.totalRevenue + event.amount,
    };
  },
});
```

## Error Handling

The SDK provides custom error classes with additional context:

```typescript
import { Web3alertError, Web3alertClientError, error } from '@web3alert/sdk';

// Throw internal errors (hidden from clients)
throw new Web3alertError('Database connection failed', {
  cause: originalError,
  details: { host: 'db.example.com', port: 5432 },
});

// Throw client-visible errors
throw new Web3alertClientError('Invalid token address', {
  details: { address: '0x...' },
});

// Or use the helper function
throw error('Invalid parameters', { details: { field: 'amount' } });
```

## Utilities

### Retry with Exponential Backoff

```typescript
import { retry } from '@web3alert/sdk';

const result = await retry(
  async () => {
    return await fetchExternalAPI();
  },
  {
    retries: 5,
    minDelay: 250,
    maxDelay: 120_000,
    factor: 1.5,
    jitter: 0.1,
    when: (err, attempt) => err.code === 'ETIMEDOUT',
    signal: abortController.signal,
  }
);
```

### Sleep

```typescript
import { sleep } from '@web3alert/sdk';

await sleep(1000);  // Sleep for 1 second

// With abort signal
await sleep(5000, { signal: abortController.signal });
```

### Hashing & Comparison

```typescript
import { hashOf, defaultCompare } from '@web3alert/sdk';

const hash = hashOf({ foo: 'bar' });  // MD5 hash of JSON
const isEqual = defaultCompare(objA, objB);  // Compare by hash
```

### Environment Variables

```typescript
import { getString, getBoolean, getNumber } from '@web3alert/sdk';

const dbHost = getString('DB_HOST', 'localhost');
const debug = getBoolean('DEBUG', false);
const port = getNumber('PORT', 3000);
```

### Web Server

```typescript
import { WebServer } from '@web3alert/sdk';

const server = new WebServer({
  log: telemetry.log,
  handler: (req, res) => {
    res.writeHead(200);
    res.end('OK');
  },
  listen: { port: 8080 },
});

await server.init();
// ... later ...
await server.destroy();
```

### Condition Matching

```typescript
import { is, match } from '@web3alert/sdk';

// Type checking
is.string('hello');     // true
is.number(42);          // true
is.array([1, 2, 3]);    // true
is.object({});          // true

// Pattern matching
match(value, pattern);
```

## Web Server & Metrics

Expose Prometheus metrics via HTTP:

```typescript
import { WebServer } from '@web3alert/sdk';

const metricsServer = new WebServer({
  log: telemetry.log,
  handler: async (req, res) => {
    if (req.url === '/metrics') {
      res.setHeader('Content-Type', telemetry.metrics.contentType);
      res.end(await telemetry.metrics.render());
    } else {
      res.writeHead(404);
      res.end('Not Found');
    }
  },
  listen: { port: 9090 },
});

await metricsServer.init();
```

## Cancellable action delivery

Notification senders can opt into a durable cancellation boundary. This is a
transport primitive; the application controller decides whether a subscription
was cancelled. A technical restart or missing registry entry is not cancellation.

```typescript
import { actionDeliveryFence } from '@web3alert/sdk';

const fence = await actionDeliveryFence(client.core);
// Controller only, after an authoritative active-subscription read.
const delivery = await fence.open('subscription.example');

await sender.action('send').params<{ url: string }>().callback(async (input, guard) => {
  // Perform local queueing/rate limiting BEFORE guard.start(). Every fallback
  // or retry needs its own start; do not enable unguarded HTTP-library retries.
  const response = await guard.start(() => fetch(input.params.url, {
    method: 'POST', redirect: 'manual',
  }));
  await response.text();
}, { guardedDelivery: true });

// Pass `delivery` in ActionInput when calling client.execute().
// On explicit user cancellation, await this BEFORE upstream unsubscribe:
await fence.cancel(delivery);
```

Guarded commands use a distinct `.guarded.fun` subject. Old senders cannot
silently accept them without checking cancellation. Upgrade senders before
enabling guarded commands in their caller. Unmarked actions remain compatible.

Cancellation is confirmed only after its durable write and all admitted request
starts are acknowledged. It does not wait for already-issued HTTP responses or
abort them. Queued requests and subsequent retries/fallbacks cannot start after
confirmation. The controller retries an `unavailable` cancellation until it is
confirmed; it must not remove upstream demand first. An unavailable pre-request
guard is not a delivery failure to acknowledge/drop; retain the accepted work.

There is one file-backed `action-delivery` KV record per key. Its generation
survives restart; `open()` after cancellation creates a new generation. Old
callbacks and cancellation requests cannot affect it. Do not use changing
processing/checkpoint revisions as delivery generations.

Admission has no automatic expiry. If a sender dies in the short interval
between admission and acknowledging the request start, cancellation remains
pending rather than falsely confirming. Do not clear the admission while that
old process could resume. During planned deployment, complete these short start
sections before stopping the sender; an unconfirmed boundary aborts switching.
This primitive does not transfer in-flight action RPCs or implement delivery replay.

## Requirements

- Node.js >= 18
- NATS Server with JetStream enabled

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
