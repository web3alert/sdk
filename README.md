# @web3alert/sdk

A comprehensive TypeScript SDK for building distributed microservices and event-driven applications. Built on top of [NATS](https://nats.io/) messaging system with JetStream support, this SDK provides robust primitives for creating scalable, fault-tolerant Web3Alert platform services.

## Table of Contents

- [Installation](#installation)
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

## Requirements

- Node.js >= 18
- NATS Server with JetStream enabled

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
