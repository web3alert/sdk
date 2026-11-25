import { type Telemetry, type ErrorCallback } from './types';
import { type ConnectionOptions, connect as natsConnect } from 'nats';
import { type GlobalOptions, Core } from './core';
import { App } from './app';
import { isInboxPrefix } from './inbox';

export type ReconnectOptions = Pick<ConnectionOptions,
  'reconnect' | 'maxReconnectAttempts' | 'reconnectTimeWait'
  | 'reconnectJitter' | 'reconnectJitterTLS' | 'reconnectDelayHandler'
>;

export type ConnectParams = {
  telemetry: Telemetry;
  token?: string;
  username?: string;
  password?: string;
  servers?: string | string[];
  inboxPrefix?: string;
  reconnectOptions?: ReconnectOptions;
  onConnectionLost?: (error: Error) => void | Promise<void>;
  options?: Partial<GlobalOptions>;
  uncaughtException?: ErrorCallback;
  workspaceName: string;
  appName: string;
};

export async function connect(params: ConnectParams): Promise<App> {
  const {
    telemetry,
    token,
    username,
    password,
    servers,
    inboxPrefix,
    reconnectOptions,
    onConnectionLost = exitOnConnectionLoss,
    options: globalOptions,
    uncaughtException = console.error,
    workspaceName,
    appName,
  } = params;

  if (inboxPrefix !== undefined && !isInboxPrefix(inboxPrefix)) {
    throw new Error('inboxPrefix must be _INBOX or _INBOX_<name> (letters, numbers, underscores or hyphens)');
  }

  const nats = await natsConnect({
    token,
    user: username,
    pass: password,
    servers,
    inboxPrefix,
    maxReconnectAttempts: reconnectOptions?.maxReconnectAttempts ?? -1,
    reconnect: reconnectOptions?.reconnect ?? true,
    reconnectTimeWait: reconnectOptions?.reconnectTimeWait ?? 2_000,
    reconnectJitter: reconnectOptions?.reconnectJitter ?? 1_000,
    reconnectJitterTLS: reconnectOptions?.reconnectJitterTLS ?? 1_000,
    reconnectDelayHandler: reconnectOptions?.reconnectDelayHandler,
  });
  const core = new Core({
    telemetry,
    nats,
    options: globalOptions,
    uncaughtException,
  });

  // nats.js calls the protocol methods directly for broker-initiated closure.
  // Record caller intent here, including direct use through client.core.nats.
  const close = nats.close.bind(nats);
  const drain = nats.drain.bind(nats);
  nats.close = () => {
    core.beginShutdown();
    return close();
  };
  nats.drain = () => core.trackDrain(drain);

  let app: App;
  try {
    await core.init();
    app = new App({ core, workspace: workspaceName, name: appName });
    await app.init();
    if (nats.isClosed()) {
      throw await nats.closed() ?? new Error('NATS connection closed during initialization');
    }
  } catch (error) {
    await nats.close();
    throw error;
  }

  // closed() resolves with Error or undefined. Status errors can be nonterminal
  // (for example a publish permission violation), so only react to closure.
  void nats.closed().then(async error => {
    if (core.isShuttingDown) {
      return;
    }

    try {
      await onConnectionLost(error ?? new Error('NATS connection closed unexpectedly'));
    } catch (callbackError) {
      exitOnConnectionLoss(callbackError);
    }
  });

  return app;
}

function exitOnConnectionLoss(error: unknown): never {
  try {
    console.error('NATS connection lost', error);
  } finally {
    process.exit(1);
  }
}
