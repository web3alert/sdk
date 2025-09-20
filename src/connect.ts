import { type Telemetry, type ErrorCallback } from './types';
import { connect as natsConnect } from 'nats';
import { type GlobalOptions, Core } from './core';
import { App } from './app';

export type ConnectParams = {
  telemetry: Telemetry;
  token?: string;
  username?: string;
  password?: string;
  servers?: string | string[];
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
    options: globalOptions,
    uncaughtException = console.error,
    workspaceName,
    appName,
  } = params;
  
  const nats = await natsConnect({
    token,
    user: username,
    pass: password,
    servers,
  });
  const core = new Core({
    telemetry,
    nats,
    options: globalOptions,
    uncaughtException,
  });
  
  await core.init();
  
  const app = new App({
    core,
    workspace: workspaceName,
    name: appName,
  });
  await app.init();
  
  return app;
}
