import type { Log } from '../types';
import type { SourceApiClient } from './api-client';
import type { SourceBusClient } from './bus-client';
import { SubscriptionsCache } from './subscriptions-cache';

export type SystemOptions = {
  log: Log;
  metrics: any; // TODO: add types
  api: SourceApiClient;
  bus: SourceBusClient;
};

export class System {
  public log: Log;
  public metrics: any; // TODO: add types
  public api: SourceApiClient;
  public bus: SourceBusClient;
  public subscriptions: SubscriptionsCache;
  
  constructor(options: SystemOptions) {
    const {
      log,
      metrics,
      api,
      bus,
    } = options;
    
    this.log = log;
    this.metrics = metrics;
    this.api = api;
    this.bus = bus;
    this.subscriptions = new SubscriptionsCache({
      log,
      api,
    });
  }
  
  public async init(): Promise<void> {
    await this.subscriptions.init();
  }
  
  public async destroy(): Promise<void> {
    await this.subscriptions.destroy();
  }
}
