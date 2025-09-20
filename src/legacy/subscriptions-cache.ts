import { type SubscriptionObjectRaw } from '@web3alert/types';
import { type Log } from '../types';
import { type SourceApiClient } from './api-client';
import { EventEmitter } from 'events';

const DEFAULT_UPDATE_INTERVAL = 10000; // 10 seconds

export type SubscriptionsCacheOptions = {
  log: Log;
  api: SourceApiClient;
  updateInterval?: number;
};

export class SubscriptionsCache extends EventEmitter {
  private log: Log;
  private api: SourceApiClient;
  private updateInterval: number;
  private timer!: NodeJS.Timeout;
  private data: SubscriptionObjectRaw[];
  
  constructor(options: SubscriptionsCacheOptions) {
    super()
    const {
      log,
      api,
      updateInterval,
    } = options;
    this.update = this.update.bind(this);
    
    this.log = log;
    this.api = api;
    this.updateInterval = updateInterval ?? DEFAULT_UPDATE_INTERVAL;
    this.data = [];
  }
  
  public async init(): Promise<void> {
    await this.update();
    
    this.timer = setInterval(this.update, this.updateInterval);
  }
  
  public async destroy(): Promise<void> {
    clearInterval(this.timer);
  }
  
  public async update(): Promise<void> {
    try {
      this.data = await this.api.getSubscriptions();
      this.emit('update');
    } catch (err) {
      this.log.error({ err }, 'update failed');
    }
  }
  
  public get(): SubscriptionObjectRaw[] {
    return this.data;
  }
}
