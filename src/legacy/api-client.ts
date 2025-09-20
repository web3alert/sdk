import { type SubscriptionObjectRaw } from '@web3alert/types';
import { HttpAgent } from './http-agent';
import type {
  SourceAnnounce,
} from './types';

export type LegacyApi = {
  agent: HttpAgent;
};

export type SourceApiClientOptions = {
  apiUrl: string;
  apiToken: string;
  sourceName: string;
  instanceId: string;
};

export class SourceApiClient {
  public api: LegacyApi;
  public sourceName: string;
  public instanceId: string;
  
  constructor(options: SourceApiClientOptions) {
    const {
      apiUrl,
      apiToken,
      sourceName,
      instanceId,
    } = options;
    
    this.api = { agent: new HttpAgent({ url: apiUrl, token: apiToken }) };
    this.sourceName = sourceName;
    this.instanceId = instanceId;
  }
  
  public async init(): Promise<void> {
  }
  
  public async destroy(): Promise<void> {
  }
  
  public async announce(announce: SourceAnnounce): Promise<void> {
    await this.api.agent.request({
      method: 'post',
      path: `/sources/${this.sourceName}/apps`,
      body: announce,
    });
  }
  
  public async saveBundle(params: any): Promise<void> {
    await this.api.agent.request({
      method: 'post',
      path: `/sources/${this.sourceName}/bundles`,
      body: {
        name: params.name,
        version: params.version,
        types: params.types,
        events: params.events,
        force: params.force,
      },
    });
  }
  
  public async getSubscriptions(): Promise<SubscriptionObjectRaw[]> {
    return await this.api.agent.request({
      method: 'get',
      path: `/system/subscriptions`,
    });
  }
  
  public async saveState<State>(value: State): Promise<void> {
    await this.api.agent.request({
      method: 'post',
      path: `/system/state/${this.sourceName}-${this.instanceId}`,
      body: value,
    });
  }
  
  public async getState<State>(): Promise<State | undefined> {
    try {
      return await this.api.agent.request({
        method: 'get',
        path: `/system/state/${this.sourceName}-${this.instanceId}`,
      });
    } catch (err: any) {
      if (err.code == 'ENOTFOUND') {
        return undefined;
      }
      
      throw err;
    }
  }
}
