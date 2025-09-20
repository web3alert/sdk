import { type Destructible } from '../../types';
import { type EventCallback } from '../types';

export type MultisourceSetupCallback<T> = (callback: EventCallback<T>) => Promise<Destructible>;

export type MultisourceParams<T> = {
  setup: MultisourceSetupCallback<T>;
};

export class Multisource<T> {
  private _setup: MultisourceSetupCallback<T>;
  private _callbacks: Set<EventCallback<T>>;
  private _implementation?: Destructible;
  
  constructor(params: MultisourceParams<T>) {
    const {
      setup,
    } = params;
    
    this._setup = setup;
    this._callbacks = new Set();
    this._implementation = undefined;
  }
  
  private async _initImplementation(): Promise<void> {
    if (this._implementation) {
      throw new Error('implementation already initialized');
    }
    
    this._implementation = await this._setup(async event => {
      for (const callback of this._callbacks) {
        await callback(event);
      }
    });
  }
  
  private async _destroyImplementation(): Promise<void> {
    if (!this._implementation) {
      return;
    }
    
    await this._implementation.destroy();
    this._implementation = undefined;
  }
  
  public async init(): Promise<void> {
    // no-op
  }
  
  public async destroy(): Promise<void> {
    await this._destroyImplementation();
  }
  
  public async listen(callback: EventCallback<T>): Promise<Destructible> {
    this._callbacks.add(callback);
    
    if (!this._implementation) {
      await this._initImplementation();
    }
    
    return {
      destroy: async () => {
        this._callbacks.delete(callback);
        
        if (this._callbacks.size == 0) {
          await this._destroyImplementation();
        }
      },
    };
  }
}
