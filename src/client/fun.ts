import { type Telemetry } from '../types';
import { type Core } from '../core';
import { type Subscription } from '../subscription';
import { serializeError } from '../errors';

export type FunCallback<P, R> = (params: P) => Promise<R>;

export type FunRef<P, R> = {
  name: string;
  params?: P;
};

export type FunParams<P, R> = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  callback: FunCallback<P, R>;
  options?: FunOptions;
};

export type FunOptions = {
  concurrency?: number;
};

export class Fun<P, R> {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _callback: FunCallback<P, R>;
  private _options: FunOptions;
  private _subscription!: Subscription<P>;
  
  public ref: FunRef<P, R>;
  
  constructor(params: FunParams<P, R>) {
    const {
      telemetry,
      core,
      name,
      callback,
      options,
    } = params;
    
    this._telemetry = telemetry;
    this._core = core;
    this._name = name;
    this._callback = callback;
    this._options = options ?? {};
    
    this.ref = { name };
    
    this.init = this._telemetry.wrap('init', this.init);
    this.destroy = this._telemetry.wrap('destroy', this.destroy);
  }
  
  public async init(): Promise<void> {
    const subject = this._name;
    
    this._telemetry.trace({ subject });
    
    this._subscription = await this._core.subscribe<P>(subject, async message => {
      try {
        const result = await this._callback(message.data);
        
        message.respond({ result });
      } catch (err) {
        message.respond({ error: serializeError(err) });
      }
    }, {
      queue: subject,
      concurrency: this._options.concurrency,
    });
  }
  
  public async destroy(): Promise<void> {
    await this._subscription.destroy();
    this._telemetry.destroy();
  }
}

export type FunBuilder = (name: string) => {
  params<P>(): {
    result<R>(): {
      callback(callback: FunCallback<P, R>): Promise<Fun<P, R>>;
    };
  };
};

export type FunRefBuilder = (name: string) => {
  params<P>(params?: P): {
    result<R>(): FunRef<P, R>;
  };
};
