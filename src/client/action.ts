import { type Telemetry } from '../types';
import { type Core } from '../core';
import { type ActionInput } from './types';
import { Fun } from './fun';

export type ActionCallback<P> = (input: ActionInput<P>) => Promise<void>;

export type ActionRef<P> = {
  name: string;
};

export type ActionParams<P> = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  callback: ActionCallback<P>;
};

function getActionConcurrency(): number {
  const value = Number(process.env['WEB3ALERT_ACTION_CONCURRENCY']);

  if (!Number.isFinite(value)) {
    return 32;
  }

  return Math.min(Math.max(Math.trunc(value), 1), 256);
}

export class Action<P> {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _callback: ActionCallback<P>;
  private _fun!: Fun<ActionInput<P>, void>;
  
  public ref: ActionRef<P>;
  
  constructor(params: ActionParams<P>) {
    const {
      telemetry,
      core,
      name,
      callback,
    } = params;
    
    this._telemetry = telemetry;
    this._core = core;
    this._name = name;
    this._callback = callback;
    
    this.ref = { name };
    
    this.init = this._telemetry.wrap('init', this.init);
    this.destroy = this._telemetry.wrap('destroy', this.destroy);
  }
  
  public async init(): Promise<void> {
    const fun = new Fun<ActionInput<P>, void>({
      telemetry: this._telemetry.child('fun'),
      core: this._core,
      name: `${this._name}.fun`,
      callback: async params => {
        await this._callback(params);
      },
      options: {
        concurrency: getActionConcurrency(),
      },
    });
    await fun.init();
    
    this._fun = fun;
  }
  
  public async destroy(): Promise<void> {
    await this._fun.destroy();
    this._telemetry.destroy();
  }
}

export type ActionBuilder = (name: string) => {
  params<P>(): {
    callback(callback: ActionCallback<P>): Promise<Action<P>>;
  };
};

export type ActionRefBuilder = (name: string) => {
  params<P>(): ActionRef<P>;
};
