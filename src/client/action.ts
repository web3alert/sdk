import { type Telemetry } from '../types';
import { type Core } from '../core';
import { type ActionInput } from './types';
import { Fun } from './fun';
import { actionDeliveryFence, actionDeliveryError, type ActionDeliveryContext } from '../action-delivery';

export type ActionCallback<P> = (input: ActionInput<P>, delivery: ActionDeliveryContext) => Promise<void>;
export type ActionOptions = { guardedDelivery?: boolean };

export type ActionRef<P> = {
  name: string;
};

export type ActionParams<P> = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  callback: ActionCallback<P>;
  options?: ActionOptions;
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
  private _guardedFun?: Fun<ActionInput<P>, void>;
  private _options: ActionOptions;
  
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
    this._options = params.options ?? {};
    
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
        if (params.delivery) throw actionDeliveryError('unavailable');
        await this._callback(params, { start: async request => await request() });
      },
      options: {
        concurrency: getActionConcurrency(),
      },
    });
    await fun.init();
    
    this._fun = fun;
    if (this._options.guardedDelivery) {
      const fence = await actionDeliveryFence(this._core);
      this._guardedFun = new Fun({ telemetry: this._telemetry.child('guarded'), core: this._core,
        name: `${this._name}.guarded.fun`, options: { concurrency: getActionConcurrency() },
        callback: async input => {
          if (!input.delivery) throw actionDeliveryError('unavailable');
          await fence.check(input.delivery);
          await this._callback(input, { start: request => fence.start(input.delivery!, request) });
        } });
      await this._guardedFun.init();
    }
  }
  
  public async destroy(): Promise<void> {
    await this._guardedFun?.destroy();
    await this._fun.destroy();
    this._telemetry.destroy();
  }
}

export type ActionBuilder = (name: string) => {
  params<P>(): {
    callback(callback: ActionCallback<P>, options?: ActionOptions): Promise<Action<P>>;
  };
};

export type ActionRefBuilder = (name: string) => {
  params<P>(): ActionRef<P>;
};
