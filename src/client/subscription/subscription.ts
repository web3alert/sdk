import { type Telemetry } from '../../types';
import { type Core } from '../../core';
import { type Timer } from '../../timer';
import { type StreamSubscription } from '../../stream';
import { type Service } from '../../service';
import { type EventCallback } from '../types';
import { type SubscriptionRef } from './types';
import {
  type TriggerDefinition,
  type InferTriggerParams,
  type InferTriggerOutput,
  type InferTriggerTest,
  type TriggerRef,
  type TriggerSubscribeParams,
  type TriggerSubscribeResult,
} from '../trigger';

export type SubscriptionTestParams<T> = {
  values: T;
};

export type SubscriptionTestResult = void;

export type SubscriptionImplParams<D extends TriggerDefinition> = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  trigger: TriggerRef<D>;
  params: InferTriggerParams<D>;
  callback: EventCallback<InferTriggerOutput<D>>;
};

export class SubscriptionImpl<D extends TriggerDefinition> {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _trigger: TriggerRef<D>;
  private _params: InferTriggerParams<D>;
  private _callback: EventCallback<InferTriggerOutput<D>>;
  private _subscription!: StreamSubscription<InferTriggerOutput<D>>;
  private _timer!: Timer;
  private _service!: Service;
  
  constructor(params: SubscriptionImplParams<D>) {
    const {
      telemetry,
      core,
      name,
      trigger,
      params: subscriptionParams,
      callback,
    } = params;
    
    this._telemetry = telemetry;
    this._core = core;
    this._name = name;
    this._trigger = trigger;
    this._params = subscriptionParams;
    this._callback = callback;
  }
  
  public async init(): Promise<void> {
    type SP = TriggerSubscribeParams<InferTriggerParams<D>>;
    type SR = TriggerSubscribeResult;
    const subscribeMethod = `trigger.${this._trigger.name}.api.subscribe`;
    const callSubscribe = async () => {
      return await this._core.call<SP, SR>(subscribeMethod, {
        params: this._params,
      });
    };
    
    const subscribeResult = await callSubscribe();
    const { stream, key } = subscribeResult;
    const subscriptionName = (key) ? `${this._name}.${key}` : this._name;
    
    this._subscription = await this._core.stream.subscribe<InferTriggerOutput<D>>(
      this._telemetry.child('events', {
        labels: {
          trigger: this._trigger.name,
          subscription: this._name,
        },
      }),
      subscriptionName,
      stream,
      async message => {
        await this._callback(message.data);
      },
      {
        filter: key,
        concurrency: 10,
      }
    );
    
    const p = this._core.options.heartbeatInterval;
    this._timer = await this._core.localTimer(`${subscriptionName}.heartbeat`, p, async () => {
      await callSubscribe();
    });
    
    this._service = await this._core.service(`subscription.${this._name}.api`);
    
    type TP = SubscriptionTestParams<InferTriggerTest<D>>;
    type TR = SubscriptionTestResult;
    await this._service.method<TP, TR>('test', async ctx => {
      try {
        const {
          values,
        } = ctx.req.data;
        
        await this.test(values);
      } catch (err) {
        this._telemetry.error({ err }, 'failed to test subscription');
        
        throw err;
      }
    });
  }
  
  public async destroy(): Promise<void> {
    await this._service.destroy();
    await this._timer.destroy();
    await this._subscription.destroy();
    this._telemetry.destroy();
  }
  
  public async test(test: InferTriggerTest<D>): Promise<void> {
    const result = await this._core.call<any, any>(`trigger.${this._trigger.name}.api.test`, {
      values: test,
      params: this._params,
    });
    const events = result.events as InferTriggerOutput<D>[];
    
    for (const event of events) {
      await this._callback(event);
    }
  }
  
  public get ref(): SubscriptionRef<D> {
    return { name: this._name };
  }
}
