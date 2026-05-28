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
  type TriggerUnsubscribeParams,
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
  private _subscriptionKey?: string;
  private _subscriptionBroken: boolean;
  private _restore?: Promise<void>;
  private _destroying: boolean;
  
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
    this._subscriptionBroken = false;
    this._destroying = false;
  }
  
  public async init(): Promise<void> {
    type SP = TriggerSubscribeParams<InferTriggerParams<D>>;
    type SR = TriggerSubscribeResult;
    const subscribeMethod = `trigger.${this._trigger.name}.api.subscribe`;
    const callSubscribe = async () => {
      return await this._core.call<SP, SR>(subscribeMethod, {
        params: this._params,
        subscriber: this._name,
      });
    };
    
    const attachStream = async (subscribeResult: TriggerSubscribeResult): Promise<void> => {
      const { stream, key } = subscribeResult;
      const subscriptionName = (key) ? `${this._name}.${key}` : this._name;

      const subscription = await this._core.stream.subscribe<InferTriggerOutput<D>>(
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
          error: err => {
            if (this._destroying) {
              return;
            }
            
            this._subscriptionBroken = true;
            this._telemetry.warn({
              err,
              trigger: this._trigger.name,
              subscription: this._name,
            }, 'runtime stream subscription failed, scheduling restore');
            void restoreStream('stream-error');
          },
        },
      );

      this._subscriptionKey = key;
      this._subscription = subscription;
      this._subscriptionBroken = false;
    };
    
    const restoreStream = async (reason: string): Promise<void> => {
      if (this._destroying || this._restore) {
        return await this._restore;
      }
      
      this._restore = (async () => {
        try {
          const subscribeResult = await callSubscribe();
          const previous = this._subscription;
          this._subscriptionBroken = true;
          this._subscription = undefined as unknown as typeof this._subscription;
          
          if (previous) {
            try {
              await previous.destroy();
            } catch (err) {
              this._telemetry.warn({
                err,
                trigger: this._trigger.name,
                subscription: this._name,
                reason,
              }, 'failed to destroy broken runtime stream subscription');
            }
          }
          
          await attachStream(subscribeResult);
          this._telemetry.info({
            trigger: this._trigger.name,
            subscription: this._name,
            reason,
            key: this._subscriptionKey,
          }, 'runtime stream subscription restored');
        } catch (err) {
          this._subscriptionBroken = true;
          this._telemetry.error({
            err,
            trigger: this._trigger.name,
            subscription: this._name,
            reason,
          }, 'failed to restore runtime stream subscription');
        } finally {
          this._restore = undefined;
        }
      })();
      
      return await this._restore;
    };
    
    const subscribeResult = await callSubscribe();
    await attachStream(subscribeResult);
    
    const p = this._core.options.heartbeatInterval;
    this._timer = await this._core.localTimer(`${this._name}.heartbeat`, p, async () => {
      await callSubscribe();
      if (this._subscriptionBroken) {
        await restoreStream('heartbeat');
      }
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
    this._destroying = true;
    await this._service.destroy();
    await this._timer.destroy();
    if (this._restore) {
      await this._restore;
    }
    if (this._subscriptionKey) {
      try {
        await this._core.call<TriggerUnsubscribeParams, void>(`trigger.${this._trigger.name}.api.unsubscribe`, {
          key: this._subscriptionKey,
          subscriber: this._name,
        });
      } catch (err) {
        this._telemetry.warn({
          err,
          trigger: this._trigger.name,
          subscription: this._name,
          key: this._subscriptionKey,
        }, 'failed to unsubscribe runtime trigger task');
      }
      this._subscriptionKey = undefined;
    }
    if (this._subscription) {
      await this._subscription.destroy();
    }
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
