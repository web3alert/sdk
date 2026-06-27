import { type BaseClientStackParams, BaseClientStack } from './base-client';
import { Namespace } from '../namespace';
import { type ActionInput, type EventCallback } from './types';
import { type StreamSubscriptionOptions } from '../stream';
import { ClientLocal } from './local';
import { Refs } from './refs';
import { type SequenceCallback, type SequencerOptions, Sequencer } from './sequencer';
import { type FunRef, type FunBuilder, Fun } from './fun';
import {
  type TriggerDefinition,
  type InferTriggerParams,
  type InferTriggerOutput,
  type TriggerRunner,
  type TriggerTester,
  type TriggerLifecycleHooks,
  type TriggerRef,
  type Trigger,
  TriggerImpl,
} from './trigger';
import { type Subscription, SubscriptionImpl } from './subscription';
import { type ActionRef, type ActionBuilder, Action } from './action';
import { type CronCallback, Cron } from './cron';
import { type ReducerInitCallback, type ReducerExecuteCallback, Reducer } from './reducer';
import { type TestNamespace, test } from './test';

export type ClientSequencerParams = {
  name: string;
  callback: SequenceCallback;
  options?: Partial<SequencerOptions>;
};

export type ClientTriggerParams<D extends TriggerDefinition> = {
  name: string;
  runner: TriggerRunner<D>;
  tester: TriggerTester<D>;
  hooks?: TriggerLifecycleHooks<InferTriggerParams<D>>;
};

export type ClientSubscribeOptions = Pick<Partial<StreamSubscriptionOptions>, 'concurrency'>;

export type ClientSubscribeParams<D extends TriggerDefinition> = {
  name: string;
  trigger: TriggerRef<D>;
  params?: InferTriggerParams<D>;
  callback: EventCallback<InferTriggerOutput<D>>;
  options?: ClientSubscribeOptions;
};

export type ClientCronParams = {
  name: string;
  expression: string;
  callback: CronCallback;
};

export type ClientReducerParams<T> = {
  name: string;
  init: ReducerInitCallback<T>;
  execute: ReducerExecuteCallback<T>;
};

export type ClientParams = BaseClientStackParams & {
  namespace: Namespace;
};

export class Client extends BaseClientStack {
  public namespace: Namespace;
  public local: ClientLocal;
  public ref: Refs;
  
  public fun: FunBuilder;
  public action: ActionBuilder;
  public test: TestNamespace;
  
  constructor(params: ClientParams) {
    super(params);
    
    const {
      namespace,
    } = params;
    
    this.namespace = namespace;
    this.local = new ClientLocal({
      telemetry: this.telemetry,
      core: this.core,
      name: this.name,
      container: this.container,
      namespace,
    });
    this.ref = new Refs();
    
    this.fun = name => ({
      params: () => ({
        result: () => ({
          callback: async callback => {
            return this._use(async () => {
              const fun = new Fun({
                telemetry: this.telemetry.child(`fun.${name}`),
                core: this.core,
                name: `${this.namespace.workspace}.${this.namespace.project}.${name}`,
                callback,
              });
              await fun.init();
              
              return fun;
            });
          },
        }),
      }),
    });
    
    this.action = name => ({
      params: () => ({
        callback: async callback => {
          return this._use(async () => {
            const action = new Action({
              telemetry: this.telemetry.child(`action.${name}`),
              core: this.core,
              name: `${this.namespace.workspace}.${this.namespace.project}.${name}`,
              callback,
            });
            await action.init();
            
            return action;
          });
        },
      }),
    });
    
    this.test = test.apply(this);
  }
  
  public async call<P, R>(fun: FunRef<P, R>, params?: P): Promise<R> {
    const result = await this.core.call<any, any>(fun.name, params ?? fun.params);
    
    return result;
  }
  
  public async execute<P>(action: ActionRef<P>, input: ActionInput<P>): Promise<void> {
    const fun = this.ref.fun(`${action.name}.fun`)
      .params<ActionInput<P>>()
      .result<void>()
    ;
    
    await this.call(fun, input);
  }
  
  public async sequencer(
    params: ClientSequencerParams,
  ): Promise<Sequencer> {
    const {
      name,
      callback,
      options,
    } = params;
    
    return await this._use(async () => {
      const sequencer = new Sequencer({
        telemetry: this.telemetry.child(name),
        core: this.core,
        name: `${this.namespace.workspace}.${this.namespace.project}.${name}`,
        callback,
        options,
      });
      await sequencer.init();
      
      return sequencer;
    });
  }
  
  public async trigger<D extends TriggerDefinition>(
    params: ClientTriggerParams<D>,
  ): Promise<Trigger<D>> {
    const {
      name,
      runner,
      tester,
      hooks,
    } = params;
    
    return this._use(async () => {
      const trigger = new TriggerImpl({
        telemetry: this.telemetry.child(name),
        core: this.core,
        name: this.namespace.trigger(name),
        runner,
        tester,
        hooks,
      });
      await trigger.init();
      
      return trigger;
    });
  }
  
  public async subscribe<D extends TriggerDefinition>(
    params: ClientSubscribeParams<D>,
  ): Promise<Subscription<D>> {
    const {
      name,
      trigger,
      params: subscriptionParams,
      callback,
      options,
    } = params;
    
    return this._use(async () => {
      const subscription = new SubscriptionImpl({
        telemetry: this.telemetry.child(name),
        core: this.core,
        name: `${this.namespace.workspace}.${this.namespace.project}.${name}`,
        trigger,
        params: subscriptionParams,
        callback,
        options,
      });
      await subscription.init();
      
      return subscription;
    });
  }
  
  public async cron(
    params: ClientCronParams,
  ): Promise<Cron> {
    const {
      name,
      expression,
      callback,
    } = params;
    
    return this._use(async () => {
      const cron = new Cron({
        telemetry: this.telemetry.child(name),
        core: this.core,
        name: `${this.namespace.workspace}.${this.namespace.project}.${name}`,
        expression,
        callback,
      });
      await cron.init();
      
      return cron;
    });
  }
  
  public async reducer<T>(
    params: ClientReducerParams<T>,
  ): Promise<Reducer<T>> {
    const {
      name,
      init,
      execute,
    } = params;
    
    return this._use(async () => {
      const reducer = new Reducer<T>({
        telemetry: this.telemetry.child(name),
        core: this.core,
        name: `${this.namespace.workspace}.${this.namespace.project}.${name}`,
        init,
        execute,
      });
      await reducer.init();
      
      return reducer;
    });
  }
}
