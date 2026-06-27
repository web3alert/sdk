import { type Destructible, type Telemetry } from '../../types';
import { type Timer } from '../../timer';
import { setup, oneshot, hashOf } from '../../utils';
import { type Service } from '../../service';
import { type Core } from '../../core';
import { type StreamRef, Stream } from '../../stream';
import { type BucketSlice, type BucketCell } from '../../bucket';
import { type MutexCell } from '../../multimutex';
import { getNumber } from '../../env';
import { SliceSpawner } from '../../slice-spawner';
import { Emitter } from '../local/emitter';
import {
  type TriggerDefinition,
  type InferTriggerParams,
  type InferTriggerInput,
  type InferTriggerOutput,
  type InferTriggerTest,
  type TriggerLifecycleHooks,
  type TriggerTask,
  type TriggerRef,
  type Trigger,
  type TriggerRunner,
  type TriggerTester,
} from './types';

export type TriggerState = {
  lastCleanupAt: number;
};

const DEFAULT_TRIGGER_STREAM_MAX_BYTES = 100 * 1024 * 1024;
const MIN_TRIGGER_STREAM_MAX_BYTES = 1024 * 1024;
const MAX_TRIGGER_STREAM_MAX_BYTES = 10 * 1024 * 1024 * 1024;

function triggerStreamMaxBytes(): number {
  const value = getNumber('WEB3ALERT_TRIGGER_STREAM_MAX_BYTES', DEFAULT_TRIGGER_STREAM_MAX_BYTES);
  if (value < MIN_TRIGGER_STREAM_MAX_BYTES || value > MAX_TRIGGER_STREAM_MAX_BYTES) {
    throw new Error(
      `WEB3ALERT_TRIGGER_STREAM_MAX_BYTES should be between ${MIN_TRIGGER_STREAM_MAX_BYTES} and ${MAX_TRIGGER_STREAM_MAX_BYTES}`,
    );
  }

  return Math.trunc(value);
}

function taskSubscriberLeaseRefreshIntervalMs(heartbeatIntervalMs: number): number {
  return Math.max(heartbeatIntervalMs, heartbeatIntervalMs * 2);
}

function isFreshTaskSubscriberLease<P>(
  task: TriggerTask<P> | undefined,
  subscriberKey: string,
  nowMs: number,
  refreshIntervalMs: number,
): boolean {
  const timestamp = task?.subscribers?.[subscriberKey];
  if (!timestamp) {
    return false;
  }

  const timestampMs = new Date(timestamp).getTime();
  return Number.isFinite(timestampMs) && nowMs - timestampMs < refreshIntervalMs;
}

export type TriggerSubscribeParams<P> = {
  params: P;
  subscriber?: string;
};

export type TriggerSubscribeResult = {
  stream: StreamRef;
  key: string | undefined;
};

export type TriggerUnsubscribeParams = {
  key: string;
  subscriber?: string;
};

export type TriggerTestParams<P, T> = {
  params: P;
  values: T;
  itemIndex?: number;
};

export type TriggerTestResult<E> = {
  events: E[];
  count?: number;
};

export type TriggerImplParams<D extends TriggerDefinition> = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  runner: TriggerRunner<D>;
  tester: TriggerTester<D>;
  hooks?: TriggerLifecycleHooks<InferTriggerParams<D>>;
};

export class TriggerImpl<D extends TriggerDefinition> implements Trigger<D> {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _runner: TriggerRunner<D>;
  private _tester: TriggerTester<D>;
  private _hooks?: TriggerLifecycleHooks<InferTriggerParams<D>>;
  private _emitter!: Emitter<InferTriggerInput<D>>;
  private _stream!: Stream<InferTriggerOutput<D>>;
  private _tasks!: BucketSlice<TriggerTask<InferTriggerParams<D>>>;
  private _spawner!: SliceSpawner<TriggerTask<InferTriggerParams<D>>, Destructible>;
  private _mutex!: MutexCell;
  private _state!: BucketCell<TriggerState>;
  private _timer!: Timer;
  private _service!: Service;
  private _tasksActive = false;
  
  constructor(params: TriggerImplParams<D>) {
    const {
      telemetry,
      core,
      name,
      runner,
      tester,
      hooks,
    } = params;
    
    this._telemetry = telemetry;
    this._core = core;
    this._name = name;
    this._runner = runner;
    this._tester = tester;
    this._hooks = hooks;
    
    this.init = this._telemetry.wrap('init', this.init);
    this.destroy = this._telemetry.wrap('destroy', this.destroy);
  }
  
  public async init(): Promise<void> {
    await setup(async use => {
      this._emitter = new Emitter();
      
      this._stream = await use(async () => {
        const stream = new Stream<InferTriggerOutput<D>>({
          telemetry: this._telemetry.child('events', { labels: { trigger: this._name } }),
          core: this._core,
          name: `trigger.${this._name}.events`,
          options: {
            maxSize: triggerStreamMaxBytes(),
          },
        });
        await stream.init();
        
        return stream;
      });
      
      this._tasks = this._core.registry.slice(`${this._name}.tasks`);
      
      this._spawner = await use(async () => {
        const spawner = new SliceSpawner<TriggerTask<InferTriggerParams<D>>, Destructible>({
          core: this._core,
          name: this._name,
          slice: this._tasks,
          changed: (prev, next) => hashOf(prev.params) != hashOf(next.params),
          onUpdate: async update => {
            if (!this._hooks?.onTaskChange) {
              return;
            }

            try {
              if (update.operation == 'PUT') {
                await this._hooks.onTaskChange({
                  operation: 'PUT',
                  key: update.key,
                  online: update.online,
                  revision: update.revision,
                  task: update.value,
                });
                return;
              }

              await this._hooks.onTaskChange({
                operation: 'DEL',
                key: update.key,
                online: update.online,
                revision: update.revision,
              });
            } catch (err) {
              this._telemetry.warn({
                err,
                trigger: this._name,
                key: update.key,
                operation: update.operation,
              }, 'trigger task lifecycle hook failed');
            }
          },
          callback: async params => {
            const { key, value } = params;
            
            this._telemetry.debug({ key, value }, 'spawn task');
            
            const mutex = this._core.mutex(`${this._name}.data.tasks.${key}.mutex`);
            const state = this._core.registry.cell(`${this._name}.data.tasks.${key}.state`);
            
            const execute = await this._runner({
              params: value.params,
              reduce: async callback => {
                let result: any;
                
                await mutex.lock(async () => {
                  await state.mutate(async (prev, write) => {
                    result = await callback(prev as any, write as any);
                  });
                });
                
                return result;
              },
              publish: async output => {
                const items = Array.isArray(output) ? output : [output];
                
                for (const item of items) {
                  await this._stream.publishToSubject(key, item);
                }
              },
            });
            
            return await this._emitter.listen(execute);
          },
        });
        await spawner.init();
        
        return spawner;
      });
    
      this._mutex = this._core.mutex(`${this._name}.cleanup-mutex`);
      this._state = this._core.registry.cell(`${this._name}.cleanup-state`);
      
      const p = this._core.options.heartbeatInterval * 2;
      const timeout = this._core.options.heartbeatInterval * 4;
      this._timer = await this._core.localTimer(`${this._name}.cleanup-timer`, p, async () => {
        await this._mutex.lock(async () => {
          await this._state.mutate(async (state, write) => {
            if (!state) {
              state = {
                lastCleanupAt: 0,
              };
            }
            
            const now = Date.now();
            
            if (now - state.lastCleanupAt < p) {
              return;
            }
            
            const keys = await this._tasks.keys();
            
            for (const key of keys) {
              const fixedKey = key.slice(`${this._name}.tasks.`.length);
              
              const task = await this._tasks.get(fixedKey);
              if (!task) {
                continue;
              }

              if (task.subscribers) {
                const subscribers = Object.fromEntries(
                  Object.entries(task.subscribers)
                    .filter(([, timestamp]) => now - new Date(timestamp).getTime() < timeout),
                );

                if (Object.keys(subscribers).length == 0) {
                  this._telemetry.debug({ key: fixedKey }, 'task evicted by hearbeat timeout');
                  await this._tasks.delete(fixedKey);
                } else if (Object.keys(subscribers).length != Object.keys(task.subscribers).length) {
                  await this._tasks.put(fixedKey, {
                    ...task,
                    subscribers,
                    timestamp: Object.values(subscribers).sort().at(-1) ?? task.timestamp,
                  });
                }

                continue;
              }
              
              const lastOnlineAt = new Date(task.timestamp).getTime();
              
              if (now - lastOnlineAt < timeout) {
                continue;
              }
              
              this._telemetry.debug({ key: fixedKey }, 'task evicted by hearbeat timeout');
              
              await this._tasks.delete(fixedKey);
            }

            await this._syncTasksActiveState();
            
            state.lastCleanupAt = now;
            
            await write(state);
          });
        });
      });
      
      this._service = await use(async () => {
        return await this._core.service(`trigger.${this._name}.api`);
      });
      
      type SP = TriggerSubscribeParams<InferTriggerParams<D>>;
      type SR = TriggerSubscribeResult;
      await this._service.method<SP, SR>('subscribe', async ctx => {
        try {
          const { params, subscriber } = ctx.req.data;
          
          const now = new Date();
          const nowMs = now.getTime();
          const timestamp = now.toISOString();
          const key = hashOf(params);
          const subscriberKey = subscriber ?? 'default';
          const refreshIntervalMs = taskSubscriberLeaseRefreshIntervalMs(this._core.options.heartbeatInterval);
          let taskWritten = false;
          
          await this._tasks.mutate(key, async (prev, write) => {
            if (isFreshTaskSubscriberLease(prev, subscriberKey, nowMs, refreshIntervalMs)) {
              return;
            }

            taskWritten = true;
            await write({
              timestamp,
              params,
              subscribers: {
                ...(prev?.subscribers ?? {}),
                [subscriberKey]: timestamp,
              },
            });
          });

          if (taskWritten || !this._tasksActive) {
            await this._syncTasksActiveState();
          }
          
          ctx.res.data = { stream: this._stream.ref, key };
        } catch (err) {
          this._telemetry.error({ method: 'subscribe', err });
          throw err;
        }
      });

      await this._service.method<TriggerUnsubscribeParams, void>('unsubscribe', async ctx => {
        try {
          const { key, subscriber } = ctx.req.data;
          const task = await this._tasks.get(key);
          if (!task) {
            return;
          }

          if (!subscriber || !task.subscribers) {
            await this._tasks.delete(key);
            await this._syncTasksActiveState();
            return;
          }

          const subscribers = { ...task.subscribers };
          delete subscribers[subscriber];

          if (Object.keys(subscribers).length == 0) {
            await this._tasks.delete(key);
          } else {
            await this._tasks.put(key, {
              ...task,
              subscribers,
              timestamp: Object.values(subscribers).sort().at(-1) ?? task.timestamp,
            });
          }

          await this._syncTasksActiveState();
        } catch (err) {
          this._telemetry.error({ method: 'unsubscribe', err });
          throw err;
        }
      });
      
      type TP = TriggerTestParams<InferTriggerParams<D>, InferTriggerTest<D>>;
      type TR = TriggerTestResult<InferTriggerOutput<D>>;
      await this._service.method<TP, TR>('test', async ctx => {
        this._telemetry.debug({ method: 'test', data: ctx.req.data });
        
        try {
          const { values: test, params, itemIndex } = ctx.req.data;
          const output = await this.test(test, params);
          const total = output.length;

          ctx.res.data = typeof itemIndex == 'number'
            ? {
                events: output[itemIndex] == undefined ? [] : [output[itemIndex]],
                count: total,
              }
            : {
                events: output,
                count: total,
              };
        } catch (err) {
          this._telemetry.error({ method: 'test', err });
          throw err;
        }
      });

      await this._syncTasksActiveState();
    });
  }
  
  public async destroy(): Promise<void> {
    if (this._tasksActive) {
      await this._hooks?.onTasksActiveChange?.(false);
      this._tasksActive = false;
    }
    await this._service.destroy();
    await this._timer.destroy();
    await this._spawner.destroy();
    await this._stream.destroy();
    this._telemetry.destroy();
  }
  
  public async execute(input: InferTriggerInput<D>): Promise<void> {
    await this._emitter.publish(input);
  }
  
  public async test(
    test: InferTriggerTest<D>,
    params: InferTriggerParams<D>,
  ): Promise<InferTriggerInput<D>[]> {
    const input = await this._tester(test);
    const samples = Array.isArray(input) ? input : [input];
    
    return await oneshot(async use => {
      const acc: InferTriggerOutput<D>[] = [];
      let state: any = undefined;
      
      const execute = await this._runner({
        params,
        reduce: async callback => {
          return await callback(state, async next => {
            state = next;
          });
        },
        publish: async output => {
          const items = Array.isArray(output) ? output : [output];
          
          acc.push(...items);
        },
      });
      
      for (const sample of samples) {
        await execute(sample);
      }
      
      return acc;
    });
  }
  
  public get ref(): TriggerRef<D> {
    return { name: this._name };
  }

  private async _syncTasksActiveState(): Promise<void> {
    if (!this._hooks?.onTasksActiveChange) {
      return;
    }

    const keys = await this._tasks.keys();
    const nextActive = keys.length > 0;
    if (nextActive == this._tasksActive) {
      return;
    }

    this._tasksActive = nextActive;
    await this._hooks.onTasksActiveChange(nextActive);
  }
}
