import { type Telemetry } from '../types';
import { type Core } from '../core';
import { type MutexCell } from '../multimutex';
import { type BucketCell } from '../bucket';
import { Scheduler } from '../scheduler';
import { type CronExpression, parseExpression } from 'cron-parser';

export type CronCallback = (now: number) => Promise<void>;

export type CronState = {
  next: string;
};

export type CronParams = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  expression: string;
  callback: CronCallback;
};

export class Cron {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _expression: CronExpression;
  private _callback: CronCallback;
  private _mutex!: MutexCell;
  private _state!: BucketCell<CronState>;
  private _scheduler!: Scheduler;
  
  constructor(params: CronParams) {
    const {
      telemetry,
      core,
      name,
      expression,
      callback,
    } = params;
    
    this._telemetry = telemetry;
    this._core = core;
    this._name = name;
    this._expression = parseExpression(expression);
    this._callback = callback;
  }
  
  public async init(): Promise<void> {
    this._mutex = this._core.mutex(`${this._name}.mutex`);
    this._state = this._core.registry.cell(`${this._name}.state`);
    
    this._scheduler = new Scheduler({
      callback: async (now, schedule) => {
        await this._mutex.lock(async () => {
          await this._state.mutate(async (prev, write) => {
            this._expression.reset(now);
            
            if (prev) {
              const expected = Date.parse(prev.next);
              
              if (now >= expected) {
                await this._callback(expected);
                
                const next = this._expression.next();
                await write({ next: next.toISOString() });
                schedule(next.getTime() - now);
              } else {
                schedule(expected - now);
              }
            } else {
              const next = this._expression.next();
              await write({ next: next.toISOString() });
              schedule(next.getTime() - now);
            }
          });
        });
      },
    });
    
    this._scheduler.schedule(0);
  }
  
  public async destroy(): Promise<void> {
    this._scheduler.destroy();
  }
}
