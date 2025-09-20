import { type Telemetry } from '../types';
import { type Core } from '../core';
import { type MutexCell } from '../multimutex';
import { type BucketCell } from '../bucket';

export type ReducerInitCallback<T> = () => Promise<T>;
export type ReducerExecuteCallback<T> = (prev: T) => Promise<T>;

export type ReducerParams<T> = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  init: ReducerInitCallback<T>;
  execute: ReducerExecuteCallback<T>;
};

export class Reducer<T> {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _init: ReducerInitCallback<T>;
  private _execute: ReducerExecuteCallback<T>;
  private _mutex!: MutexCell;
  private _state!: BucketCell<T>;
  
  constructor(params: ReducerParams<T>) {
    const {
      telemetry,
      core,
      name,
      init,
      execute,
    } = params;
    
    this._telemetry = telemetry;
    this._core = core;
    this._name = name;
    this._init = init;
    this._execute = execute;
  }
  
  public async init(): Promise<void> {
    this._mutex = this._core.mutex(`${this._name}.mutex`);
    this._state = this._core.registry.cell(`${this._name}.state`);
  }
  
  public async destroy(): Promise<void> {
  }
  
  public async execute(): Promise<void> {
    await this._mutex.lock(async () => {
      await this._state.mutate(async (prev, write) => {
        if (prev == undefined) {
          prev = await this._init();
        }
        
        const next = await this._execute(prev);
        
        await write(next);
      });
    });
  }
}
