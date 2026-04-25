import { type SubscriptionObjectRaw } from '@web3alert/types';
import { type Bundle, type Event } from './types';
import { type System } from './system';

export type HandleContext = {
  subscriptions: SubscriptionObjectRaw[];
  bundle: (bundle: Bundle) => Promise<void>;
  event: (bundle: string, event: Event | Event[]) => Promise<void>;
};

export type FetchResult<Task> = {
  more: boolean;
  tasks: Task[];
};

export type PollDaemon<State, Task> = {
  initialState(): Promise<State>;
  fetch(state: State): Promise<FetchResult<Task>>;
  handle(ctx: HandleContext, state: State, task: Task): Promise<State>;
};

export type PollRunnerOptions<State, Task> = {
  system: System;
  daemon: PollDaemon<State, Task>;
  pollInterval: number;
};

export class PollRunner<State, Task> {
  private system: System;
  private daemon: PollDaemon<State, Task>;
  private pollInterval: number;
  private state!: State;
  private _destroyed: boolean;
  private _timer: NodeJS.Timeout | null = null;
  
  constructor(options: PollRunnerOptions<State, Task>) {
    const {
      system,
      daemon,
      pollInterval,
    } = options;
    
    this._tick = this._tick.bind(this);
    
    this.system = system;
    this.daemon = daemon;
    this.pollInterval = pollInterval;
    this._destroyed = false;
    this._timer = null;
  }
  
  public async init(): Promise<void> {
    let state = await this.system.api.getState<State>();
    if (!state) {
      state = await this.daemon.initialState();
      await this.system.api.saveState(state);
    }
    this.state = state;
    
    this._timer = setTimeout(this._tick, 0);
  }
  
  public async destroy(): Promise<void> {
    this._destroyed = true;
    if (this._timer) {
      clearTimeout(this._timer);
    }
  }
  
  private _tick(): void {
    this._timer = null;
    this._fetch()
      .then(result => {
        if (this._destroyed) {
          return;
        }
        
        const delay = (result.more) ? 0 : this.pollInterval;
        this._timer = setTimeout(this._tick, delay);
      })
      .catch(err => {
        if (this._destroyed) {
          return;
        }
        
        this.system.log.error({ err }, 'tick failed');
        this._timer = setTimeout(this._tick, 10_000);
      })
    ;
  }
  
  private async _fetch(): Promise<FetchResult<Task>> {
    const result = await this.daemon.fetch(this.state);
    
    this.system.log.debug({ result }, 'fetch');
    
    for (const task of result.tasks) {
      const ctx: HandleContext = {
        subscriptions: this.system.subscriptions.get(),
        bundle: async bundle => {
          await this.system.api.saveBundle(bundle);
        },
        event: async (bundle, event) => {
          await this.system.bus.publishEvent(bundle, event);
        },
      };
      
      this.state = await this.daemon.handle(ctx, this.state, task);
      await this.system.api.saveState(this.state);
    }
    
    return result;
  }
}
