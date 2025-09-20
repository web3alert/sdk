import { type Mortal, type SpawnCallback } from './types';
import { kill } from './kill';
import { retry } from './utils';

export type ImmortalParams = {
  spawn: SpawnCallback;
};

export class Immortal {
  private _spawn: SpawnCallback;
  private _mortal: Mortal | undefined;
  private _terminator: AbortController;
  private _starting: Promise<void> | undefined;
  private _stopping: Promise<void> | undefined;
  private _restart: boolean;
  
  constructor(params: ImmortalParams) {
    const {
      spawn,
    } = params;
    
    this._spawn = spawn;
    this._mortal = undefined;
    this._terminator = new AbortController();
    this._starting = undefined;
    this._stopping = undefined;
    this._restart = false;
  }
  
  public async init(): Promise<void> {
    this._startInBackground();
  }
  
  public async destroy(): Promise<void> {
    this._terminator.abort(new Error('destroyed'));
    
    if (this._starting) {
      await this._starting;
    }
    
    if (this._stopping) {
      await this._stopping;
    }
    
    if (this._mortal) {
      await this._stop();
    }
  }
  
  private async _start(): Promise<void> {
    if (this._mortal) {
      throw new Error('mortal already exists');
    }
    
    await retry(async () => {
      this._mortal = await this._spawn();
    }, {
      retries: Infinity,
      signal: this._terminator.signal,
      minDelay: 2_000,
    });
  }
  
  private async _stop(): Promise<void> {
    if (!this._mortal) {
      throw new Error('mortal does not exist');
    }
    
    await kill(this._mortal);
    this._mortal = undefined;
  }
  
  private _wake(): void {
    if (this._terminator.signal.aborted) {
      return;
    }
    
    if (this._starting || this._stopping) {
      return;
    }
    
    if (this._mortal) {
      if (this._restart) {
        this._restart = false;
        this._stopInBackground();
      }
    } else {
      this._restart = false;
      this._startInBackground();
    }
  }
  
  private _startInBackground(): void {
    this._starting = this._start().then(() => {
      this._starting = undefined;
      this._wake();
    }).catch(err => { /* ignore */ });
  }
  
  private _stopInBackground(): void {
    this._stopping = this._stop().then(() => {
      this._stopping = undefined;
      this._wake();
    });
  }
  
  public restart(): void {
    this._restart = true;
    this._wake();
  }
}
