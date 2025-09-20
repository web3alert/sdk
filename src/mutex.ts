import { type Callback } from './types';

export class Mutex {
  private _queue: Callback[];
  private _busy: boolean;
  
  constructor() {
    this._queue = [];
    this._busy = false;
    this._next = this._next.bind(this);
  }
  
  private _next(): void {
    if (this._queue.length == 0) {
      this._busy = false;
      return;
    }
    
    this._busy = true;
    const callback = this._queue.shift()!;
    callback().then(this._next);
  }
  
  public async lock(callback: Callback): Promise<void> {
    return new Promise((resolve, reject) => {
      this._queue.push(async () => {
        try {
          await callback();
          resolve();
        } catch (err) {
          reject(err);
        }
      });
      
      if (!this._busy) {
        this._next();
      }
    });
  }
}
