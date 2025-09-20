import { type Callback } from '../types';

export type ErrorHandler = (err: unknown) => void;

export type SchedulerOptions = {
  error?: ErrorHandler;
};

export class Scheduler {
  private error?: ErrorHandler;
  
  constructor(options?: SchedulerOptions) {
    const {
      error,
    } = options ?? {};
    
    this.error = error;
  }
  
  public interval(ms: number, callback: Callback): Callback {
    let promise: Promise<void> | null = null;
    
    const tick = () => {
      if (promise) {
        return;
      }
      
      promise = callback()
        .then(() => {
          promise = null;
        })
        .catch(err => {
          promise = null;
          
          if (this.error) {
            this.error(err);
          }
        })
      ;
    };
    
    const interval = setInterval(tick, ms);
    tick();
    
    return async () => {
      if (promise) {
        await promise;
      }
      
      clearInterval(interval);
    };
  }
}
