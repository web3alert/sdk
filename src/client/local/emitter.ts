import { type Destructible } from '../../types';
import { type EventCallback } from '../types';
import { type BuilderContext } from '../builders';

export class Emitter<T> {
  private _callbacks: Set<EventCallback<T>>;
  
  constructor() {
    this._callbacks = new Set();
  }
  
  public async init(): Promise<void> {
    // no-op
  }
  
  public async destroy(): Promise<void> {
    // no-op
  }
  
  public async listen(callback: EventCallback<T>): Promise<Destructible> {
    this._callbacks.add(callback);
    
    return {
      destroy: async () => {
        this._callbacks.delete(callback);
      },
    };
  }
  
  public async publish(value: T): Promise<void> {
    for (const callback of this._callbacks) {
      await callback(value);
    }
  }
}

export type EmitterBuilder = () => {
  event<T>(): Promise<Emitter<T>>;
};

export function createEmitterBuilder(ctx: BuilderContext): EmitterBuilder {
  return () => ({
    event: async <T>() => {
      return await ctx.container.use(async () => {
        const emitter = new Emitter<T>();
        await emitter.init();
        
        return emitter;
      });
    },
  });
}
