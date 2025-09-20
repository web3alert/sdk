import { type Destructible } from '../../types';
import { type EventCallback, type Source } from '../types';
import { type BuilderContext } from '../builders';

export type ListenerParams<T> = {
  source: Source<T>;
  callback: EventCallback<T>;
};

export class Listener<T> {
  private _source: Source<T>;
  private _callback: EventCallback<T>;
  private _subscription!: Destructible;
  
  constructor(params: ListenerParams<T>) {
    const {
      source,
      callback,
    } = params;
    
    this._source = source;
    this._callback = callback;
  }
  
  public async init(): Promise<void> {
    this._subscription = await this._source.listen(this._callback);
  }
  
  public async destroy(): Promise<void> {
    await this._subscription.destroy();
  }
}

export type ListenerBuilder = () => {
  source<T>(source: Source<T>): {
    callback(callback: EventCallback<T>): Promise<Listener<T>>;
  };
};

export function createListenerBuilder(ctx: BuilderContext): ListenerBuilder {
  return () => ({
    source: <T>(source: Source<T>) => ({
      callback: async (callback: EventCallback<T>) => {
        return await ctx.container.use(async () => {
          const listener = new Listener<T>({
            source,
            callback,
          });
          await listener.init();
          
          return listener;
        });
      },
    }),
  });
}
