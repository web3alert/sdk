import { type Destructible } from './types';
import { Container } from './container';
import { caught } from './utils';

export type Dependencies = { [key: string]: Token<any, any, any> };
export type InferInterface<T> = T extends Token<infer I, any, any> ? I : never;
export type InferContext<D> = D extends Dependencies ? {
  [K in keyof D]: InferInterface<D[K]>;
} : never;

export type Provider<
  I extends Destructible = Destructible,
  C extends unknown = void,
> = () => Promise<Factory<I, C, any>>;

export type Factory<
  I extends Destructible = Destructible,
  C extends unknown = void,
  D extends Dependencies = {},
> = (context: InferContext<D>, config: C) => Promise<I>;

export type Token<
  I extends Destructible = Destructible,
  C extends unknown = void,
  D extends Dependencies = {},
> = {
  name: string;
  provider: Provider<I, C>;
  dependencies: D;
};

export type InferFactory<T> = T extends Token<infer I, infer C, infer D> ? Factory<I, C, D> : never;

export type TokenBuilder<
  I extends Destructible = Destructible,
  C extends unknown = void,
  D extends Dependencies = {},
> = {
  config<NC>(): TokenBuilder<I, NC, D>;
  interface<NI extends Destructible>(): TokenBuilder<NI, C, D>;
  provider(callback: Provider<I, C>): TokenBuilder<I, C, D>;
  dependencies<ND extends Dependencies>(dependencies: ND): TokenBuilder<I, C, ND>;
  close(): Token<I, C, D>;
};

export function token(name: string): TokenBuilder {
  const token: Token = {
    name,
    provider: async () => {
      throw new Error(`no provider for token '${name}'`);
    },
    dependencies: {},
  };
  
  const builder: TokenBuilder<any, any, any> = {
    config() {
      return builder;
    },
    interface() {
      return builder;
    },
    provider(callback) {
      token.provider = callback;
      return builder;
    },
    dependencies(dependencies) {
      token.dependencies = dependencies;
      return builder;
    },
    close: () => {
      return token;
    },
  };
  
  return builder;
}

export function provider<T extends Token<any, any, any>>(
  token: T,
  factory: InferFactory<T>,
): InferFactory<T> {
  return factory;
}

export type RegisteredToken = {
  body: Token;
  factory?: Factory;
};

export type PluginModule = {
  name?: string;
  plugin: string;
  config?: unknown;
  dependencies?: Record<string, string>;
};

export type Wiring = PluginModule[];

export type Prefab = {
  name: string;
  token: Token;
  factory: Factory<any, any, any>;
  config?: unknown;
  dependencies?: Record<string, string>;
};

export class Registry {
  private _plugins: Map<string, RegisteredToken>;
  
  constructor() {
    this._plugins = new Map();
  }
  
  public add(token: Token<any, any, any>): void {
    this._plugins.set(token.name, { body: token });
  }
  
  public async bootstrap(wiring: Wiring): Promise<Destructible> {
    const container = new Container();
    const prefabs = new Map<string, Prefab>();
    
    for (const item of wiring) {
      const token = this._plugins.get(item.plugin);
      if (!token) {
        throw new Error(`unknown plugin '${item.plugin}'`);
      }
      
      const name = item.name ?? item.plugin;
      if (prefabs.has(name)) {
        throw new Error(`duplicate instance '${name}'`);
      }
      
      if (!token.factory) {
        token.factory = await token.body.provider();
      }
      
      prefabs.set(name, {
        name,
        token: token.body,
        factory: token.factory,
        config: item.config,
        dependencies: item.dependencies,
      });
    }
    
    const instances = new Map<string, Destructible>();
    const stack = new Set<string>();
    const spawn = async (name: string) => {
      if (instances.has(name)) {
        return instances.get(name);
      }
      
      if (stack.has(name)) {
        throw new Error(`circular dependency detected for '${name}'`);
      }
      
      stack.add(name);
      
      const prefab = prefabs.get(name);
      if (!prefab) {
        throw new Error(`unknown instance '${name}'`);
      }
      
      const context = {} as Record<string, any>;
      
      for (const tokenName in prefab.token.dependencies) {
        const instanceName = prefab.dependencies ? prefab.dependencies[tokenName] : tokenName;
        const instance = await spawn(instanceName);
        
        context[tokenName] = instance;
      }
      
      const instance = await container.use(async () => {
        return await prefab.factory(context, prefab.config);
      });
      
      instances.set(name, instance);
      stack.delete(name);
      
      return instance;
    };
    
    for (const name of prefabs.keys()) {
      await spawn(name);
    }
    
    return await caught(async () => {
      return container;
    }, container);
  }
}
