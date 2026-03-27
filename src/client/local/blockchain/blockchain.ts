import { type Gauge, type Telemetry, type Destructible } from '../../../types';
import { defaults, setup } from '../../../utils';
import { Container } from '../../../container';
import { Multisource } from '../../utils';
import { type BlockCallback } from './types';
import { type Backend, type BackendFactory } from './backend';
import { Upstream } from './upstream';

export type BlockchainUpstreamSpec<S> = {
  name: string;
  spec: S;
};

export type BlockchainOptions = {
  upstreamTimeout: number;
  upstreamAcquireTimeout: number;
  upstreamAcquirePoll: number;
};

export type BlockchainParams<S, B extends Backend> = {
  telemetry: Telemetry;
  specs: BlockchainUpstreamSpec<S>[];
  factory: BackendFactory<S, B>;
  options?: Partial<BlockchainOptions>;
};

export class Blockchain<S, B extends Backend> {
  private _telemetry: Telemetry;
  private _specs: BlockchainUpstreamSpec<S>[];
  private _factory: BackendFactory<S, B>;
  private _options: BlockchainOptions;
  private _upstreams: Map<string, Upstream<S, B>>;
  private _block: number;
  private _source!: Multisource<number>;
  
  private _gaugeLatestBlock: Gauge;
  private _gaugeUpstreamOnline: Gauge;
  private _gaugeUpstreamCurrentBlock: Gauge;
  
  constructor(params: BlockchainParams<S, B>) {
    const {
      telemetry,
      specs,
      factory,
      options,
    } = params;
    
    this._setup = this._setup.bind(this);
    
    this._telemetry = telemetry;
    this._specs = specs;
    this._factory = factory;
    this._options = defaults(options, {
      upstreamTimeout: 30_000,
      upstreamAcquireTimeout: 1_000,
      upstreamAcquirePoll: 100,
    });
    this._upstreams = new Map();
    this._block = -1;
    
    this._gaugeLatestBlock = this._telemetry.gauge({
      name: 'blockchain_latest_block',
      help: 'Latest block',
    });
    this._gaugeUpstreamOnline = this._telemetry.gauge({
      name: 'blockchain_upstream_online',
      help: 'Does it work or not',
      labelNames: ['upstream'],
    });
    this._gaugeUpstreamCurrentBlock = this._telemetry.gauge({
      name: 'blockchain_upstream_current_block',
      help: 'Current block',
      labelNames: ['upstream'],
    });
  }
  
  private async _setup(callback: BlockCallback): Promise<Destructible> {
    return await setup(async use => {
      const listeners = new Container();
      
      for (const upstream of this._upstreams.values()) {
        await listeners.use(async () => {
          return await use(async () => {
            return await upstream.listen(async block => {
              this._gaugeUpstreamCurrentBlock.set({ upstream: upstream.name }, block);
              
              if (block <= this._block) {
                return;
              }
              
              const now = Date.now();
              const all = Array.from(this._upstreams.values());
              const online = this._online(now);
              const offline = all.filter(upstream => !online.includes(upstream));
              
              for (const upstream of online) {
                this._gaugeUpstreamOnline.set({ upstream: upstream.name }, 1);
              }
              for (const upstream of offline) {
                this._gaugeUpstreamOnline.set({ upstream: upstream.name }, 0);
              }
              
              let lowest = Infinity;
              for (const upstream of online) {
                lowest = Math.min(upstream.block, lowest);
              }
              
              if (lowest <= this._block) {
                return;
              }
              
              this._block = lowest;
              
              this._gaugeLatestBlock.set(this._block);
              
              await callback(this._block);
            });
          });
        });
      }
      
      return listeners;
    });
  }
  
  private _online(now: number): Upstream<S, B>[] {
    const online = [];
    
    for (const upstream of this._upstreams.values()) {
      if (now - upstream.timestamp < this._options.upstreamTimeout) {
        online.push(upstream);
      }
    }
    
    return online;
  }
  
  public async init(): Promise<void> {
    await setup(async use => {
      for (const upstreamSpec of this._specs) {
        const { name: upstreamName, spec: backendSpec } = upstreamSpec;
        
        const upstream = await use(async () => {
          const upstream = new Upstream({
            telemetry: this._telemetry.child(upstreamName),
            name: upstreamName,
            spec: backendSpec,
            factory: this._factory,
          });
          await upstream.init();
          
          return upstream;
        });
        
        this._upstreams.set(upstreamName, upstream);
      }
      
      this._source = await use(async () => {
        const source = new Multisource<number>({
          setup: this._setup,
        });
        await source.init();
        
        return source;
      });
    });
  }
  
  public async destroy(): Promise<void> {
    await this._source.destroy();
    
    for (const upstream of this._upstreams.values()) {
      await upstream.destroy();
    }
    
    this._telemetry.destroy();
  }
  
  public async listen(callback: BlockCallback): Promise<Destructible> {
    return await this._source.listen(callback);
  }
  
  public async upstream(): Promise<B | undefined> {
    const deadline = Date.now() + this._options.upstreamAcquireTimeout;
    
    while (true) {
      const now = Date.now();
      const online = this._online(now)
        .map(upstream => upstream.backend)
        .filter((backend): backend is B => backend != undefined)
      ;
      
      if (online.length > 0) {
        const index = Math.floor(Math.random() * online.length);
        return online[index];
      }
      
      if (now >= deadline) {
        return undefined;
      }
      
      const delay = Math.min(
        this._options.upstreamAcquirePoll,
        Math.max(deadline - now, 0),
      );
      
      await new Promise<void>(resolve => {
        const timer = setTimeout(resolve, delay);
        timer.unref?.();
      });
    }
  }
}

export type BlockchainFactory = <S, B extends Backend>(
  name: string,
  specs: BlockchainUpstreamSpec<S>[],
  factory: BackendFactory<S, B>,
  options?: Partial<BlockchainOptions>,
) => Promise<Blockchain<S, B>>;
