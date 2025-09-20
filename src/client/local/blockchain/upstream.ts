import { EventEmitter } from 'eventemitter3';
import { type Telemetry, type Destructible } from '../../../types';
import { setup } from '../../../utils';
import { Debouncer } from '../../../debouncer';
import { Immortal} from '../../../immortal';
import { type BlockCallback } from './types';
import { type BackendFactory, type Backend } from './backend';

export type UpstreamParams<S, B extends Backend> = {
  telemetry: Telemetry;
  name: string;
  spec: S;
  factory: BackendFactory<S, B>;
};

export class Upstream<S, B extends Backend> {
  private _telemetry: Telemetry;
  private _name: string;
  private _spec: S;
  private _factory: BackendFactory<S, B>;
  private _block: number;
  private _timestamp: number;
  private _emitter: EventEmitter<{ block: [block: number] }>;
  private _immortal!: Immortal;
  private _backend?: B;
  
  constructor(params: UpstreamParams<S, B>) {
    const {
      telemetry,
      name,
      spec,
      factory,
    } = params;
    
    this._telemetry = telemetry;
    this._name = name;
    this._spec = spec;
    this._factory = factory;
    this._block = -1;
    this._timestamp = 0;
    this._emitter = new EventEmitter();
  }
  
  public async init(): Promise<void> {
    await setup(async use => {
      this._immortal = await use(async () => {
        const undead = new Immortal({
          spawn: async () => {
            return await setup(async use => {
              this._telemetry.debug('starting backend');
              
              const backend = await use(async () => {
                return await this._factory(this._name, this._spec);
              });
              
              this._telemetry.debug('subscribing to blocks');
              
              const unsubscribe = await use(async () => {
                const unsubscribe = await backend.subscribe(block => {
                  this._emitter.emit('block', block);
                });
                
                return { destroy: unsubscribe };
              });
              
              this._telemetry.debug('online');
              
              this._backend = backend;
              
              return {
                destroy: async () => {
                  this._backend = undefined;
                  
                  this._telemetry.debug('unsubscribing from blocks');
                  
                  await unsubscribe.destroy();
                  
                  this._telemetry.debug('stopping backend');
                  
                  await backend.destroy();
                  
                  this._telemetry.debug('offline');
                },
              };
            });
          },
        });
        await undead.init();
        
        return undead;
      });
    });
  }
  
  public async destroy(): Promise<void> {
    await this._immortal.destroy();
  }
  
  public async listen(callback: BlockCallback): Promise<Destructible> {
    const debouncer = new Debouncer<number>({ callback });
    
    const handler = (block: number) => {
      this._block = block;
      this._timestamp = Date.now();
      debouncer.push(block);
    };
    
    this._emitter.on('block', handler);
    
    return {
      destroy: async () => {
        this._emitter.off('block', handler);
        
        await debouncer.destroy();
      },
    };
  }
  
  public get name(): string {
    return this._name;
  }
  
  public get backend(): B | undefined {
    return this._backend;
  }
  
  public get block(): number {
    return this._block;
  }
  
  public get timestamp(): number {
    return this._timestamp;
  }
}
