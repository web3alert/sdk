import { type Telemetry } from '../types';
import { defaults, setup } from '../utils';
import { type BucketCell } from '../bucket';
import {
  type MutexCell,
} from '../multimutex';
import {
  type Core,
} from '../core';
import {
  nanoid,
} from '../utils';
import {
  Stream,
  StreamSubscription,
} from '../stream';

export type SequencerInstance = {
  timestamp: number;
  index: number;
};

export type SequencerState = {
  instances: Record<string, SequencerInstance>;
  index: number;
};

export type SequencerIndex = {
  value: number;
};

export type SequenceCallback = (index: number) => Promise<void>;

export type SequencerOptions = {
  start: number;
  instanceTimeout: number;
};

export type SequencerParams = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  callback: SequenceCallback;
  options?: Partial<SequencerOptions>;
};

export class Sequencer {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _callback: SequenceCallback;
  private _options: SequencerOptions;
  private _id: string;
  private _mutex!: MutexCell;
  private _state!: BucketCell<SequencerState>;
  private _stream!: Stream<SequencerIndex>;
  private _subscription!: StreamSubscription<SequencerIndex>;
  
  constructor(params: SequencerParams) {
    const {
      telemetry,
      core,
      name,
      callback,
      options,
    } = params;
    
    this._telemetry = telemetry;
    this._core = core;
    this._name = name;
    this._callback = callback;
    this._options = defaults(options, {
      start: -1,
      instanceTimeout: 15_000,
    });
    this._id = nanoid(4);
  }
  
  public async init(): Promise<void> {
    await setup(async use => {
      this._mutex = this._core.mutex(this._name);
      
      this._state = this._core.registry.cell(`${this._name}.state`);
      
      this._stream = await use(async () => {
        const stream = new Stream<SequencerIndex>({
          telemetry: this._telemetry.child('indexes'),
          core: this._core,
          name: `${this._name}.indexes`,
          options: {
            maxSize: 4 * 1024,
            maxMessages: 200,
          },
        });
        await stream.init();
        
        return stream;
      });
      
      this._subscription = await use(async () => {
        const subscription = new StreamSubscription<SequencerIndex>({
          telemetry: this._telemetry.child('sequence'),
          core: this._core,
          name: `${this._name}.sequence`,
          ref: this._stream.ref,
          callback: async message => {
            await this._callback(message.data.value);
          },
          options: { concurrency: 10 },
        });
        await subscription.init();
        
        return subscription;
      });
    });
  }
  
  public async destroy(): Promise<void> {
    await this._subscription.destroy();
    await this._stream.destroy();
    this._telemetry.destroy();
  }
  
  public async push(index: number): Promise<void> {
    await this._mutex.lock(async () => {
      await this._state.mutate(async (state, write) => {
        if (!state) {
          const start = (this._options.start >= 0) ? this._options.start : index;
          
          state = {
            instances: {},
            index: start - 1,
          };
        }
        
        const now = Date.now();
        const instances = { ...state.instances };
        const self = instances[this._id] ?? { timestamp: now, index };
        
        instances[this._id] = {
          timestamp: now,
          index: Math.max(index, self.index),
        };
        
        for (const key of Object.keys(instances)) {
          const instance = instances[key];
          
          if (now - instance.timestamp >= this._options.instanceTimeout) {
            delete instances[key];
          }
        }
        
        let lowest = Infinity;
        for (const key of Object.keys(instances)) {
          const instance = instances[key];
          
          lowest = Math.min(lowest, instance.index);
        }
        
        const prev = state;
        const next = {
          instances,
          index: Math.max(lowest, prev.index),
        };
        
        for (let i = prev.index + 1; i <= next.index; i++) {
          await this._stream.publish({ value: i });
        }
        
        this._telemetry.trace({ prev, next });
        
        await write(next);
      });
    });
  }
  
  public get stream(): Stream<SequencerIndex> {
    return this._stream;
  }
}

export type SequencerFactory = (
  name: string,
  options?: Partial<SequencerOptions>,
) => Promise<Sequencer>;
