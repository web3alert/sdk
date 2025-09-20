import fastq from 'fastq';
import {
  type StreamInfo,
  type JetStreamPublishOptions,
  type ConsumerInfo,
  type Consumer,
  type JsMsg,
  NatsError,
  RetentionPolicy,
  StorageType,
  DiscardPolicy,
  AckPolicy,
  DeliverPolicy,
} from 'nats';
import { type Gauge, type Telemetry, type ErrorCallback, type Headers } from './types';
import { defaults, setup, toHeaders, fromHeaders } from './utils';
import { Web3alertError } from './errors';
import { type Core } from './core';

export type StreamRef = {
  stream: string;
  subject: string;
};

export type StreamPublishOptions = {
  id: string;
  headers: Headers;
};

export type StreamOptions = {
  maxAge: number;
  maxSize: number;
  maxMessages: number;
  maxMessageSize: number;
};

export type StreamParams = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  options?: Partial<StreamOptions>;
};

export class Stream<T> {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _options: StreamOptions;
  private _info!: StreamInfo;
  private _ref!: StreamRef;
  
  private _gaugeLastPublishedSeq: Gauge;
  
  constructor(params: StreamParams) {
    const {
      telemetry,
      core,
      name,
      options,
    } = params;
    
    this._telemetry = telemetry;
    this._core = core;
    this._name = name;
    this._options = defaults(options, defaults(this._core.options.streamDefaults, {
      maxAge: 2 * 60 * 60 * 1000, // 2 hours
      maxSize: 100 * 1024 * 1024, // 100 MB
      maxMessages: 100_000,
      maxMessageSize: 100 * 1024, // 100 KB
    }));
    
    this._gaugeLastPublishedSeq = this._telemetry.gauge({
      name: 'stream_last_published_seq',
      help: 'Last published sequence',
      labelNames: ['stream', 'subject'],
    });
    
    this.init = this._telemetry.wrap('init', this.init);
    this.destroy = this._telemetry.wrap('destroy', this.destroy);
  }
  
  private async _publish(
    rawSubject: string,
    data: T,
    options?: Partial<StreamPublishOptions>,
  ): Promise<void> {
    this._telemetry.trace({ subject: rawSubject, data }, 'publish');
    
    const publishOptions = prepareStreamPublishOptions(options);
    
    const ack = await this._core.js.publish(rawSubject, this._core.encode(data), publishOptions);
    
    this._gaugeLastPublishedSeq.set({
      stream: this._info.config.name,
    }, ack.seq);
  }
  
  public async init(): Promise<void> {
    const name = this._name.replaceAll('.', '_');
    const subject = this._name;
    
    try {
      this._info = await this._core.jsm.streams.add({
        name,
        retention: RetentionPolicy.Interest,
        storage: StorageType.File,
        subjects: [subject, `${subject}.>`],
        max_msgs: this._options.maxMessages,
        max_age: this._options.maxAge * 1_000_000,
        max_bytes: this._options.maxSize,
        max_msg_size: this._options.maxMessageSize,
        discard: DiscardPolicy.Old,
        num_replicas: this._core.replicas,
      });
    } catch (err) {
      // stream name already in use with a different configuration
      if (err instanceof NatsError && err.api_error && err.api_error.err_code == 10058) {
        this._info = await this._core.jsm.streams.update(name, {
          max_msgs: this._options.maxMessages,
          max_age: this._options.maxAge * 1_000_000,
          max_bytes: this._options.maxSize,
          max_msg_size: this._options.maxMessageSize,
        });
      } else {
        throw err;
      }
    }
    
    this._ref = {
      stream: this._info.config.name,
      subject,
    };
  }
  
  public async destroy(): Promise<void> {
    this._telemetry.destroy();
  }
  
  public async publish(
    data: T,
    options?: Partial<StreamPublishOptions>,
  ): Promise<void> {
    await this._publish(this._ref.subject, data, options);
  }
  
  public async publishToSubject(
    subject: string,
    data: T,
    options?: Partial<StreamPublishOptions>,
  ): Promise<void> {
    await this._publish(`${this._ref.subject}.${subject}`, data, options);
  }
  
  public get ref(): StreamRef {
    return this._ref;
  }
  
  public get info(): StreamInfo {
    return this._info;
  }
}

export type StreamMessage<T> = {
  subject: string;
  header: Headers;
  data: T;
};

export type StreamSubscriptionCallback<T> = (message: StreamMessage<T>) => Promise<void>;

export type StreamSubscriptionOptions = {
  filter: string;
  concurrency: number;
  error: ErrorCallback;
};

export type StreamSubscriptionParams<T> = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  ref: StreamRef;
  callback: StreamSubscriptionCallback<T>;
  options?: Partial<StreamSubscriptionOptions>;
};

export class StreamSubscription<T> {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _ref: StreamRef;
  private _callback: StreamSubscriptionCallback<T>;
  private _filter?: string;
  private _concurrency: number;
  private _error?: ErrorCallback;
  private _info!: ConsumerInfo;
  private _consumer!: Consumer;
  private _terminator!: AbortController;
  private _queue!: fastq.queueAsPromised<JsMsg, void>;
  private _loop!: Promise<void>;
  
  private _gaugeLastConsumedSeq: Gauge;
  
  constructor(params: StreamSubscriptionParams<T>) {
    const {
      telemetry,
      core,
      name,
      ref,
      callback,
      options,
    } = params;
    
    this._handleMessage = this._handleMessage.bind(this);
    
    this._telemetry = telemetry;
    this._core = core;
    this._name = name;
    this._ref = ref;
    this._callback = callback;
    this._filter = options?.filter;
    this._concurrency = options?.concurrency ?? 1;
    this._error = options?.error;
    
    this._gaugeLastConsumedSeq = this._telemetry.gauge({
      name: 'stream_last_consumed_seq',
      help: 'Last consumed sequence',
      labelNames: ['stream', 'consumer', 'subject'],
    });
    
    this.init = this._telemetry.wrap('init', this.init);
    this.destroy = this._telemetry.wrap('destroy', this.destroy);
    this._handleMessage = this._telemetry.wrap('_handleMessage', this._handleMessage);
  }
  
  private async _handleMessage(message: JsMsg): Promise<void> {
    await this._core.nothrow(async () => {
      try {
        const data = this._core.decode(message.data) as T;
        
        await this._callback({
          subject: message.subject,
          header: (message.headers) ? fromHeaders(message.headers) : {},
          data,
        });
        
        message.ack();
        
        this._gaugeLastConsumedSeq.set({
          stream: this._info.stream_name,
          consumer: this._info.name,
        }, message.seq);
      } catch (err) {
        this._core.warn(new Web3alertError('message handling failed', {
          cause: err,
          details: { ref: this._ref, name: this._name },
        }));
        
        const after = Math.min(1000 * Math.pow(2, message.info.redeliveryCount), 120_000);
        
        message.nak(after);
      }
    }, { ref: this._ref, name: this._name, handler: 'messages' });
  }
  
  public async init(): Promise<void> {
    await setup(async use => {
      const name = this._name.replaceAll('.', '_');
      
      const info = await this._core.jsm.consumers.add(this._ref.stream, {
        ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.New,
        durable_name: name,
        filter_subject: (this._filter) ? `${this._ref.subject}.${this._filter}` : undefined,
      });
      this._info = info;
      
      this._consumer = await this._core.js.consumers.get(info.stream_name, info.name);
      this._terminator = new AbortController();
      this._queue = fastq.promise(this._handleMessage, this._concurrency);
      this._loop = this._consume();
    });
  }
  
  public async destroy(): Promise<void> {
    this._terminator.abort();
    await this._loop;
    await this._queue.drained();
    this._telemetry.destroy();
  }
  
  private async _consume(): Promise<void> {
    while (!this._terminator.signal.aborted) {
      const messages = await this._consumer.fetch({ max_messages: 20 });
      
      for await (const message of messages) {
        this._queue.push(message);
      }
      
      await this._queue.drained();
    }
  }
  
  public get info(): ConsumerInfo {
    return this._info;
  }
}

export type StreamPipeOptions = {
  stream?: Partial<StreamOptions>;
  subscription?: Partial<StreamSubscriptionOptions>;
};

export type StreamPipeParams<T> = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  callback: StreamSubscriptionCallback<T>;
  options?: Partial<StreamPipeOptions>;
};

export class StreamPipe<T> {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _callback: StreamSubscriptionCallback<T>;
  private _options: StreamPipeOptions;
  
  public stream!: Stream<T>;
  public subscription!: StreamSubscription<T>;
  
  constructor(params: StreamPipeParams<T>) {
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
    this._options = defaults(options, {});
    
    this.init = this._telemetry.wrap('init', this.init);
    this.destroy = this._telemetry.wrap('destroy', this.destroy);
  }
  
  public async init(): Promise<void> {
    await setup(async use => {
      this.stream = await use(async () => {
        const stream = new Stream({
          telemetry: this._telemetry.child('stream'),
          core: this._core,
          name: this._name,
          options: this._options.stream,
        });
        await stream.init();
        
        return stream;
      });
      
      this.subscription = await use(async () => {
        const subscription = new StreamSubscription({
          telemetry: this._telemetry.child('subscription'),
          core: this._core,
          name: 'pipe',
          ref: this.stream.ref,
          callback: this._callback,
          options: this._options.subscription,
        });
        await subscription.init();
        
        return subscription;
      });
    });
  }
  
  public async destroy(): Promise<void> {
    await this.subscription.destroy();
    await this.stream.destroy();
    this._telemetry.destroy();
  }
  
  public async publish(
    data: T,
    options?: Partial<StreamPublishOptions>,
  ): Promise<void> {
    await this.stream.publish(data, options);
  }
}

export function prepareStreamPublishOptions(
  options?: Partial<StreamPublishOptions>,
): Partial<JetStreamPublishOptions> | undefined {
  if (!options) {
    return undefined;
  }
  
  const result: Partial<JetStreamPublishOptions> = {};
  let something = false;
  
  if (options.id) {
    result.msgID = options.id;
    something = true;
  }
  
  if (options.headers) {
    result.headers = toHeaders(options.headers);
    something = true;
  }
  
  if (something) {
    return result;
  }
  
  return undefined;
}

export type StreamFactory = {
  <T>(
    telemetry: Telemetry,
    name: string,
    options?: Partial<StreamOptions>,
  ): Promise<Stream<T>>;
  subscribe<T>(
    telemetry: Telemetry,
    name: string,
    ref: StreamRef,
    callback: StreamSubscriptionCallback<T>,
    options?: Partial<StreamSubscriptionOptions>,
  ): Promise<StreamSubscription<T>>;
  pipe<T>(
    telemetry: Telemetry,
    name: string,
    callback: StreamSubscriptionCallback<T>,
    options?: Partial<StreamPipeOptions>,
  ): Promise<StreamPipe<T>>;
};
