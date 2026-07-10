import fastq from 'fastq';
import {
  type StreamInfo,
  type JetStreamPublishOptions,
  type ConsumerInfo,
  type Consumer,
  type ConsumerMessages,
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
import { Web3alertError, getRequestedRedeliveryDelayMs } from './errors';
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
  discardPolicy: DiscardPolicy;
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
      discardPolicy: DiscardPolicy.New,
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
    this._telemetry.trace({ subject: rawSubject, data: summarizeStreamPayload(data) }, 'publish');

    const publishOptions = prepareStreamPublishOptions(options);

    let ack;
    try {
      ack = await this._core.js.publish(rawSubject, this._core.encode(data), publishOptions);
    } catch (err) {
      this._telemetry.error({
        err,
        stream: this._info.config.name,
        subject: rawSubject,
        discardPolicy: this._options.discardPolicy,
        limits: {
          maxAge: this._options.maxAge,
          maxSize: this._options.maxSize,
          maxMessages: this._options.maxMessages,
          maxMessageSize: this._options.maxMessageSize,
        },
      }, isJetStreamLimitError(err) ? 'stream publish rejected by limits' : 'stream publish failed');

      throw err;
    }

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
        discard: this._options.discardPolicy,
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
          discard: this._options.discardPolicy,
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
  batchSize: number;
  fetchExpiresMs: number;
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

function normalizeStreamSubscriptionBatchSize(value: number | undefined, concurrency: number): number {
  if (value == undefined || !Number.isFinite(value)) {
    return Math.min(Math.max(concurrency * 8, 100), 1000);
  }

  return Math.min(Math.max(Math.trunc(value), 1), 1000);
}

function normalizeStreamSubscriptionFetchExpiresMs(value: number | undefined): number {
  if (value == undefined || !Number.isFinite(value)) {
    return 10_000;
  }

  return Math.min(Math.max(Math.trunc(value), 1_000), 120_000);
}

async function sleep(ms: number): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, ms));
}

function isMissingJetStreamResourceError(err: unknown): boolean {
  if (!(err instanceof NatsError)) {
    return false;
  }

  const description = err.api_error?.description ?? err.message;

  return (
    (
      err.code == '404'
      && (
        description == 'stream not found'
        || description == 'consumer not found'
      )
    )
    || (
      err.code == '409'
      && (
        description == 'consumer deleted'
      )
    )
  );
}

function isJetStreamResourceStoppedError(err: unknown): boolean {
  if (isMissingJetStreamResourceError(err)) {
    return true;
  }

  if (!(err instanceof NatsError)) {
    return false;
  }

  const description = err.api_error?.description ?? err.message;

  return (
    err.code == '409'
    && (
      description == 'consumer deleted'
    )
  );
}

function isJetStreamFetchTimeout(err: unknown): boolean {
  if (!(err instanceof NatsError)) {
    return false;
  }

  return err.code == '408' || err.message.toLowerCase().includes('timeout');
}

function isJetStreamLimitError(err: unknown): boolean {
  if (!(err instanceof NatsError)) {
    return false;
  }

  const description = err.api_error?.description ?? err.message;

  return (
    err.code == '503'
    && (
      description.includes('maximum')
      || description.includes('exceeded')
      || description.includes('limit')
    )
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value == 'object' && value != null && !Array.isArray(value);
}

function summarizeStreamPayload(value: unknown): unknown {
  if (Array.isArray(value)) {
    return {
      type: 'array',
      length: value.length,
    };
  }

  if (!isRecord(value)) {
    return {
      type: typeof value,
    };
  }

  const data = isRecord(value['data']) ? value['data'] : value;
  const payload = isRecord(value['payload']) ? value['payload'] : undefined;
  const system = isRecord(payload?.['__system']) ? payload['__system'] : undefined;
  const runtime = isRecord(payload?.['__runtime']) ? payload['__runtime'] : undefined;

  return {
    type: 'object',
    keys: Object.keys(value).slice(0, 20),
    ...(typeof value['name'] == 'string' ? { name: value['name'] } : {}),
    ...(typeof value['title'] == 'string' ? { title: value['title'] } : {}),
    ...(typeof data['block'] == 'number' || typeof data['block'] == 'string' ? { block: data['block'] } : {}),
    ...(typeof data['index'] == 'number' || typeof data['index'] == 'string' ? { index: data['index'] } : {}),
    ...(typeof data['hash'] == 'string' ? { hash: data['hash'] } : {}),
    ...(typeof data['transactionHash'] == 'string' ? { transactionHash: data['transactionHash'] } : {}),
    ...(system ? {
      system: {
        ...(typeof system['block'] == 'number' ? { block: system['block'] } : {}),
        ...(typeof system['itemIndex'] == 'number' ? { itemIndex: system['itemIndex'] } : {}),
        ...(typeof system['transactionHash'] == 'string' ? { transactionHash: system['transactionHash'] } : {}),
      },
    } : {}),
    ...(runtime ? {
      runtime: {
        ...(typeof runtime['revision'] == 'number' ? { revision: runtime['revision'] } : {}),
        ...(typeof runtime['durationMs'] == 'number' ? { durationMs: runtime['durationMs'] } : {}),
        ...(typeof runtime['outputIndex'] == 'number' ? { outputIndex: runtime['outputIndex'] } : {}),
        ...(typeof runtime['outputCount'] == 'number' ? { outputCount: runtime['outputCount'] } : {}),
        ...(typeof runtime['eventId'] == 'string' ? { eventId: runtime['eventId'] } : {}),
        ...(typeof runtime['sourceFullname'] == 'string' ? { sourceFullname: runtime['sourceFullname'] } : {}),
        ...(typeof runtime['bindingId'] == 'string' ? { bindingId: runtime['bindingId'] } : {}),
      },
    } : {}),
  };
}

export class StreamSubscription<T> {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _ref: StreamRef;
  private _callback: StreamSubscriptionCallback<T>;
  private _filter?: string;
  private _concurrency: number;
  private _batchSize: number;
  private _fetchExpiresMs: number;
  private _error?: ErrorCallback;
  private _info!: ConsumerInfo;
  private _consumer!: Consumer;
  private _activeMessages?: ConsumerMessages;
  private _terminator!: AbortController;
  private _queue!: fastq.queueAsPromised<JsMsg, void>;
  private _loop!: Promise<void>;
  private _loopError?: unknown;

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
    this._batchSize = normalizeStreamSubscriptionBatchSize(options?.batchSize, this._concurrency);
    this._fetchExpiresMs = normalizeStreamSubscriptionFetchExpiresMs(options?.fetchExpiresMs);
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
      let data: T | undefined;

      try {
        data = this._core.decode(message.data) as T;

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
        const requestedDelayMs = getRequestedRedeliveryDelayMs(err);

        if (requestedDelayMs == undefined) {
          this._core.warn(new Web3alertError('message handling failed', {
            cause: err,
            details: {
              ref: this._ref,
              name: this._name,
              subject: message.subject,
              sequence: message.seq,
              redeliveryCount: message.info.redeliveryCount,
              data: summarizeStreamPayload(data),
            },
          }));
        } else {
          this._telemetry.debug({
            err,
            ref: this._ref,
            name: this._name,
            subject: message.subject,
            sequence: message.seq,
            redeliveryCount: message.info.redeliveryCount,
            redeliveryDelayMs: requestedDelayMs,
          }, 'message handler requested delayed redelivery');
        }

        const after = requestedDelayMs
          ?? Math.min(1000 * Math.pow(2, message.info.redeliveryCount), 120_000);

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
      this._loop = this._consume().catch(async err => {
        this._loopError = err;

        if (this._terminator.signal.aborted) {
          this._telemetry.warn({
            err,
            ref: this._ref,
            name: this._name,
          }, 'stream subscription stopped while underlying jetstream resource was removed');
          return;
        }

        if (this._error) {
          try {
            await this._error(err);
            return;
          } catch (callbackErr) {
            this._core.warn(new Web3alertError('stream subscription error callback failed', {
              cause: callbackErr,
              details: { ref: this._ref, name: this._name },
            }));
          }
        }

        this._core.uncaughtException(err);
      });
    });
  }

  public async destroy(): Promise<void> {
    this._terminator?.abort();
    this._stopActiveMessages();

    await this._loop;

    if (this._queue) {
      await this._queue.drained();
    }

    if (this._info) {
      try {
        await this._core.jsm.consumers.delete(this._info.stream_name, this._info.name);
      } catch (err) {
        if (!isMissingJetStreamResourceError(err)) {
          throw err;
        }
      }
    }

    this._telemetry.destroy();
  }

  private _stopActiveMessages(): void {
    try {
      this._activeMessages?.stop();
    } catch {
      // Best-effort shutdown helper. The consume loop also observes _terminator
      // and treats fetch closure during destroy as a normal exit path.
    }
  }

  private async _consume(): Promise<void> {
    while (!this._terminator.signal.aborted) {
      await this._waitForQueueCapacity();

      if (this._terminator.signal.aborted) {
        return;
      }

      const maxMessages = this._nextFetchMessageCount();
      let messages;
      let messagesClosed: Promise<void | Error> | undefined;

      try {
        messages = await this._consumer.fetch({
          max_messages: maxMessages,
          expires: this._fetchExpiresMs,
        });
        this._activeMessages = messages;
        messagesClosed = messages.closed().catch(err => err as Error);
      } catch (err) {
        if (this._terminator.signal.aborted) {
          this._telemetry.warn({
            err,
            ref: this._ref,
            name: this._name,
          }, 'stream subscription stopped while underlying jetstream resource was removed');

          return;
        }

        throw err;
      }

      let streamErr: unknown;

      try {
        for await (const message of messages) {
          await this._waitForQueueCapacity();

          if (this._terminator.signal.aborted) {
            break;
          }

          void this._queue.push(message);
        }
      } catch (err) {
        streamErr = err;
      } finally {
        if (this._activeMessages == messages) {
          this._activeMessages = undefined;
        }
      }

      const closedErr = await messagesClosed;
      const err = streamErr ?? closedErr;

      if (this._terminator.signal.aborted) {
        return;
      }

      if (err && !isJetStreamFetchTimeout(err)) {
        if (this._terminator.signal.aborted) {
          this._telemetry.warn({
            err,
            ref: this._ref,
            name: this._name,
          }, 'stream subscription stopped while underlying jetstream resource was removed');

          return;
        }

        throw err;
      }

      if (!streamErr && !closedErr) {
        await this._waitForQueueCapacity();
      }
    }
  }

  private async _waitForQueueCapacity(): Promise<void> {
    const maxLocalMessages = this._batchSize + this._concurrency;

    while (!this._terminator.signal.aborted) {
      const queued = this._queue.length() + this._queue.running();
      if (queued < maxLocalMessages) {
        return;
      }

      await sleep(10);
    }
  }

  private _nextFetchMessageCount(): number {
    const maxLocalMessages = this._batchSize + this._concurrency;
    const queued = this._queue.length() + this._queue.running();
    const available = Math.max(maxLocalMessages - queued, 1);
    return Math.min(this._batchSize, available);
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
