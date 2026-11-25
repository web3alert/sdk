import fastq from 'fastq';
import { type Subscription as NatsSubscription, Msg as NatsMsg } from 'nats';
import { Web3alertError } from './errors';
import { type Listener } from './listener';
import { type Core } from './core';
import { isReplySubject } from './inbox';
import { type Counter, type Metrics } from './types';

const rejectedReplies = new WeakMap<Metrics, Counter>();

export type SubscriptionBackendOptions = {
  queue?: string | undefined;
  concurrency?: number;
  requestHandler?: boolean;
};

export type SubscriptionMessageRespondCallback = (data?: unknown) => void;

export type SubscriptionMessage<T> = {
  subject: string;
  data: T;
  respond: SubscriptionMessageRespondCallback;
};

export type SubscriptionCallback<T> = (message: SubscriptionMessage<T>) => Promise<void>;

export type SubscriptionOptions<T> = {
  core: Core;
  subject: string;
  options?: SubscriptionBackendOptions;
  callback: SubscriptionCallback<T>;
};

export class Subscription<T> {
  private _core: Core;
  private _subject: string;
  private _options?: SubscriptionBackendOptions;
  private _callback: SubscriptionCallback<T>;
  private _subscription!: NatsSubscription;
  private _subscriptionListener!: Listener<NatsMsg>;
  private _queue!: fastq.queueAsPromised<NatsMsg, void>;
  
  constructor(options: SubscriptionOptions<T>) {
    const {
      core,
      subject,
      options: backendOptions,
      callback,
    } = options;
    
    this._core = core;
    this._subject = subject;
    this._options = backendOptions;
    this._callback = callback;
  }

  private _concurrency(): number {
    const value = this._options?.concurrency;

    if (value == undefined || !Number.isFinite(value)) {
      return 1;
    }

    return Math.min(Math.max(Math.trunc(value), 1), 256);
  }

  private async _handleMessage(message: NatsMsg): Promise<void> {
    try {
      const replyAllowed = Boolean(message.reply && isReplySubject(message.reply));
      if (this._options?.requestHandler && message.reply && !replyAllowed) {
        this._rejectReply(message.subject);
        return;
      }

      await this._callback({
        subject: message.subject,
        data: this._core.decode(message.data) as T,
        respond: data => {
          if (replyAllowed) {
            message.respond(this._core.encode(data));
          } else if (message.reply) {
            this._rejectReply(message.subject);
          }
        },
      });
    } catch (err) {
      throw new Web3alertError('subscription error', {
        cause: err,
        details: {
          subject: this._subject,
        },
      });
    }
  }

  private _rejectReply(subject: string): void {
    const telemetry = this._core.telemetry;
    let counter = rejectedReplies.get(telemetry.metrics);
    if (!counter) {
      counter = telemetry.counter({
        name: 'nats_invalid_reply_subject_total',
        help: 'NATS requests and responses dropped because the reply subject was outside an inbox or service mapping',
      });
      rejectedReplies.set(telemetry.metrics, counter);
    }
    counter.inc();
    telemetry.warn({ subject }, 'dropped invalid NATS reply subject');
  }
  
  public async init(): Promise<void> {
    this._core.telemetry.trace({ subject: this._subject }, 'listen');
    
    this._subscription = this._core.nats.subscribe(this._subject, {
      queue: this._options?.queue,
    });
    this._queue = fastq.promise(this._handleMessage.bind(this), this._concurrency());
    this._subscriptionListener = await this._core.listen(
      `subscription.${this._subject}`,
      this._subscription,
      async message => {
        void this._queue.push(message).catch(err => {
          this._core.uncaughtException(err);
        });
      },
    );
  }
  
  public async destroy(): Promise<void> {
    await this._subscription.drain();
    await this._subscriptionListener.destroy();
    await this._queue.drained();
  }
}
