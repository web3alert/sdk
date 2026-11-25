import { type Callback } from './types';
import { defaults, anyway, retry, isWrongLastSequenceError } from './utils';
import { type KV, type StreamInfo, ErrorCode, NatsError, StorageType } from 'nats';
import { Web3alertError } from './errors';
import { AutoabortableError } from './autoabortable';
import { type Core } from './core';

export type MutexBucketOptions = {
  bucketName: string;
  ttl: number;
};

export type MutexBucketParams = {
  core: Core;
  options?: Partial<MutexBucketOptions>;
};

export class MutexBucket {
  private _core: Core;
  private _options: MutexBucketOptions;
  private _kv!: KV;
  private _recovery?: Promise<void>;
  private _recoveryShutdown = new AbortController();
  private _bindOnly = false;
  
  constructor(params: MutexBucketParams) {
    const {
      core,
      options,
    } = params;
    
    this._core = core;
    this._options = defaults(options, {
      bucketName: this._core.options.lockBucketName ?? 'lock',
      ttl: this._core.options.lockTimeout,
    });
  }
  
  public async init(): Promise<void> {
    this._kv = await this._open();
    void this._core.nats.closed().then(() => this.destroy());
  }

  private _streamInfo(): Promise<StreamInfo> {
    return this._core.jsm.streams.info(`KV_${this._options.bucketName}`);
  }

  private async _withStreamIdentity<T>(operation: (created: string) => Promise<T>): Promise<T> {
    const { nats } = this._core;
    const connection = nats.info;
    const { created } = await this._streamInfo();
    // Read the identity again if the transport changed while fetching it.
    if (nats.info !== connection) {
      throw NatsError.errorForCode(ErrorCode.Disconnect);
    }
    return await operation(created);
  }

  private async _open(): Promise<KV> {
    const name = this._options.bucketName;
    if (name !== 'lock' || this._bindOnly) {
      const { config } = await this._streamInfo();
      return await this._core.js.views.kv(name, { bindOnly: true, allow_direct: config.allow_direct });
    }

    return await this._core.js.views.kv(name, {
      replicas: 1,
      ttl: this._options.ttl,
      storage: StorageType.File,
    });
  }

  private _recover(): Promise<void> {
    this._recovery ??= retry(async () => {
      this._recoveryShutdown.signal.throwIfAborted();
      this._kv = await this._open();
    }, {
      retries: Infinity,
      signal: this._recoveryShutdown.signal,
      minDelay: 1_000,
      maxDelay: 5_000,
      when: error => {
        if (!(error instanceof NatsError) || this._recoveryShutdown.signal.aborted || this._core.nats.isClosed()) {
          return false;
        }
        if (error.code === ErrorCode.PermissionsViolation) {
          this._bindOnly = true;
        }
        this._core.telemetry.warn({ error, bucket: this._options.bucketName }, 'retrying NATS mutex bucket recovery');
        return true;
      },
    }).finally(() => {
      this._recovery = undefined;
    });
    return this._recovery;
  }

  public async destroy(): Promise<void> {
    this._recoveryShutdown.abort(new AutoabortableError());
  }

  public async acquire(key: string): Promise<Callback> {
    return await retry(async () => {
      if (this._recovery) await this._recovery;
      const kv = this._kv;
      try {
        return await this._withStreamIdentity(async created => {
          const revision = await kv.create(key, this._core.encode(undefined));
          return () => this._release(kv, key, revision, created);
        });
      } catch (error) {
        if (isWrongLastSequenceError(error)) {
          throw new MutexLockedError();
        }
        if (isMissingBucket(error) && kv === this._kv) {
          await this._recover();
        }
        throw error;
      }
    }, {
      when: error => isMissingBucket(error) || (error instanceof NatsError && error.code === ErrorCode.Disconnect),
      retries: 2,
      signal: this._recoveryShutdown.signal,
    });
  }

  private async _release(kv: KV, key: string, revision: number, created: string): Promise<void> {
    try {
      await retry(() => this._withStreamIdentity(async current => {
        // Sequence numbers can be reused when JetStream creates a new stream.
        if (current === created) {
          await kv.delete(key, { previousSeq: revision });
        }
      }), {
        when: error => isNoResponders(error) || (error instanceof NatsError && (
          error.code === ErrorCode.Disconnect || error.code === ErrorCode.Timeout
        )),
        retries: 2,
        signal: this._recoveryShutdown.signal,
      });
    } catch (error) {
      if (!isWrongLastSequenceError(error) && !isMissingStream(error)) {
        throw error;
      }
    }
  }
  
  public mutex(key: string): MutexCell {
    return new MutexCell({ bucket: this, key });
  }
}

function isMissingBucket(error: unknown): boolean {
  return isMissingStream(error) || isNoResponders(error);
}

function isMissingStream(error: unknown): boolean {
  return error instanceof NatsError && error.api_error?.err_code === 10059;
}

function isNoResponders(error: unknown): boolean {
  return error instanceof NatsError && !error.api_error && error.code === ErrorCode.NoResponders;
}

export type MutexCellParams = {
  bucket: MutexBucket;
  key: string;
};

export class MutexCell {
  private _bucket: MutexBucket;
  private _key: string;
  
  constructor(params: MutexCellParams) {
    this._bucket = params.bucket;
    this._key = params.key;
  }
  
  public async lock(callback: Callback): Promise<void> {
    try {
      const release = await retry(() => this._bucket.acquire(this._key), {
        when: err => err instanceof MutexLockedError,
        maxDelay: 2_000,
      });
      await anyway(callback, release);
    } catch (err) {
      if (err instanceof MutexLockedError) {
        throw new Web3alertError('cannot acquire lock', { details: { key: this._key } });
      }
      
      throw err;
    }
  }
}

export class MutexLockedError extends Web3alertError {
  constructor() {
    super('mutex locked');
  }
}
