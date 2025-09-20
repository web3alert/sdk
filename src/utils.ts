import crypto from 'node:crypto';
import { customAlphabet } from 'nanoid';
import stringify from 'fast-json-stable-stringify';
import {
  type MsgHdrs,
  NatsError,
  headers as createHeaders,
} from 'nats';
import {
  type Callback,
  type FallbackCallback,
  type UseSpawnCallback,
  type Teardown,
  type Mortal,
  type Headers,
} from './types';
import { Container } from './container';

export function defaults<T>(value: Partial<T> | undefined, defaults: T): T {
  return { ...defaults, ...value };
}

export type SleepOptions = {
  signal?: AbortSignal;
};

export async function sleep(ms: number, options?: Partial<SleepOptions>): Promise<void> {
  const {
    signal,
  } = options ?? {};
  
  await new Promise<void>((resolve, reject) => {
    if (signal) {
      const abort = () => reject(new Error('aborted', { cause: signal.reason }));
      
      if (signal.aborted) {
        return abort();
      }
      
      const timeout = setTimeout(() => {
        signal.removeEventListener('abort', fail);
        resolve();
      }, ms);
      
      const fail = () => {
        clearTimeout(timeout);
        signal.removeEventListener('abort', fail);
        abort();
      };
      
      signal.addEventListener('abort', fail);
    } else {
      setTimeout(resolve, ms);
    }
  });
}

export type RetryWhenCallback = (err: unknown, retry: number) => boolean;

export type RetryOptions = {
  when: RetryWhenCallback;
  retries: number;
  signal?: AbortSignal;
  minDelay: number;
  maxDelay: number;
  factor: number;
  jitter: number;
};

export async function retry<T>(
  callback: Callback<T>,
  options?: Partial<RetryOptions>,
): Promise<T> {
  const {
    when = () => true,
    retries = 10,
    signal,
    minDelay = 250,
    maxDelay = 120_000,
    factor = 1.5,
    jitter = 0.1,
  } = options ?? {};
  
  let retry = 0;
  
  while (true) {
    try {
      return await callback();
    } catch (err) {
      if (retry < retries && when(err, retry)) {
        const raw = minDelay * Math.pow(factor, retry);
        const limited = Math.min(raw, maxDelay);
        const jittered = limited * (1 - jitter + Math.random() * (jitter * 2));
        const ms = Math.round(jittered);
        
        await sleep(ms, { signal });
        retry++;
      } else {
        throw err;
      }
    }
  }
}

export function rootCause(err: unknown): unknown {
  while (err instanceof Error && err.cause) {
    err = err.cause;
  }
  
  return err;
}

export async function kill(mortal: Mortal): Promise<void> {
  if (typeof mortal == 'function') {
    return await mortal();
  }
  
  await mortal.destroy();
}

export async function caught<T>(
  callback: Callback<T>,
  mortal: Mortal,
): Promise<T> {
  try {
    return await callback();
  } catch (err) {
    await kill(mortal);
    
    throw err;
  }
}

export async function anyway<T>(
  callback: Callback<T>,
  cleanup: Callback,
): Promise<T> {
  const result = await caught(callback, cleanup);
  await cleanup();
  
  return result;
}

export function hashOf(value: unknown | undefined): string {
  if (value == undefined) {
    return '00000000000000000000000000000000';
  }
  
  const json = stringify(value);
  const hash = crypto.createHash('md5').update(json).digest('hex');
  
  return hash;
}

export type CompareCallback<T> = (a: T, b: T) => boolean;

export function defaultCompare<T>(a: T, b: T): boolean {
  return hashOf(a) == hashOf(b);
}

export async function fallback<T>(
  callback: Callback<T>,
  fallback: FallbackCallback<T>,
): Promise<T> {
  try {
    return await callback();
  } catch (err) {
    return await fallback(err);
  }
}

export function isWrongLastSequenceError(err: unknown): boolean {
  return !!(err && err instanceof NatsError && err.api_error && err.api_error.err_code == 10071);
}

export type SetupCallback<T> = (use: UseSpawnCallback, teardown: Teardown) => Promise<T>;

export async function setup<T>(callback: SetupCallback<T>): Promise<T> {
  const container = new Container();
  const use: UseSpawnCallback = async spawn => await container.use(spawn);
  const teardown = async () => await container.destroy();
  
  return await caught(async () => {
    return await callback(use, teardown);
  }, teardown);
}

export function toHeaders(headers: Headers): MsgHdrs {
  const result = createHeaders();
  const keys = Object.keys(headers);
  
  for (const key of keys) {
    const value = headers[key];
    
    if (Array.isArray(value)) {
      for (const item of value) {
        result.append(key, item);
      }
    } else {
      result.set(key, value);
    }
  }
  
  return result;
}

export function fromHeaders(headers: MsgHdrs): Headers {
  const result: Headers = {};
  const keys = headers.keys();
  
  for (const key of keys) {
    const values = headers.values(key);
    
    if (values.length == 1) {
      result[key] = values[0];
    } else {
      result[key] = values;
    }
  }
  
  return result;
}

export const NANOID_ALPHABET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
export const nanoid = customAlphabet(NANOID_ALPHABET, 24);

export type OneshotCallback<T = void> = (use: UseSpawnCallback) => Promise<T>;

export async function oneshot<T>(callback: OneshotCallback<T>): Promise<T> {
  return await setup(async (use, destroy) => {
    const result = await callback(use);
    await destroy();
    
    return result;
  });
}

export type Loader<T> = () => Promise<T>;

export function load<T>(module: string): Loader<T> {
  return async () => {
    const mod = await import(module);
    return mod.default;
  };
}

export async function mapInBatches<T, U>(
  array: T[],
  batchSize: number,
  callback: (item: T, index: number, array: T[]) => U,
): Promise<U[]> {
  const result: U[] = [];
  
  for (let i = 0; i < array.length; i += batchSize) {
    const batchEnd = Math.min(i + batchSize, array.length);
    
    for (let index = i; index < batchEnd; index++) {
      const item = array[index];
      const value = callback(item, index, array);
      
      result.push(value);
    }
    
    await new Promise(resolve => setImmediate(resolve));
  }
  
  return result;
}

export function formatDateForFilename(date: Date): string {
  const year = date.getFullYear().toString();
  const month = (date.getMonth() + 1).toString().padStart(2, '0');
  const day = date.getDate().toString().padStart(2, '0');
  const hours = date.getHours().toString().padStart(2, '0');
  const minutes = date.getMinutes().toString().padStart(2, '0');
  const seconds = date.getSeconds().toString().padStart(2, '0');
  
  return `${year}-${month}-${day}-${hours}-${minutes}-${seconds}`;
}
