import util from 'node:util';
import { AsyncLocalStorage, AsyncResource } from 'node:async_hooks';
import chalk from 'chalk';
import {
  type AsyncFunction,
  type SpanCallback,
  type TracingSpanOptions,
  type TracingWrapOptions,
} from './types';

export type TracingParams = {
  storage: AsyncLocalStorage<SpanImpl>;
  backend: Backend;
  root?: SpanImpl;
  tag: string;
};

export class TracingImpl {
  public storage: AsyncLocalStorage<SpanImpl>;
  public backend: Backend;
  public root: SpanImpl;
  public tag: string;
  
  constructor(params: TracingParams) {
    const {
      storage,
      backend,
      root,
      tag,
    } = params;
    
    this.storage = storage;
    this.backend = backend;
    this.root = root ?? new SpanImpl({
      origin: this,
      parent: null,
      name: '',
    });
    this.tag = tag;
  }
  
  public child(tag: string): TracingImpl {
    return new TracingImpl({
      storage: this.storage,
      backend: this.backend,
      root: this.root,
      tag: `${this.tag}.${tag}`,
    });
  }
  
  public head(): SpanImpl {
    const span = this.storage.getStore();
    
    if (!span) {
      throw new Error(`tracing out of context: ${this.tag}`);
    }
    
    return span;
  }
  
  public async span<T>(
    name: string,
    callback: SpanCallback<T>,
    options?: TracingSpanOptions,
  ): Promise<T> {
    let result: T;
    
    const span = new SpanImpl({
      origin: this,
      parent: (options?.root) ? this.root : this.head(),
      name: `${this.tag}.${name}`,
    });
    
    span.enter();
    
    return await this.storage.run(span, async () => {
      try {
        result = await callback(span);
      } catch (err) {
        span.exit(err);
        
        throw err;
      }
      
      span.exit();
      
      return result;
    });
  }
  
  public wrap<T, A extends any[], R>(
    name: string,
    fn: AsyncFunction<T, A, R>,
    options?: TracingWrapOptions,
  ): AsyncFunction<T, A, R> {
    const self = this;
    
    let wrapper: AsyncFunction<T, A, R> = async function (...args) {
      const them = this;
      
      return await self.span(name, async () => {
        return await fn.apply(them, args);
      }, options);
    };
    
    if (options?.bind) {
      wrapper = AsyncResource.bind<AsyncFunction<T, A, R>, T>(wrapper);
    }
    
    return wrapper;
  }
  
  public trace(msg?: string): void;
  public trace(details?: object, msg?: string): void;
  public trace(detailsOrMsg?: object | string, maybeMsg?: string): void {
    this.backend.trace(this, this.head(), detailsOrMsg, maybeMsg);
  }
}

export type CreateTracingParams = {
  backend: Backend;
};

export function createTracing(params: CreateTracingParams): TracingImpl {
  const {
    backend,
  } = params;
  
  const storage = new AsyncLocalStorage<SpanImpl>();
  const tracing = new TracingImpl({
    storage,
    backend,
    tag: '',
  });
  
  storage.enterWith(tracing.root);
  
  return tracing;
}

export type SpanParams = {
  origin: TracingImpl;
  parent: SpanImpl | null;
  name: string;
};

export class SpanImpl {
  public origin: TracingImpl;
  public parent: SpanImpl | null;
  public name: string;
  public depth: number;
  public start: number;
  public stop: number;
  
  constructor(params: SpanParams) {
    const {
      origin,
      parent,
      name,
    } = params;
    
    this.origin = origin;
    this.parent = parent;
    this.name = name;
    this.depth = (parent) ? parent.depth + 1 : 0;
    this.start = -1;
    this.stop = -1;
  }
  
  public enter(): void {
    this.start = this.origin.backend.now();
    this.origin.backend.enter(this);
  }
  
  public exit(err?: unknown): void {
    this.stop = this.origin.backend.now();
    this.origin.backend.exit(this, err);
  }
  
  public trace(msg?: string): void;
  public trace(details?: object, msg?: string): void;
  public trace(detailsOrMsg?: object | string, maybeMsg?: string): void {
    this.origin.backend.trace(this.origin, this, detailsOrMsg, maybeMsg);
  }
}

type TracingObject = {
  tag: string;
};

type SpanObject = {
  origin: TracingObject;
  parent: SpanObject | null;
  name: string;
  depth: number;
  start: number;
  stop: number;
};

type Backend = {
  now(): number;
  enter(span: SpanObject): void;
  exit(span: SpanObject, err?: unknown): void;
  trace(
    origin: TracingObject,
    span: SpanObject,
    detailsOrMsg?: object | string,
    maybeMsg?: string,
  ): void;
};

export function createTracingNoopBackend(): Backend {
  return {
    now() { return Date.now() },
    enter(span) {},
    exit(span, err) {},
    trace(origin, span, detailsOrMsg, maybeMsg) {},
  };
}

type Stream = {
  write(line: string): void;
};

export function createTracingConsoleBackend(stream: Stream): Backend {
  let last: SpanObject | null = null;
  let timestamp = Date.now();
  
  const write = (s: string) => {
    const now = Date.now();
    const elapsed = now - timestamp;
    const e = chalk.gray(`+${elapsed}ms`);
    
    stream.write(`${s} ${e}\n`);
    timestamp = now;
  };
  
  return {
    now() {
      return Date.now();
    },
    enter(span) {
      let s = name(span.name);
      
      if (span.parent && span.parent != last) {
        s = `${s} @ ${name(span.parent.name)}`;
      }
      
      s = chalk.gray(`${s} {`);
      
      s = indent(s, depth(span.depth));
      
      write(s);
      
      last = span;
    },
    exit(span, err) {
      let s = chalk.gray(`} =${span.stop - span.start}ms`);
      
      if (span != last) {
        s = `${s} ${chalk.gray(`@ ${name(span.name)}`)}`;
      }
      
      if (err) {
        s = `${s} ${chalk.red('error')} ${chalk.grey(errorMessage(err))}`;
      }
      
      s = indent(s, depth(span.depth));
      
      write(s);
      
      last = span.parent;
    },
    trace(origin, span, detailsOrMsg?: object | string, maybeMsg?: string) {
      const [msg, details] = (typeof detailsOrMsg == 'string')
        ? [detailsOrMsg, undefined]
        : [maybeMsg, detailsOrMsg]
      ;
      
      let m = msg && chalk.cyan(msg);
      let d = details && util.inspect(details, { depth: 20, colors: true });
      let s = join(m, d);
      
      if (!s) {
        return;
      }
      
      if (span != last) {
        s = `${s} ${chalk.gray(`@ ${name(span.name)}`)}`;
      }
      
      if (origin != span.origin) {
        s = `${s} ${chalk.gray(`from ${name(origin.tag)}`)}`;
      }
      
      s = indent(s, depth(span.depth + 1));
      
      write(s);
      
      last = span;
    },
  };
}

function indent(string: string, level: number): string {
  return string.replace(/^/mg, '  '.repeat(level));
}

function join(a: string | undefined, b: string | undefined): string | undefined {
  if (a === undefined) {
    return b;
  }
  
  if (b === undefined) {
    return a;
  }
  
  return `${a} ${b}`;
}

function errorMessage(err: unknown): string {
  if (err instanceof Error) {
    if (err.cause) {
      return `${err.message}: ${errorMessage(err.cause)}`;
    }
    
    return err.message || '<no error message>';
  }
  
  return '' + err;
}

function name(s: string): string {
  if (s == '') {
    return '.';
  }
  
  return s;
}

function depth(n: number): number {
  return Math.max(0, n - 1);
}
