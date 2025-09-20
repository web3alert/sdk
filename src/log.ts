import util from 'node:util';
import chalk from 'chalk';
import { inspectify } from './inspectify';
import { type Labels, type LogLevel, type LogFn, type LogChildParams, type Log } from './types';
import { merge, join, formatDate, errorMessage } from './misc';

export type LogFormat = 'json' | 'human';

export type LogLine = {
  time: string;
  level: LogLevel;
  labels?: Labels;
  details?: object;
  msg?: string;
};

export type LogBackend = {
  write(line: LogLine): void;
};

export type LogStream = {
  write(line: string): void;
};

export const COLORS: { [key in LogLevel]: chalk.Chalk } = {
  trace: chalk.gray,
  debug: chalk.blue,
  info: chalk.green,
  warn: chalk.yellow,
  error: chalk.red,
  fatal: chalk.red,
};

export type LogImplParams = {
  level: LogLevel;
  labels?: Labels;
  details?: object;
  backend: LogBackend;
};

export class LogImpl {
  private _level: LogLevel;
  private _labels?: Labels;
  private _details?: object;
  private _backend: LogBackend;
  
  public trace!: LogFn;
  public debug!: LogFn;
  public info!: LogFn;
  public warn!: LogFn;
  public error!: LogFn;
  public fatal!: LogFn;
  
  constructor(params: LogImplParams) {
    const {
      level,
      labels,
      details,
      backend,
    } = params;
    
    this._level = level;
    this._labels = labels;
    this._details = details;
    this._backend = backend;
    
    this._reset();
  }
  
  private _reset(): void {
    const levels: LogLevel[] = ['fatal', 'error', 'warn', 'info', 'debug', 'trace'];
    const target = levels.indexOf(this._level);
    const noop = () => {};
    
    for (let i = 0; i < levels.length; i++) {
      const method = levels[i];
      this[method] = (i <= target) ? this._make(method) : noop;
    }
  }
  
  private _make(level: LogLevel): LogFn {
    return (maybeDetailsOrMsg?: object | string, maybeMsg?: string) => {
      const [details, msg] = (typeof maybeDetailsOrMsg == 'object')
        ? [maybeDetailsOrMsg, maybeMsg]
        : [undefined, maybeDetailsOrMsg]
      ;
      
      const now = new Date();
      
      this._backend.write({
        time: now.toISOString(),
        level,
        labels: this._labels,
        details: merge(this._details, details),
        msg,
      });
    };
  }
  
  public child(params?: LogChildParams): LogImpl {
    const {
      labels,
      details,
    } = params ?? {};
    
    return new LogImpl({
      level: this._level,
      labels: merge(this._labels, labels),
      details: merge(this._details, details),
      backend: this._backend,
    });
  }
}

export function createJsonLogBackend(stream: LogStream): LogBackend {
  return {
    write: line => stream.write(JSON.stringify(inspectify(line)) + '\n'),
  };
}

export function createHumanLogBackend(stream: LogStream): LogBackend {
  const inspect = (value: any) => util.inspect(value, {
    depth: 20,
    colors: true,
    maxArrayLength: 200,
    breakLength: 80,
  });
  
  return {
    write: line => {
      const { time, level, labels, details, msg } = line;
      
      let str = msg;
      
      if (details && 'err' in details) {
        str = join(str, errorMessage(details.err), ': ');
      }
      
      if (str) {
        str = chalk.cyan(str);
      }
      
      if (details) {
        str = join(str, inspect(details), ' ');
      }
      
      if (labels) {
        const keys = Object.keys(labels);
        const pairs = keys.map(key => `${key}=${labels[key]}`);
        
        str = join(str, chalk.grey(pairs.join(' ')), ' ');
      }
      
      const strTime = chalk.grey(formatDate(new Date(time)));
      const strLevel = COLORS[level](level.toUpperCase().padStart(5));
      const prefix = `[${strTime}] ${strLevel}`;
      
      str = join(prefix, str, ': ');
      
      stream.write(str + '\n');
    },
  };
}

export function createLogBackend(stream: LogStream, format?: LogFormat): LogBackend {
  if (format == 'json') {
    return createJsonLogBackend(stream);
  } else {
    return createHumanLogBackend(stream);
  }
}

export type CreateLogParams = {
  level: string;
  format: string;
};

export function createLog(params: CreateLogParams): LogImpl {
  return new LogImpl({
    level: params.level as LogLevel,
    backend: createLogBackend(process.stdout, params.format as LogFormat),
  });
}
