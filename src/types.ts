export type Tags = string[];
export type Labels = Record<string, string>;

export type JsonValue = JsonPrimitive | JsonObject | JsonArray;
export type JsonPrimitive = string | number | boolean | null;
export type JsonObject = { [key: string]: JsonValue };
export type JsonArray = JsonValue[];

export type LogLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal';

export type LogFn = {
  (msg?: string): void;
  (details?: object, msg?: string): void;
};

export type LogChildParams = {
  labels?: Labels;
  details?: object;
};

export type Log = {
  child(params?: LogChildParams): Log;
  trace: LogFn;
  debug: LogFn;
  info: LogFn;
  warn: LogFn;
  error: LogFn;
  fatal: LogFn;
};

export type Collect<T> = (metric: T) => void | Promise<void>;

export type Stop = () => number;

export type MetricParams = {
  name: string;
  help: string;
  labelNames?: string[];
};

export type CounterParams = MetricParams & {
  collect?: Collect<Counter>;
};

export type CounterMini = {
  inc(value?: number): void;
};

export type Counter = {
  inc(value?: number): void;
  inc(labels: Labels, value?: number): void;
  labels(labels: Labels): CounterMini;
  remove(labels: Labels): void;
  reset(): void;
};

export type GaugeParams = MetricParams & {
  collect?: Collect<Gauge>;
};

export type GaugeMini = {
  inc(value?: number): void;
  dec(value?: number): void;
  set(value: number): void;
};

export type Gauge = {
  inc(value?: number): void;
  inc(labels: Labels, value?: number): void;
  dec(value?: number): void;
  dec(labels: Labels, value?: number): void;
  set(value: number): void;
  set(labels: Labels, value: number): void;
  labels(labels: Labels): GaugeMini;
  remove(labels: Labels): void;
  reset(): void;
};

export type HistogramParams = MetricParams & {
  collect?: Collect<Histogram>;
  buckets?: number[];
};

export type HistogramMini = {
  observe(value: number): void;
  startTimer(): Stop;
};

export type Histogram = {
  observe(value: number): void;
  observe(labels: Labels, value: number): void;
  startTimer(labels?: Labels): Stop;
  labels(labels: Labels): HistogramMini;
  remove(labels: Labels): void;
  zero(labels: Labels): void;
  reset(): void;
};

export type SummaryParams = MetricParams & {
  collect?: Collect<Summary>;
	percentiles?: number[];
	maxAgeSeconds?: number;
	ageBuckets?: number;
};

export type SummaryMini = {
  observe(value: number): void;
  startTimer(): Stop;
};

export type Summary = {
  observe(value: number): void;
  observe(labels: Labels, value: number): void;
  startTimer(labels?: Labels): Stop;
  labels(labels: Labels): SummaryMini;
  remove(labels: Labels): void;
  reset(): void;
};

export type Metrics = {
  destroy(): void;
  child(labels?: Labels): Metrics;
  counter(params: CounterParams): Counter;
  gauge(params: GaugeParams): Gauge;
  histogram(params: HistogramParams): Histogram;
  summary(params: SummaryParams): Summary;
  render(): Promise<string>;
  contentType: string;
};

export type AsyncFunction<T, A extends any[], R> = (this: T, ...args: A) => Promise<R>;

export type Span = {
  origin: Tracing;
  parent: Span | null;
  name: string;
  depth: number;
  start: number;
  stop: number;
  enter(): void;
  exit(err?: unknown): void;
  trace(msg?: string): void;
  trace(details?: object, msg?: string): void;
};

export type SpanCallback<T> = (span: Span) => Promise<T>;

export type TracingSpanOptions = {
  root?: boolean;
};

export type TracingWrapOptions = TracingSpanOptions & {
  bind?: boolean;
};

export type Tracing = {
  tag: string;
  child(tag: string): Tracing;
  head(): Span;
  span<T>(
    name: string,
    callback: SpanCallback<T>,
    options?: TracingSpanOptions,
  ): Promise<T>;
  wrap<T, A extends any[], R>(
    name: string,
    fn: AsyncFunction<T, A, R>,
    options?: TracingWrapOptions,
  ): AsyncFunction<T, A, R>;
  trace(msg?: string): void;
  trace(details?: object, msg?: string): void;
};

export type TelemetryChildOptions = {
  labels?: Labels;
  details?: object;
};

export type Telemetry = {
  log: Log;
  metrics: Metrics;
  tracing: Tracing;
  
  destroy(): void;
  child(tag: string, options?: TelemetryChildOptions): Telemetry;
  
  debug: LogFn;
  info: LogFn;
  warn: LogFn;
  error: LogFn;
  
  counter(params: CounterParams): Counter;
  gauge(params: GaugeParams): Gauge;
  histogram(params: HistogramParams): Histogram;
  summary(params: SummaryParams): Summary;
  
  span<T>(
    name: string,
    callback: SpanCallback<T>,
    options?: TracingSpanOptions,
  ): Promise<T>;
  wrap<T, A extends any[], R>(
    name: string,
    fn: AsyncFunction<T, A, R>,
    options?: TracingWrapOptions,
  ): AsyncFunction<T, A, R>;
  
  trace(msg?: string): void;
  trace(details?: object, msg?: string): void;
};

export type ErrorCallback = (err: unknown) => void;

export type Callback<T = void> = () => Promise<T>;
export type CallbackSync<T = void> = () => T;

export type FallbackCallback<T> = (err: unknown) => Promise<T>;
export type FallbackCallbackSync<T> = (err: unknown) => T;

export type UseCallback<T> = (value: T) => Promise<void>;
export type UseCallbackSync<T> = (value: T) => void;

export type Destructible = {
  destroy(): Promise<void>;
};

export type Teardown = () => Promise<void>;

export type Mortal = Destructible | Teardown;

export type SpawnCallback<T extends Mortal = Mortal> = () => Promise<T>;
export type UseSpawnCallback = <T extends Mortal = Mortal>(spawn: SpawnCallback<T>) => Promise<T>;

export type Trapdoor = {
  uncaughtException: ErrorCallback;
};

export type Headers = Record<string, string | string[]>;
