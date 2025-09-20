import { type BucketCell } from '../../bucket';

export type TriggerDefinition = {
  params: any;
  input: any;
  output: object;
  test: object;
};

export type InferTriggerParams<D extends TriggerDefinition> = D['params'];
export type InferTriggerInput<D extends TriggerDefinition> = D['input'];
export type InferTriggerOutput<D extends TriggerDefinition> = D['output'];
export type InferTriggerTest<D extends TriggerDefinition> = D['test'];

export type TriggerRef<D extends TriggerDefinition> = { name: string };

export type TriggerWorkerReducer<S, R> = (
  prev: S | undefined,
  write: (next: S) => Promise<void>,
) => Promise<R>;

export type TriggerWorker<P, O> = {
  params: P;
  reduce<S, R>(callback: TriggerWorkerReducer<S, R>): Promise<R>;
  publish(output: O | O[]): Promise<void>;
};

export type TriggerExecuteCallback<I> = (input: I) => Promise<void>;

export type TriggerDaemonCallback<P, I, O> = (
  worker: TriggerWorker<P, O>,
) => Promise<TriggerExecuteCallback<I>>;

export type TriggerTestCallback<T, I> = (test: T) => Promise<I | I[]>;

export type TriggerRunner<D extends TriggerDefinition> =
  TriggerDaemonCallback<InferTriggerParams<D>, InferTriggerInput<D>, InferTriggerOutput<D>>;

export type TriggerTester<D extends TriggerDefinition> =
  TriggerTestCallback<InferTriggerTest<D>, InferTriggerInput<D>>;

export type Trigger<D extends TriggerDefinition> = {
  execute(input: InferTriggerInput<D>): Promise<void>;
  test(test: InferTriggerTest<D>, params: InferTriggerParams<D>): Promise<InferTriggerOutput<D>[]>;
  ref: TriggerRef<D>;
};
