import { type Telemetry } from '../../../types';

export type BackendBlockCallback = (block: number) => void;

export type BackendUnsubscribe = () => Promise<void>;

export type Backend = {
  init(): Promise<void>;
  destroy(): Promise<void>;
  subscribe(callback: BackendBlockCallback): Promise<BackendUnsubscribe>;
  /** Optional adapter for transport housekeeping tied to head subscriptions. */
  setHeadSubscriptionActive?(active: boolean): Promise<void>;
};

export type BackendFactoryContext = {
  telemetry?: Telemetry;
};

export type BackendFactory<S, B extends Backend> = (
  name: string,
  spec: S,
  context?: BackendFactoryContext,
) => Promise<B>;
