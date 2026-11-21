import { type Destructible } from '../types';

export type EventCallback<T> = (event: T) => Promise<void>;

export type Source<T> = {
  listen(callback: EventCallback<T>): Promise<Destructible>;
};

export type MessageVariants = {
  short: string;
  long?: string;
};

export type Link = {
  title: string;
  url: string;
};

export type TriggerRuntimeMetadata = {
  triggerFullname?: string;
  revision?: number;
  durationMs?: number;
  outputIndex?: number;
  outputCount?: number;
  eventId?: string;
  sourceFullname?: string;
  bindingId?: string;
};

export type TriggerOutput = {
  title?: string;
  message?: MessageVariants;
  links?: Link[];
  icon?: string;
  cover?: string;
  avatar?: string;
  data?: object;
  raw?: Record<string, unknown>;
  human?: Record<string, unknown>;
  payload?: {
    __runtime?: TriggerRuntimeMetadata;
    __system?: Record<string, unknown>;
    [key: string]: unknown;
  };
};

export type TriggerInfo = {
  name: string;
  slug: string;
  title?: string;
};

export type ProjectInfo = {
  name: string;
  slug: string;
  title?: string;
};

export type ActionEvent = {
  /** Delivery ID (`evt_` followed by 32 lowercase hex characters), stable across attempts. */
  id: string;
  /** `subscription.event` for a regular delivery or `subscription.test` for a test delivery. */
  type: string;
  /** ISO 8601 UTC time when the delivery was first built, stable across attempts. */
  timestamp: string;
  /** 1-based delivery attempt number. */
  attempt: number;
};

export type ActionInput<P> = {
  delivery?: import('../action-delivery').ActionDeliveryRef;
  event?: ActionEvent;
  workspace: string;
  subscription: string;
  trigger: TriggerInfo;
  project: ProjectInfo;
  title?: string;
  message: MessageVariants;
  links?: Link[];
  icon?: string;
  cover?: string;
  avatar?: string;
  data?: object;
  params: P;
};
