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

export type TriggerOutput = {
  title?: string;
  message?: MessageVariants;
  links?: Link[];
  icon?: string;
  cover?: string;
  avatar?: string;
  data?: object;
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

export type ActionInput<P> = {
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
