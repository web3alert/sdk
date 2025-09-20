import { type TriggerDefinition, type InferTriggerTest } from '../trigger';

export type SubscriptionRef<D extends TriggerDefinition> = { name: string };

export type Subscription<D extends TriggerDefinition> = {
  test(test: InferTriggerTest<D>): Promise<void>;
  ref: SubscriptionRef<D>;
};
