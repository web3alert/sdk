import { type TriggerDefinition, type InferTriggerTest } from '../trigger';

export type SubscriptionRef<D extends TriggerDefinition> = { name: string };

export type Subscription<D extends TriggerDefinition> = {
  test(test: InferTriggerTest<D>): Promise<void>;
  /** Close this local handle without unsubscribing or deleting its durable consumer.
   * Recreate the same subscription name/params to resume pending delivery.
   * Subsequent client teardown does not undo the suspension; real removal must
   * destroy a resumed handle or explicitly remove the retained resources.
   */
  suspend(): Promise<void>;
  ref: SubscriptionRef<D>;
};
