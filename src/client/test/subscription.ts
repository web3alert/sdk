import { type Self } from '../self';
import { type TriggerDefinition, type InferTriggerTest } from '../trigger';
import { type SubscriptionRef } from '../subscription';

export type SubscriptionMethod = <D extends TriggerDefinition>(
  subscription: SubscriptionRef<D>,
  values: InferTriggerTest<D>,
) => Promise<void>;

export function subscription(this: Self): SubscriptionMethod {
  return async (subscription, values) => {
    await this.core.call<any, any>(`subscription.${subscription.name}.api.test`, { values });
  };
}
