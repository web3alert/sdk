import { Self } from '../self';
import { type TriggerMethod, trigger } from './trigger';
import { type SubscriptionMethod, subscription } from './subscription';

export type TestNamespace = {
  trigger: TriggerMethod;
  subscription: SubscriptionMethod;
};

export function test(this: Self): TestNamespace {
  return {
    trigger: trigger.apply(this),
    subscription: subscription.apply(this),
  };
}
