import { type TriggerDefinition } from '../trigger';
import { type SubscriptionRef } from './types';

export function subscription<D extends TriggerDefinition>(name: string): SubscriptionRef<D> {
  return { name };
}
