import { type TriggerDefinition, type TriggerRef } from './types';

export function trigger<D extends TriggerDefinition>(name: string): TriggerRef<D> {
  return { name };
}
