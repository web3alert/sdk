import { type Self } from '../self';
import {
  type TriggerDefinition,
  type InferTriggerParams,
  type InferTriggerOutput,
  type InferTriggerTest,
  type TriggerRef,
} from '../trigger';

export type TriggerMethod = <D extends TriggerDefinition>(
  trigger: TriggerRef<D>,
  values: InferTriggerTest<D>,
  params?: InferTriggerParams<D>,
) => Promise<InferTriggerOutput<D>[]>;

export function trigger(this: Self): TriggerMethod {
  return async (trigger, values, params) => {
    const result = await this.core.call<any, any>(`trigger.${trigger.name}.api.test`, {
      values,
      params,
    });
    
    return result.events;
  };
}
