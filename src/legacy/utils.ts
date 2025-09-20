import {
  type SubscriptionRuleRaw,
  type SubscriptionObjectRaw,
} from '@web3alert/types';



export type EventReference = {
  name: string;
  source: string;
  bundle: string;
  payload?: object;
};

export function getEventReference(rule: SubscriptionRuleRaw): EventReference {
  const parts = rule.event.split('.');
  
  return {
    name: parts.slice(2).join('.'),
    source: parts[0],
    bundle: parts[1],
    payload: rule.payload,
  };
}

export function getUniqEvents(
  source: string,
  bundle: string,
  subscriptions: SubscriptionObjectRaw[],
): EventReference[] {
  const prefix = `${source}.${bundle}`;
  const events = new Map<string, EventReference>();
  
  for (const subscription of subscriptions) {
    for (const rule of subscription.rules) {
      if (rule.event.startsWith(prefix)) {
        if (!events.has(rule.event)) {
          events.set(rule.event, getEventReference(rule));
        }
      }
    }
  }
  
  return Array.from(events.values());
}
