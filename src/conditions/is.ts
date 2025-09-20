import type {
  ConditionSelect,
  ConditionValue,
  ConditionSelectValue,
  ConditionContains,
  ConditionAll,
  ConditionAny,
  Condition,
} from './types';

export function isSelect(condition: Condition): condition is ConditionSelect {
  return ('param' in condition && 'condition' in condition);
}

export function isValue(condition: Condition): condition is ConditionValue {
  return (!('param' in condition) && 'op' in condition && 'value' in condition);
}

export function isSelectValue(condition: Condition): condition is ConditionSelectValue {
  return ('param' in condition && 'op' in condition && 'value' in condition);
}

export function isContains(condition: Condition): condition is ConditionContains {
  return ('contains' in condition);
}

export function isAll(condition: Condition): condition is ConditionAll {
  return ('all' in condition);
}

export function isAny(condition: Condition): condition is ConditionAny {
  return ('any' in condition);
}
