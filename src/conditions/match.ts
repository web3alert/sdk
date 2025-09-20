import _ from 'lodash';
import type {
  Condition,
  ConditionTopLevel,
} from './types';
import { Web3alertError } from '../errors';
import {
  isContains,
  isSelect,
  isSelectValue,
  isAll,
  isAny,
} from './is';

export function match(condition: ConditionTopLevel, value: unknown): boolean {
  if (condition == null) {
    return true;
  }
  
  return matchCondition({
    where: {
      condition: '',
      path: '',
    },
  }, condition, value);
}

type Operator = (param: unknown, value: unknown) => boolean;

type Context = {
  where: {
    condition: string;
    path: string;
  };
};

const OPERATORS: Record<string, Operator> = {
  eq: (param, value) => equals(param, value),
  ne: (param, value) => !equals(param, value),
  gt: (param, value) => (Number(param) > Number(value)),
  gte: (param, value) => (Number(param) >= Number(value)),
  lt: (param, value) => (Number(param) < Number(value)),
  lte: (param, value) => (Number(param) <= Number(value)),
};

function matchCondition(ctx: Context, condition: Condition, value: unknown): boolean {
  const {
    where,
  } = ctx;
  
  try {
    if (isAny(condition)) {
      return condition.any.some((child, index) => matchCondition({
        where: {
          condition: `${where.condition}.any[${index}]`,
          path: where.path,
        },
      }, child, value));
    } else if (isAll(condition)) {
      return condition.all.every((child, index) => matchCondition({
        where: {
          condition: `${where.condition}.all[${index}]`,
          path: where.path,
        },
      }, child, value));
    } else if (isSelectValue(condition)) {
      const targetValue = _.get(value, condition.param);
      
      return matchBasic({
        where: {
          condition: where.condition,
          path: `${where.path}.${condition.param}`,
        },
      }, targetValue, condition.op, condition.value);
    } else if (isSelect(condition)) {
      const targetValue = _.get(value, condition.param);
      
      return matchCondition({
        where: {
          condition: `${where.condition}.condition`,
          path: `${where.path}.${condition.param}`,
        },
      }, condition.condition, targetValue);
    } else if (isContains(condition)) {
      if (!Array.isArray(value)) {
        throw new Web3alertError('target value must be an array', {
          details: { where },
        });
      }
      
      return value.some((item, index) => {
        return matchCondition({
          where: {
            condition: `${where.condition}.contains`,
            path: `${where.path}[${index}]`,
          },
        }, condition.contains, item);
      });
    } else {
      return matchBasic({ where }, value, condition.op, condition.value);
    }
  } catch (err) {
    throw new Web3alertError('match condition failed', {
      cause: err,
      details: { where },
    });
  }
}

function matchBasic(ctx: Context, a: unknown, op: string, b: unknown): boolean {
  const {
    where,
  } = ctx;
  
  try {
    const operator = OPERATORS[op];
    if (!operator) {
      throw new Web3alertError('unknown operator', { details: { where, operator: op } });
    }
    
    return operator(a, b);
  } catch (err) {
    throw new Web3alertError('condition operator failed', {
      cause: err,
      details: { where, op, a, b },
    });
  }
}

function equals(param: unknown, value: unknown): boolean {
  if (typeof param == 'string' && typeof value == 'string') {
    return (param.toLowerCase() == value.toLowerCase());
  }
  
  return (param === value);
}
