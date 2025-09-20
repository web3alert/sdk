export type ConditionSelect = {
  param: string;
  condition: Condition;
};

export type ConditionValue = {
  op: string;
  value: unknown;
};

export type ConditionSelectValue = {
  param: string;
  op: string;
  value: unknown;
};

export type ConditionContains = {
  contains: Condition;
};

export type ConditionAll = {
  all: Condition[];
}

export type ConditionAny = {
  any: Condition[];
}

export type Condition =
  | ConditionSelect
  | ConditionValue
  | ConditionSelectValue
  | ConditionContains
  | ConditionAll
  | ConditionAny
;

export type ConditionTopLevel = ConditionAll | ConditionAny | null;
