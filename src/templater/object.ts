import { type JsonObject, type JsonValue } from '../types';
import { type StringInterpolateOptions, interpolate } from './string';

export type Execute<C, T> = (context: C) => T;

export type ObjectInterpolateOptions = {
  string: StringInterpolateOptions;
  object: {
    call: string;
  };
};

export function compile<C = object, T = any>(
  template: JsonValue,
  options?: ObjectInterpolateOptions,
): Execute<C, T> {
  return fn(js(template, options));
}

export function fn<C, T>(code: string): Execute<C, T> {
  const fn = new Function('context', `with (context) { return ${code}; }`);
  
  return fn as Execute<C, T>;
}

export function js(value: JsonValue, options?: ObjectInterpolateOptions): string {
  const opts = options ?? {
    string: { left: '{{', right: '}}', escape: '\\' },
    object: { call: '$' },
  };
  
  const type = typeof value;
  const map = (mappers as Record<string, Mapper<JsonValue>>)[type];
  if (!map) {
    throw new Error(`value cannot be of type '${type}'`);
  }
  
  return map(value, opts, js);
}

type MapperNext = (value: JsonValue, options: ObjectInterpolateOptions) => string;
type Mapper<T> = (value: T, options: ObjectInterpolateOptions, next: MapperNext) => string;
type Mappers = {
  boolean: Mapper<boolean>;
  number: Mapper<number>;
  string: Mapper<string>;
  object: Mapper<JsonObject>;
};

const mappers: Mappers = {
  boolean(value, options, next) {
    return `${value}`;
  },
  number(value, options, next) {
    return `${value}`;
  },
  string(value, options, next) {
    let template = false;
    let all = false;
    let replaced = interpolate(value, substring => {
      const capture = substring.trim();
      
      template = true;
      
      if (substring.length + 4 == value.length) {
        all = true;
        
        return capture;
      } else {
        return `\${${capture}}`;
      }
    }, options.string);
    
    if (template) {
      if (all) {
        return replaced;
      } else {
        return `\`${replaced}\``;
      }
    } else {
      return JSON.stringify(replaced);
    }
  },
  object(value, options, next) {
    if (value == null) {
      return 'null';
    } else if (Array.isArray(value)) {
      return `[${value.map(item => next(item, options)).join(', ')}]`;
    } else {
      const keys = Object.keys(value);
      const nextKey = (value: string) => {
        const code = next(value, options);
        
        return (code[0] == '"') ? code : `[${code}]`;
      };
      
      if (keys.length == 1) {
        const key = keys[0];
        
        if (key.startsWith(options.object.call)) {
          const unescaped = key.slice(options.object.call.length);
          const kvalue = value[key];
          
          if (unescaped.startsWith(options.object.call)) {
            return `{ ${nextKey(unescaped)}: ${next(kvalue, options)} }`;
          } else {
            const args = Array.isArray(kvalue) ? kvalue : [kvalue];
            
            return `${unescaped}(${args.map(arg => next(arg, options)).join(', ')})`;
          }
        }
      }
      
      return `{ ${keys.map(key => `${nextKey(key)}: ${next(value[key], options)}`).join(', ')} }`;
    }
  },
};
