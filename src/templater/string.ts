export type ReplaceCallback = (substring: string) => string;

export type StringInterpolateOptions = {
  left: string;
  right: string;
  escape: string;
};

export function interpolate(
  str: string,
  replace: ReplaceCallback,
  options?: StringInterpolateOptions,
): string {
  const {
    left,
    right,
    escape,
  } = options ?? { left: '{{', right: '}}', escape: '\\' };
  const escapables = [left, right, escape];
  
  let result = '';
  let token = '';
  let i = 0;
  let raw = true;
  
  while (i < str.length) {
    const sub = str.substring(i);
    
    if (sub.startsWith(escape)) {
      const after = sub.substring(escape.length);
      const escapable = escapables.find(escapable => after.startsWith(escapable));
      if (!escapable) {
        throw new Error(`invalid escaping at ${i}`);
      }
      
      token += escapable;
      i += escape.length + escapable.length;
    } else if (raw && sub.startsWith(left)) {
      result += token;
      token = '';
      i += left.length;
      raw = false;
    } else if (!raw && sub.startsWith(right)) {
      result += replace(token);
      token = '';
      i += right.length;
      raw = true;
    } else {
      token += sub[0];
      i++;
    }
  }
  
  result += token;
  
  return result;
}
