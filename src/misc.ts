export function merge<T extends object>(a: T | undefined, b: T | undefined): T | undefined {
  if (a && b) {
    return { ...a, ...b };
  }
  
  return a || b;
}

export function join(a: string | undefined, b: string | undefined, sep: string): string {
  if (a && b) {
    return `${a}${sep}${b}`;
  } else {
    return a || b || '';
  }
}

export function formatDate(ts: Date): string {
  const HH = ts.getHours().toString().padStart(2, '0');
  const mm = ts.getMinutes().toString().padStart(2, '0');
  const ss = ts.getSeconds().toString().padStart(2, '0');
  const SSS = ts.getMilliseconds().toString().padStart(3, '0');
  
  return `${HH}:${mm}:${ss}.${SSS}`;
}

export function errorMessage(err: unknown): string {
  if (err instanceof Error) {
    if (err.cause) {
      return `${err.message}: ${errorMessage(err.cause)}`;
    }
    
    return err.message;
  }
  
  return '' + err;
}

export function start(): () => number {
  const startedAt = Date.now();
  
  return () => {
    const finistedAt = Date.now();
    return finistedAt - startedAt;
  };
}
