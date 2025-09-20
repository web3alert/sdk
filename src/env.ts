export function getString(key: string, defaultValue: string): string {
  const value = process.env[key];
  
  if (value == undefined) {
    return defaultValue;
  }
  
  return value;
}

export function getNumber(key: string, defaultValue: number): number {
  const value = process.env[key];
  
  if (value == undefined) {
    return defaultValue;
  }
  
  const number = Number(value);
  
  if (Number.isNaN(number)) {
    throw new Error(`invalid number for environment variable '${key}'`);
  }
  
  return number;
}

export function getBoolean(key: string, defaultValue: boolean): boolean {
  const value = process.env[key];
  
  if (value == undefined) {
    return defaultValue;
  }
  
  if (value === 'true') {
    return true;
  }
  
  if (value === 'false') {
    return false;
  }
  
  throw new Error(`invalid boolean for environment variable '${key}'`);
}
