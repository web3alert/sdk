import fs from 'node:fs/promises';
import { parse, stringify } from 'yaml';

export * as yaml from 'yaml';

export async function readYamlFile<T>(path: string): Promise<T> {
  const data = await fs.readFile(path, { encoding: 'utf-8' });
  const value = parse(data) as T;
  
  return value;
}

export async function writeYamlFile<T>(path: string, value: T): Promise<void> {
  const data = stringify(value);
  
  await fs.writeFile(path, data, { encoding: 'utf-8' });
}

export async function readJsonFile<T>(path: string): Promise<T> {
  const data = await fs.readFile(path, { encoding: 'utf-8' });
  const value = JSON.parse(data) as T;
  
  return value;
}

export async function writeJsonFile<T>(path: string, value: T): Promise<void> {
  const data = JSON.stringify(value);
  
  await fs.writeFile(path, data, { encoding: 'utf-8' });
}
