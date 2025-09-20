import { type Callback, type UseSpawnCallback } from './types';
import { oneshot } from './utils';

export async function interruption(): Promise<void> {
  return new Promise(resolve => process.once('SIGINT', resolve));
}

export type MainCallback = (
  use: UseSpawnCallback,
  interruption: Callback,
) => Promise<void>;

export function main(callback: MainCallback): void {
  oneshot(async use => {
    await callback(use, interruption);
  }).catch(err => {
    console.error(err);
    process.exit(1);
  });
}
