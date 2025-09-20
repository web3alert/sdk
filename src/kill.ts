import { type Mortal } from './types';

export async function kill(mortal: Mortal): Promise<void> {
  if (typeof mortal == 'function') {
    return await mortal();
  }
  
  await mortal.destroy();
}
