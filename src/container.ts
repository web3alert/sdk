import { type Mortal, type SpawnCallback } from './types';
import { kill } from './kill';

export class Container {
  private _mortals: Mortal[];
  
  constructor() {
    this._mortals = [];
  }
  
  public async use<T extends Mortal>(spawn: SpawnCallback<T>): Promise<T> {
    const mortal = await spawn();
    this._mortals.push(mortal);
    
    return mortal;
  }
  
  public async destroy(): Promise<void> {
    for (let i = this._mortals.length - 1; i >= 0; i--) {
      await kill(this._mortals[i]);
    }
  }
}
