import { type FunRefBuilder } from './fun';
import { type ActionRefBuilder } from './action';

export class Refs {
  public fun: FunRefBuilder;
  public action: ActionRefBuilder;
  
  constructor() {
    this.fun = name => ({
      params: params => ({
        result: () => {
          return { name, params };
        },
      }),
    });
    
    this.action = name => ({
      params: () => {
        return { name };
      },
    });
  }
}
