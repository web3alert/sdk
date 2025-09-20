import { type UseCallback } from './types';

export type DebouncerParams<T> = {
  callback: UseCallback<T>;
};

export class Debouncer<T> {
  private _callback: UseCallback<T>;
  private _next: { value: T } | undefined;
  private _promise: Promise<void> | undefined;
  
  constructor(params: DebouncerParams<T>) {
    const {
      callback,
    } = params;
    
    this._callback = callback;
    this._next = undefined;
    this._promise = undefined;
  }
  
  public async destroy(): Promise<void> {
    if (this._promise) {
      await this._promise;
    }
  }
  
  private _wake(): void {
    if (this._promise) {
      return;
    }
    
    if (this._next) {
      const { value } = this._next;
      
      this._next = undefined;
      this._executeInBackground(value);
    }
  }
  
  private _executeInBackground(value: T): void {
    this._promise = this._callback(value).then(() => {
      this._promise = undefined;
      this._wake();
    });
  }
  
  public push(value: T): void {
    this._next = { value };
    this._wake();
  }
}
