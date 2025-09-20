export type BackendBlockCallback = (block: number) => void;

export type BackendUnsubscribe = () => Promise<void>;

export type Backend = {
  init(): Promise<void>;
  destroy(): Promise<void>;
  subscribe(callback: BackendBlockCallback): Promise<BackendUnsubscribe>;
};

export type BackendFactory<S, B extends Backend> = (name: string, spec: S) => Promise<B>;
