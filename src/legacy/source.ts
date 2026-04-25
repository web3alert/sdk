import {
  type Bundle,
  type CustomBundleRequest,
  type SourceAnnounce,
  type Event,
} from './types';

export type Issue = {
  message: string;
  details?: object;
};

export type HandleResult = {
  events: Event[];
  warnings?: Issue[];
};

export class LegacySource<Task> {
  constructor() {
  }
  
  public async init(): Promise<void> {
  }
  
  public async destroy(): Promise<void> {
  }
  
  public announce(): SourceAnnounce {
    return { customBundles: [] };
  }
  
  public async bundles(): Promise<Bundle[]> {
    return [];
  }
  
  public async customBundle(request: CustomBundleRequest): Promise<Bundle> {
    throw new Error('custom bundles are not supported');
  }
  
  public async handle(task: Task): Promise<HandleResult> {
    return { events: [] };
  }
  
  public async human(bundle: string, event: Event): Promise<Event> {
    return event;
  }
}
