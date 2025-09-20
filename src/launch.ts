import { type Log } from './types';

export type Module = {
  init(): Promise<void>;
  destroy(): Promise<void>;
};

export class MainModule {
  private readonly modules: Module[];
  
  constructor() {
    this.modules = [];
  }
  
  public use<M extends Module>(mod: M): M {
    this.modules.push(mod);
    
    return mod;
  }
  
  public async init(): Promise<void> {
    for (const mod of this.modules)
      await mod.init();
  }
  
  public async destroy(): Promise<void> {
    const reversed = this.modules.slice().reverse();
    for (const mod of reversed)
      await mod.destroy();
  }
}

export type LaunchMain = () => Promise<MainModule>;

export type LaunchParams = {
  log: Log;
};

export async function launch(main: LaunchMain, params: LaunchParams): Promise<void> {
  const {
    log,
  } = params;
  
  const fatal = (err: Error, message: string): void => {
    log.fatal({ err }, `${message}: ${err.message}`);
    process.exit(1);
  };
  
  process.on('uncaughtException', (err: Error) => fatal(err, 'uncaught exception'));
  process.on('unhandledRejection', (reason: {} | null | undefined, promise: Promise<any>) => {
    fatal(reason as Error, 'unhandled rejection');
  });
  
  try {
    const app = await main();
    
    await app.init();
    
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
    
    log.info('online');
    
    async function shutdown(): Promise<void> {
      process.removeListener('SIGINT', shutdown);
      process.removeListener('SIGTERM', shutdown);
      
      try {
        await app.destroy();
        
        log.info('offline');
      } catch (err) {
        fatal(err as Error, 'shutdown failed');
      }
    }
  } catch (err) {
    fatal(err as Error, 'boot failed');
  }
}
