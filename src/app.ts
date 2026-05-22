import { type Labels } from './types';
import { Container } from './container';
import { Namespace } from './namespace';
import { Core } from './core';
import { Client } from './client';

export type AppClientOptions = {
  detach: boolean;
};

export type AppParams = {
  core: Core;
  workspace: string;
  name: string;
};

export class App {
  private _core: Core;
  private _workspace: string;
  private _name: string;
  private _container: Container;
  
  constructor(params: AppParams) {
    const {
      core,
      workspace,
      name,
    } = params;
    
    this._core = core;
    this._workspace = workspace;
    this._name = name;
    this._container = new Container();
  }
  
  public async init(): Promise<void> {
    // no-op
  }
  
  public async destroy(): Promise<void> {
    await this._container.destroy();
    await this._core.destroy();
  }
  
  public async client(
    project: string,
    labels?: Labels,
    options?: Partial<AppClientOptions>,
  ): Promise<Client> {
    const create = async () => {
      const namespace = new Namespace({
        workspace: this._workspace,
        project,
        // scope,
      });
      // const clientName = `${this._workspace}.${project}.${this._name}.${scope}`;
      
      const clientName = `${this._workspace}.${project}`;
      
      const client = new Client({
        telemetry: this._core.telemetry.child(clientName, { labels }),
        core: this._core,
        name: clientName,
        container: new Container(),
        namespace,
      });
      await client.init();
      
      return client;
    };
    
    if (options?.detach) {
      return await create();
    }
    
    return await this._container.use(create);
  }
}
