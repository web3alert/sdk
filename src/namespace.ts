export type NamespaceParams = {
  workspace: string;
  project: string;
  // scope: string;
};

export class Namespace {
  public workspace: string;
  public project: string;
  // public scope: string;
  
  constructor(params: NamespaceParams) {
    const {
      workspace,
      project,
      // scope,
    } = params;
    
    this.workspace = workspace;
    this.project = project;
    // this.scope = scope;
  }
  
  public trigger(name: string): string {
    return `${this.workspace}.${this.project}.${name}`;
  }
}
