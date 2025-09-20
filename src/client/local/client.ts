import { type BaseClientLocalParams, BaseClientLocal } from '../base-client';
import { Namespace } from '../../namespace';
import { type EmitterBuilder, createEmitterBuilder } from './emitter';
import { type ListenerBuilder, createListenerBuilder } from './listener';
import { type BlockchainFactory, Blockchain } from './blockchain';

export type ClientLocalParams = BaseClientLocalParams & {
  namespace: Namespace;
};

export class ClientLocal extends BaseClientLocal {
  public namespace: Namespace;
  
  public emitter: EmitterBuilder;
  public listener: ListenerBuilder;
  public blockchain: BlockchainFactory;
  
  constructor(params: ClientLocalParams) {
    super(params);
    
    this.namespace = params.namespace;
    this.emitter = createEmitterBuilder(this);
    this.listener = createListenerBuilder(this);
    
    this.blockchain = async (name, specs, factory, options) => {
      return await this._use(async () => {
        const blockchain = new Blockchain({
          telemetry: this.telemetry.child(name),
          specs,
          factory,
          options,
        });
        await blockchain.init();
        
        return blockchain;
      });
    }
  }
}
