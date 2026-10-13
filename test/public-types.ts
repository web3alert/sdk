import {
  HttpAgent,
  HttpAgentClientError,
  HttpAgentError,
  type Event,
  type ClientLocal,
  type HttpAgentClientErrorOptions,
  type HttpAgentErrorOptions,
  type HttpAgentParams,
  type HttpAgentRequestParams,
} from '../dist';

declare const source: Awaited<ReturnType<ClientLocal['blockchain']>>;
const pause: Promise<void> = source.setHeadSubscriptionActive(false);
const headOptions: Parameters<ClientLocal['blockchain']>[3] = { headSubscriptionActive: false };
void pause;
void headOptions;

const event: Event = {
  name: 'transfer',
  params: {
    raw: { from: '0x1', to: '0x2' },
    human: { amount: '1 ETH' },
  },
  payload: { chain: 'ethereum' },
};

const agentParams: HttpAgentParams = {
  url: 'https://api.example.test',
  token: 'token',
};
const requestParams: HttpAgentRequestParams<{ enabled: boolean }> = {
  path: '/resource',
  method: 'patch',
  body: { enabled: true },
};
const errorOptions: HttpAgentErrorOptions = {
  status: 503,
  statusMessage: 'Unavailable',
  body: 'retry later',
};
const clientErrorOptions: HttpAgentClientErrorOptions = {
  code: 'invalid_resource',
  payload: { resource: 'telegram' },
};

const agent = new HttpAgent(agentParams);
const httpError = new HttpAgentError('failed', errorOptions);
const clientError = new HttpAgentClientError('invalid', clientErrorOptions);

void event;
void requestParams;
void agent;
void httpError;
void clientError;
