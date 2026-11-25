import {
  HttpAgent,
  HttpAgentClientError,
  HttpAgentError,
  OutboundRequestError,
  createOutboundLookup,
  guardedRequest,
  isOutboundAddressAllowed,
  type OutboundAddress,
  type OutboundErrorCode,
  type OutboundLookupOptions,
  type OutboundPolicy,
  type OutboundProtocol,
  type OutboundRequestOptions,
  type OutboundResolver,
  type OutboundResponse,
  type Event,
  type ClientLocal,
  type HttpAgentClientErrorOptions,
  type HttpAgentErrorOptions,
  type HttpAgentParams,
  type HttpAgentRequestParams,
  type Subscription,
  type Trigger,
  type ClientTriggerParams,
  type Client,
  type ActionDeliveryContext,
  type ActionEvent,
  type ActionInput,
} from '../dist';
import type { SocketConnectOpts } from 'node:net';

const outboundAddress: OutboundAddress = { address: '8.8.8.8', family: 4 };
const resolveOutbound: OutboundResolver = async (_hostname: string) => [outboundAddress];
const wss: OutboundProtocol = 'wss:';
const outboundPolicy: OutboundPolicy = {
  allowedProtocols: [wss],
  allowedPorts: { 'wss:': [443, 8443] },
  allowedOrigins: ['wss://rpc.example.test:8443'],
  allowedPrivateCidrs: ['127.0.0.1/32'],
  resolve: resolveOutbound,
};
const outboundLookupOptions: OutboundLookupOptions = { policy: outboundPolicy };
const wsLookup = createOutboundLookup(new URL('wss://rpc.example.test:8443'), outboundLookupOptions);
const socketOptions: SocketConnectOpts = { host: 'rpc.example.test', port: 8443, lookup: wsLookup };
const outboundOptions: OutboundRequestOptions = {
  method: 'POST',
  body: Buffer.from('{}'),
  headers: { 'content-type': 'application/json' },
  timeoutMs: 2000,
  maxResponseBytes: 4096,
  maxRedirects: 1,
  secretHeaders: ['x-auth-token'],
  proxy: 'http://squid:8080',
};
const outboundResult: Promise<OutboundResponse> = guardedRequest('https://example.test', outboundOptions);
const outboundBody: Promise<Buffer> = outboundResult.then(response => response.body);
const outboundAllowed: boolean = isOutboundAddressAllowed('::1', ['::1/128']);
const outboundCode: OutboundErrorCode = new OutboundRequestError('HTTP_STATUS', 429).code;
void [socketOptions, outboundBody, outboundAllowed, outboundCode];
// @ts-expect-error Node's HTTP client options cannot override the pinned connection
void guardedRequest('https://example.test', { lookup: wsLookup });

declare const client: Client;
void client.action('send').params<{ url: string }>().callback(async (input, delivery: ActionDeliveryContext) => {
  await delivery.start(() => fetch(input.params.url));
}, { guardedDelivery: true });

const actionEvent: ActionEvent = {
  id: 'evt_0123456789abcdef0123456789abcdef',
  type: 'subscription.event',
  timestamp: '2026-11-21T00:00:00Z',
  attempt: 1,
};
const actionInput: ActionInput<{ url: string }> = {
  workspace: 'workspace',
  subscription: 'subscription',
  trigger: { name: 'Transfer', slug: 'transfer' },
  project: { name: 'Project', slug: 'project' },
  message: { short: 'Transfer' },
  params: { url: 'https://example.test' },
};
const actionInputWithEvent: ActionInput<{ url: string }> = { ...actionInput, event: actionEvent };
const receivedActionEvent: ActionEvent | undefined = actionInputWithEvent.event;
void receivedActionEvent;

declare const source: Awaited<ReturnType<ClientLocal['blockchain']>>;
const pause: Promise<void> = source.setHeadSubscriptionActive(false);
const headOptions: Parameters<ClientLocal['blockchain']>[3] = { headSubscriptionActive: false };
void pause;
void headOptions;

type SampleTrigger = { params: { wallet: string }; input: number; output: { value: number }; test: { block: number } };
declare const subscription: Subscription<SampleTrigger>;
declare const trigger: Trigger<SampleTrigger>;
void subscription.suspend();
void trigger.executeForParams(1, [{ wallet: '0x1' }]);
const retainedTrigger: ClientTriggerParams<SampleTrigger> = {
  name: 'retained', streamOptions: { maxAge: 0, maxMessages: 1000 },
  runner: async ({ publish }) => async value => publish({ value }),
  tester: async () => [],
};
void retainedTrigger;
// @ts-expect-error explicit dispatch retains the trigger's parameter contract
void trigger.executeForParams(1, [{ wallet: 42 }]);

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
