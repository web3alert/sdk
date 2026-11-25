import { lookup } from 'node:dns/promises';
import { request as httpRequest, type IncomingHttpHeaders, type IncomingMessage, type OutgoingHttpHeaders } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { connect, isIP, type LookupFunction, type Socket } from 'node:net';
import { performance } from 'node:perf_hooks';
import { connect as connectTls } from 'node:tls';
import { createAddressCheck } from './outbound-address';

export { isOutboundAddressAllowed } from './outbound-address';

export type OutboundProtocol = 'http:' | 'https:' | 'ws:' | 'wss:';
export type OutboundAddress = { address: string; family: 4 | 6 };
/** A trusted resolver that returns all IPv4 and IPv6 answers for a hostname. */
export type OutboundResolver = (hostname: string) => Promise<readonly OutboundAddress[]>;

export interface OutboundPolicy {
  allowedProtocols?: readonly OutboundProtocol[];
  /** An entry replaces the default port for that protocol. */
  allowedPorts?: Partial<Record<OutboundProtocol, readonly number[]>>;
  allowedOrigins?: readonly string[];
  allowedPrivateCidrs?: readonly string[];
  resolve?: OutboundResolver;
}

export interface OutboundLookupOptions {
  policy?: OutboundPolicy;
  timeoutMs?: number;
  signal?: AbortSignal;
}

export interface OutboundRequestOptions extends OutboundLookupOptions {
  method?: string;
  headers?: Readonly<Record<string, string | readonly string[] | number>>;
  body?: string | Uint8Array;
  maxResponseBytes?: number;
  maxRedirects?: number;
  secretHeaders?: readonly string[];
  /** A trusted HTTP forward proxy origin. Targets are always tunneled by numeric IP. */
  proxy?: string | URL;
}

export interface OutboundResponse {
  status: number;
  headers: IncomingHttpHeaders;
  body: Buffer;
}

const errorMessages = {
  INVALID_POLICY: 'Invalid outbound request policy',
  INVALID_URL: 'Invalid outbound URL',
  INVALID_REQUEST: 'Invalid outbound request',
  PROTOCOL_DENIED: 'Outbound protocol is not allowed',
  PORT_DENIED: 'Outbound port is not allowed',
  ORIGIN_DENIED: 'Outbound origin is not allowed',
  ADDRESS_DENIED: 'Outbound address is not allowed',
  DNS_FAILED: 'Outbound DNS lookup failed',
  PROXY_FAILED: 'Outbound proxy connection failed',
  NETWORK_FAILED: 'Outbound network request failed',
  HTTP_STATUS: 'Outbound request returned an unsuccessful HTTP status',
  REDIRECT_LIMIT: 'Outbound redirect limit reached',
  RESPONSE_LIMIT: 'Outbound response exceeded the size limit',
  DEADLINE: 'Outbound deadline exceeded',
  ABORTED: 'Outbound request aborted',
};

export type OutboundErrorCode = keyof typeof errorMessages;

/** Contains a fixed message, a code and, for HTTP_STATUS, a numeric status. */
export class OutboundRequestError extends Error {
  constructor(readonly code: OutboundErrorCode, readonly status?: number) {
    super(errorMessages[code]);
    this.name = 'OutboundRequestError';
  }
}

const defaultPorts: Record<OutboundProtocol, number> = { 'http:': 80, 'https:': 443, 'ws:': 80, 'wss:': 443 };
const defaultTimeoutMs = 10_000;
const defaultMaxResponseBytes = 1024 * 1024;
const redirectStatuses = new Set([301, 302, 303, 307, 308]);
const controlledHeaders = new Set([
  'host', 'connection', 'proxy-connection', 'proxy-authorization', 'keep-alive',
  'content-length', 'transfer-encoding', 'te', 'trailer', 'upgrade', 'expect', 'accept-encoding',
]);

interface Target {
  url: URL;
  hostname: string;
  port: number;
  family: number;
}

function parseUrl(input: string | URL, base?: URL): URL {
  let url: URL;
  try {
    url = new URL(input, base);
  } catch {
    throw new OutboundRequestError('INVALID_URL');
  }
  if (!url.hostname || url.username || url.password) throw new OutboundRequestError('INVALID_URL');
  return url;
}

function hostnameOf(url: URL): string {
  return url.hostname.startsWith('[') ? url.hostname.slice(1, -1) : url.hostname;
}

function systemResolve(hostname: string) {
  return lookup(hostname, { all: true, verbatim: true });
}

function compilePolicy(policy: OutboundPolicy = {}) {
  try {
    const ports = new Map<OutboundProtocol, Set<number>>();
    for (const protocol of policy.allowedProtocols ?? ['http:', 'https:']) {
      if (!Object.hasOwn(defaultPorts, protocol)) throw new OutboundRequestError('INVALID_POLICY');
      const allowed = policy.allowedPorts?.[protocol] ?? [defaultPorts[protocol]];
      if (allowed.some(port => !Number.isInteger(port) || port < 1 || port > 65535)) {
        throw new OutboundRequestError('INVALID_POLICY');
      }
      ports.set(protocol, new Set(allowed));
    }
    const origins = policy.allowedOrigins && new Set(policy.allowedOrigins.map(input => {
      const url = parseUrl(input);
      if (!Object.hasOwn(defaultPorts, url.protocol) || url.href !== `${url.origin}/`) {
        throw new OutboundRequestError('INVALID_POLICY');
      }
      return url.origin;
    }));
    return { ports, origins, addressAllowed: createAddressCheck(policy.allowedPrivateCidrs), resolve: policy.resolve ?? systemResolve };
  } catch {
    throw new OutboundRequestError('INVALID_POLICY');
  }
}

type CompiledPolicy = ReturnType<typeof compilePolicy>;

function checkTarget(url: URL, policy: CompiledPolicy): Target {
  const protocol = url.protocol as OutboundProtocol;
  const allowedPorts = policy.ports.get(protocol);
  if (!allowedPorts) throw new OutboundRequestError('PROTOCOL_DENIED');
  const port = url.port ? Number(url.port) : defaultPorts[protocol];
  if (!allowedPorts.has(port)) throw new OutboundRequestError('PORT_DENIED');
  if (policy.origins && !policy.origins.has(url.origin)) throw new OutboundRequestError('ORIGIN_DENIED');

  const hostname = hostnameOf(url);
  const family = isIP(hostname);
  // Node skips lookup hooks for IP literals, so validate them at URL binding time.
  if (family && !policy.addressAllowed(hostname)) throw new OutboundRequestError('ADDRESS_DENIED');
  return { url, hostname, port, family };
}

async function resolveTarget(target: Target, policy: CompiledPolicy): Promise<OutboundAddress[]> {
  if (target.family === 4 || target.family === 6) return [{ address: target.hostname, family: target.family }];

  let answers;
  try {
    answers = (await policy.resolve(target.hostname)).map(({ address, family }) => ({ address, family }));
  } catch {
    throw new OutboundRequestError('DNS_FAILED');
  }
  if (!answers.length) throw new OutboundRequestError('DNS_FAILED');
  return answers.map(({ address, family }) => {
    if ((family !== 4 && family !== 6) || isIP(address) !== family || !policy.addressAllowed(address)) {
      throw new OutboundRequestError('ADDRESS_DENIED');
    }
    return { address, family };
  });
}

function validateInteger(value: number, min: number, max: number): void {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new OutboundRequestError('INVALID_POLICY');
}

function withDeadline<T>(
  timeoutMs: number,
  externalSignal: AbortSignal | undefined,
  run: (signal: AbortSignal, checkDeadline: () => void) => Promise<T>,
): Promise<T> {
  const controller = new AbortController();
  const { signal } = controller;
  const expires = performance.now() + timeoutMs;
  const expire = () => controller.abort(new OutboundRequestError('DEADLINE'));
  const cancel = () => controller.abort(new OutboundRequestError('ABORTED'));
  const checkDeadline = () => {
    if (performance.now() >= expires) expire();
    signal.throwIfAborted();
  };

  const timer = setTimeout(expire, timeoutMs);
  externalSignal?.addEventListener('abort', cancel, { once: true });
  if (externalSignal?.aborted) cancel();

  return new Promise<T>((resolve, reject) => {
    checkDeadline();
    signal.addEventListener('abort', () => reject(signal.reason), { once: true });
    run(signal, checkDeadline).then(result => {
      checkDeadline();
      resolve(result);
    }).catch(reject);
  }).finally(() => {
    clearTimeout(timer);
    externalSignal?.removeEventListener('abort', cancel);
  });
}

/**
 * Bind a Node lookup hook to the same final URL passed to the client.
 * Every connection resolves afresh, and validation precedes family selection.
 */
export function createOutboundLookup(input: string | URL, options: OutboundLookupOptions = {}): LookupFunction {
  const policy = compilePolicy(options.policy);
  const target = checkTarget(parseUrl(input), policy);
  const timeoutMs = options.timeoutMs ?? defaultTimeoutMs;
  validateInteger(timeoutMs, 1, 2_147_483_647);

  return (hostname, lookupOptions, callback) => {
    void withDeadline(timeoutMs, options.signal, async () => {
      if (hostname.toLowerCase() !== target.hostname) throw new OutboundRequestError('ORIGIN_DENIED');
      const answers = await resolveTarget(target, policy);
      const requestedFamily = lookupOptions.family;
      const family = requestedFamily === 'IPv4' ? 4 : requestedFamily === 'IPv6' ? 6 : requestedFamily;
      const candidates = family === 4 || family === 6 ? answers.filter(answer => answer.family === family) : answers;
      if (!candidates.length) throw new OutboundRequestError('DNS_FAILED');
      return candidates;
    }).then(answers => {
      if (lookupOptions.all) callback(null, answers);
      else callback(null, answers[0].address, answers[0].family);
    }, error => callback(error, ''));
  };
}

function parseProxy(input: string | URL): URL {
  const proxy = parseUrl(input);
  if (proxy.protocol !== 'http:' || proxy.href !== `${proxy.origin}/` || proxy.port === '0') {
    throw new OutboundRequestError('INVALID_POLICY');
  }
  return proxy;
}

function openTunnel(proxy: URL, target: Target, address: OutboundAddress, signal: AbortSignal): Promise<Socket> {
  const host = address.family === 6 ? `[${address.address}]` : address.address;
  const authority = `${host}:${target.port}`;
  return new Promise((resolve, reject) => {
    const fail = () => reject(new OutboundRequestError('PROXY_FAILED'));
    const request = httpRequest({
      hostname: hostnameOf(proxy),
      port: proxy.port || 80,
      path: authority,
      method: 'CONNECT',
      headers: { host: authority, connection: 'keep-alive' },
      agent: false,
      signal,
    });
    request.once('error', fail);
    request.once('connect', (response, socket, head) => {
      if (response.statusCode === 200 && head.length === 0 && !signal.aborted) resolve(socket);
      else {
        socket.destroy();
        fail();
      }
    });
    request.end();
  });
}

function openResponse(
  target: Target,
  address: OutboundAddress,
  method: string,
  headers: OutgoingHttpHeaders,
  body: string | Uint8Array | undefined,
  signal: AbortSignal,
  tunnel?: Socket,
): Promise<IncomingMessage> {
  const secure = target.url.protocol === 'https:';
  const send = secure ? httpsRequest : httpRequest;
  return new Promise((resolve, reject) => {
    const request = send({
      hostname: target.hostname,
      port: target.port,
      path: target.url.pathname + target.url.search,
      method,
      headers: {
        ...headers,
        host: target.url.host,
        'accept-encoding': 'identity',
        ...(body === undefined ? {} : { 'content-length': Buffer.byteLength(body) }),
      },
      signal,
      // A per-hop connection also bypasses global agents and ambient proxy settings.
      createConnection: () => {
        const socket = tunnel ?? connect({ host: address.address, family: address.family, port: target.port });
        return secure ? connectTls({
          socket,
          host: target.hostname,
          servername: target.family ? undefined : target.hostname,
          rejectUnauthorized: true,
        }) : socket;
      },
    }, resolve);
    request.once('error', reject);
    request.once('upgrade', (_response, socket) => {
      socket.destroy();
      reject(new OutboundRequestError('NETWORK_FAILED'));
    });
    request.end(body);
  });
}

async function readResponse(response: IncomingMessage, maxBytes: number, checkDeadline: () => void): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of response as AsyncIterable<Buffer>) {
    checkDeadline();
    size += chunk.length;
    if (size > maxBytes) throw new OutboundRequestError('RESPONSE_LIMIT');
    chunks.push(chunk);
  }
  if (!response.complete) throw new OutboundRequestError('NETWORK_FAILED');
  return Buffer.concat(chunks, size);
}

/** Make a bounded HTTP(S) request to an address validated by the outbound policy. */
export async function guardedRequest(input: string | URL, options: OutboundRequestOptions = {}): Promise<OutboundResponse> {
  const {
    signal,
    timeoutMs = defaultTimeoutMs,
    maxResponseBytes = defaultMaxResponseBytes,
    maxRedirects = 0,
  } = options;
  validateInteger(timeoutMs, 1, 2_147_483_647);
  validateInteger(maxResponseBytes, 0, Number.MAX_SAFE_INTEGER);
  validateInteger(maxRedirects, 0, Number.MAX_SAFE_INTEGER);

  try {
    return await withDeadline(timeoutMs, signal, async (requestSignal, checkDeadline) => {
      const policy = compilePolicy(options.policy);
      const proxy = options.proxy === undefined ? undefined : parseProxy(options.proxy);
      const secrets = ['authorization', 'cookie', ...(options.secretHeaders ?? []).map(name => name.toLowerCase())];
      const headers: OutgoingHttpHeaders = Object.create(null);
      for (const [name, value] of Object.entries(options.headers ?? {})) {
        const key = name.toLowerCase();
        if (!controlledHeaders.has(key)) headers[key] = typeof value === 'string' || typeof value === 'number' ? value : [...value];
      }
      let method = (options.method ?? 'GET').toUpperCase();
      let body = options.body;
      if (method === 'CONNECT' || (body !== undefined && (method === 'GET' || method === 'HEAD'))) {
        throw new OutboundRequestError('INVALID_REQUEST');
      }
      let url = parseUrl(input);

      for (let redirects = 0; ; redirects++) {
        checkDeadline();
        if (url.protocol !== 'http:' && url.protocol !== 'https:') throw new OutboundRequestError('PROTOCOL_DENIED');
        const target = checkTarget(url, policy);
        const [address] = await resolveTarget(target, policy);
        checkDeadline();
        const tunnel = proxy && await openTunnel(proxy, target, address, requestSignal);
        let response: IncomingMessage | undefined;
        try {
          checkDeadline();
          response = await openResponse(target, address, method, headers, body, requestSignal, tunnel);
          checkDeadline();
          const status = response.statusCode!;
          const location = response.headers.location;
          if (maxRedirects && location && redirectStatuses.has(status)) {
            if (redirects >= maxRedirects) throw new OutboundRequestError('REDIRECT_LIMIT');
            const next = parseUrl(location, url);
            if (next.origin !== url.origin) {
              for (const name of secrets) delete headers[name];
            }
            if (((status === 301 || status === 302) && method === 'POST') || (status === 303 && method !== 'HEAD')) {
              method = 'GET';
              body = undefined;
              for (const name of ['content-type', 'content-encoding', 'content-language', 'content-location']) {
                delete headers[name];
              }
            }
            url = next;
            continue;
          }
          if (status < 200 || status >= 300) throw new OutboundRequestError('HTTP_STATUS', status);
          const data = await readResponse(response, maxResponseBytes, checkDeadline);
          return { status, headers: response.headers, body: data };
        } finally {
          response?.destroy();
          tunnel?.destroy();
        }
      }
    });
  } catch (error) {
    if (error instanceof OutboundRequestError) throw error;
    throw new OutboundRequestError('NETWORK_FAILED');
  }
}
