import { randomUUID } from 'node:crypto';
import { type Bucket } from './bucket';
import { type Core } from './core';
import { Web3alertError, isWeb3alertErrorLike } from './errors';

export type ActionDeliveryRef = { key: string; generation: string };
export type ActionDeliveryState = ActionDeliveryRef & {
  cancelled: boolean;
  /** Held only across admission -> synchronous HTTP invocation, NOT its response. */
  starting?: string;
};
export type ActionDeliveryContext = {
  /** Invoke the external request directly in start; do not queue or await here. */
  start<T>(request: () => Promise<T>): Promise<T>;
};

export function actionDeliveryError(kind: 'cancelled' | 'unavailable'): Web3alertError {
  return new Web3alertError(`action delivery ${kind}`, { details: { actionDelivery: kind } });
}

export function actionDeliveryErrorKind(error: unknown): 'cancelled' | 'unavailable' | undefined {
  for (let depth = 0; depth < 8 && isWeb3alertErrorLike(error); depth++, error = error.cause) {
    const kind = error.details?.['actionDelivery'];
    if (kind === 'cancelled' || kind === 'unavailable') return kind;
  }
  return undefined;
}

type Store = Pick<Bucket<ActionDeliveryState>, 'mutate' | 'get'>;

/** One durable record per subscription. No TTL/lease: an expired admission
 * could let a suspended old process start HTTP after cancellation confirmed.
 * Unknown storage/unfinished admission fails closed and never means cancelled. */
export class ActionDeliveryFence {
  constructor(private readonly store: Store) {}

  /** Controller only: call after an authoritative active subscription read.
   * Reuse the generation on technical restart; create one only after cancel. */
  async open(key: string): Promise<ActionDeliveryRef> {
    const candidate = { key, generation: randomUUID() };
    return await this.mutate<ActionDeliveryRef>(candidate, async (state, write) => {
      if (state && !state.cancelled) return { key, generation: state.generation };
      if (state?.starting) throw actionDeliveryError('unavailable');
      await write({ ...candidate, cancelled: false });
      return candidate;
    });
  }

  private async mutate<T>(ref: ActionDeliveryRef, callback: Parameters<Store['mutate']>[1]): Promise<T> {
    if (!ref.key || !ref.generation) throw actionDeliveryError('unavailable');
    try { return await this.store.mutate(ref.key, callback) as T; }
    catch (error) {
      if (actionDeliveryErrorKind(error)) throw error;
      throw actionDeliveryError('unavailable');
    }
  }

  async activate(ref: ActionDeliveryRef): Promise<void> {
    await this.mutate(ref, async (state, write) => {
      if (state?.generation === ref.generation) {
        if (state.cancelled) throw actionDeliveryError('cancelled');
        return;
      }
      if (state?.starting) throw actionDeliveryError('unavailable');
      await write({ ...ref, cancelled: false });
    });
  }

  async check(ref: ActionDeliveryRef): Promise<void> {
    await this.mutate(ref, async state => {
      if (!state) throw actionDeliveryError('unavailable');
      if (state.generation !== ref.generation || state.cancelled) throw actionDeliveryError('cancelled');
    });
  }

  /** Retry until acknowledged, BEFORE upstream unsubscribe. A pending start
   * does not require the external HTTP to finish. No lock is stolen on timeout. */
  async cancel(ref: ActionDeliveryRef): Promise<void> {
    await this.mutate(ref, async (state, write) => {
      if (state && state.generation !== ref.generation) return; // old cleanup, new activation
      if (!state?.cancelled) state = await write({ ...state, ...ref, cancelled: true });
      if (state.starting) throw actionDeliveryError('unavailable');
    });
  }

  async start<T>(ref: ActionDeliveryRef, request: () => Promise<T>): Promise<T> {
    const token = randomUUID();
    let acquisitionAttempted = false;
    try {
      await this.mutate(ref, async (state, write) => {
        if (!state) throw actionDeliveryError('unavailable');
        if (state.generation !== ref.generation || state.cancelled) throw actionDeliveryError('cancelled');
        if (state.starting) throw actionDeliveryError('unavailable');
        acquisitionAttempted = true;
        await write({ ...state, starting: token });
      });
    } catch (error) {
      // A lost write ACK may still have acquired this token. Release only our
      // token before reporting a safe-to-retry, pre-HTTP failure.
      if (acquisitionAttempted) await this.release(ref, token);
      throw error;
    }
    let response: Promise<T>;
    try {
      // A cancel may have arrived while the admission ACK was in flight.
      await this.check(ref);
      response = Promise.resolve(request());
    }
    catch (error) { response = Promise.reject(error); }
    // The HTTP is already issued; retain its result while acknowledging start.
    const outcome = response.then(value => ({ ok: true as const, value }), error => ({ ok: false as const, error }));
    // If storage goes away here, leave the start unconfirmed rather than lie
    // about cancellation. Retrying release cannot issue the HTTP a second time.
    await this.release(ref, token);
    const result = await outcome;
    if (!result.ok) throw result.error;
    return result.value;
  }

  private async release(ref: ActionDeliveryRef, token: string): Promise<void> {
    for (;;) {
      try {
        await this.mutate(ref, async (state, write) => {
          // A successful release with a lost ACK may already have allowed a
          // different start/generation. Never remove that new owner's token.
          if (!state || state.generation !== ref.generation || state.starting !== token) return;
          const { starting: _starting, ...released } = state;
          await write(released);
        });
        break;
      } catch { await new Promise(resolve => setTimeout(resolve, 100)); }
    }
  }
}

const fences = new WeakMap<Core, Promise<ActionDeliveryFence>>();
export function actionDeliveryFence(core: Core): Promise<ActionDeliveryFence> {
  let pending = fences.get(core);
  if (!pending) {
    pending = core.bucket<ActionDeliveryState>('action-delivery', { storage: 'file', ttl: 0 })
      .then(store => new ActionDeliveryFence(store)).catch(error => { fences.delete(core); throw error; });
    fences.set(core, pending);
  }
  return pending;
}
