import { type Telemetry } from '../types';
import { type Core } from '../core';
import { type MutexCell } from '../multimutex';
import { type BucketCell } from '../bucket';
import { Scheduler } from '../scheduler';
import { type CronExpression, parseExpression } from 'cron-parser';

export type CronCallback = (now: number, signal?: AbortSignal) => Promise<void>;

export type CronState = {
  next: string;
};

export type CronParams = {
  telemetry: Telemetry;
  core: Core;
  name: string;
  expression: string;
  callback: CronCallback;
  /** Replay each persisted due timestamp instead of coalescing downtime. */
  catchUp?: boolean;
};

export class Cron {
  private _telemetry: Telemetry;
  private _core: Core;
  private _name: string;
  private _expression: CronExpression;
  private _callback: CronCallback;
  private _mutex!: MutexCell;
  private _state!: BucketCell<CronState>;
  private _scheduler!: Scheduler;
  private _catchUp: boolean;
  private _closing = false;
  private _abort = new AbortController();
  private _running?: Promise<void>;
  
  constructor(params: CronParams) {
    const {
      telemetry,
      core,
      name,
      expression,
      callback,
    } = params;
    
    this._telemetry = telemetry;
    this._core = core;
    this._name = name;
    this._expression = parseExpression(expression);
    this._callback = callback;
    this._catchUp = params.catchUp === true;
  }
  
  public async init(): Promise<void> {
    this._mutex = this._core.mutex(`${this._name}.mutex`);
    this._state = this._core.registry.cell(`${this._name}.state`);
    
    this._scheduler = new Scheduler({
      callback: async (now, schedule) => {
        const run = this._tick(now, schedule);
        this._running = run;
        try { await run; }
        finally { if (this._running === run) this._running = undefined; }
      },
    });

    this._scheduler.schedule(0);
  }

  private async _tick(now: number, schedule: (delay: number) => void): Promise<void> {
    if (this._closing) return;
    try {
      let nextDelay: number | undefined;
      await this._mutex.lock(async () => {
        if (this._closing) return;
        await this._state.mutate(async (prev, write) => {
          this._abort.signal.throwIfAborted();
          this._expression.reset(now);
          if (prev) {
            const expected = Date.parse(prev.next);
            if (!Number.isFinite(expected)) throw new Error('invalid persisted cron timestamp');
            if (now >= expected) {
              await this._callback(expected, this._abort.signal);
              // Even a callback that handles cancellation cannot acknowledge
              // an input whose completion at shutdown is unknown.
              this._abort.signal.throwIfAborted();
              if (this._catchUp) this._expression.reset(expected);
              const next = this._expression.next();
              await write({ next: next.toISOString() });
              nextDelay = Math.max(0, next.getTime() - Date.now());
            } else {
              nextDelay = expected - now;
            }
          } else {
            const next = this._expression.next();
            await write({ next: next.toISOString() });
            nextDelay = next.getTime() - now;
          }
        });
      });
      // Do not start a second tick while releasing the distributed mutex.
      if (!this._closing && nextDelay != null) schedule(nextDelay);
    } catch (err) {
      if (!this._closing) {
        this._telemetry.warn({ err, cron: this._name }, 'cron input remains pending; retrying');
        const requested = (err as { redeliveryDelayMs?: unknown } | null)?.redeliveryDelayMs;
        if (typeof requested === 'number' && Number.isFinite(requested) && requested > 0) {
          schedule(Math.max(1000, requested));
        } else {
          // A persistent provider failure must not turn a two-hour timer into
          // paid RPC work every second. Keep its input, retry at its cadence.
          this._expression.reset(Date.now());
          const first = this._expression.next().getTime();
          const second = this._expression.next().getTime();
          schedule(Math.max(1000, second - first));
        }
      }
    }
  }
  
  public async destroy(): Promise<void> {
    this._closing = true;
    this._abort.abort(new Error('cron suspended for shutdown'));
    await this._scheduler.destroy();
    await this._running;
  }
}
