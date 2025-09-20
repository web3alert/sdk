import {
  type CollectFunction as PromCollect,
  Counter as PromCounter,
  Gauge as PromGauge,
  Histogram as PromHistogram,
  Summary as PromSummary,
  Registry,
  collectDefaultMetrics,
} from 'prom-client';
import {
  type Labels,
  type Collect,
  type CounterParams,
  type Counter,
  type GaugeParams,
  type Gauge,
  type HistogramParams,
  type Histogram,
  type SummaryParams,
  type Summary,
  type Metrics,
} from './types';

declare module 'prom-client' {
  namespace Histogram {
    interface Internal<T extends string> {
      startTimer(): (labels?: LabelValues<T>) => number;
    }
  }
}

export type MetricsParams = {
  parent?: MetricsImpl;
  labels?: Labels;
  defaultMetrics?: boolean;
};

export class MetricsImpl {
  private _parent: MetricsImpl | undefined;
  private _children: MetricsImpl[];
  private _labels: Labels;
  private _registry: Registry;
  
  constructor(params?: MetricsParams) {
    const {
      parent,
      labels = {},
      defaultMetrics = false,
    } = params ?? {};
    
    this._parent = parent;
    this._children = [];
    this._labels = labels;
    this._registry = new Registry();
    this._registry.setDefaultLabels(this._labels);
    
    if (defaultMetrics) {
      collectDefaultMetrics({ register: this._registry });
    }
  }
  
  public destroy(): void {
    if (this._parent) {
      this._parent._children.splice(this._parent._children.indexOf(this), 1);
    }
  }
  
  public child(labels?: Labels): Metrics {
    const child = new MetricsImpl({
      parent: this,
      labels: { ...this._labels, ...labels },
      defaultMetrics: false,
    });
    
    this._children.push(child);
    
    return child;
  }
  
  public counter(params: CounterParams): Counter {
    const { collect, ...rest } = params;
    
    return new PromCounter({
      ...rest,
      collect: recollect(collect),
      registers: [this._registry],
    });
  }
  
  public gauge(params: GaugeParams): Gauge {
    const { collect, ...rest } = params;
    
    return new PromGauge({
      ...rest,
      collect: recollect(collect),
      registers: [this._registry],
    });
  }
  
  public histogram(params: HistogramParams): Histogram {
    const { collect, ...rest } = params;
    
    return new PromHistogram({
      ...rest,
      collect: recollect(collect),
      registers: [this._registry],
    });
  }
  
  public summary(params: SummaryParams): Summary {
    const { collect, ...rest } = params;
    
    return new PromSummary({
      ...rest,
      collect: recollect(collect),
      registers: [this._registry],
    });
  }
  
  public async collect(): Promise<string> {
    let output = await this._registry.metrics();
    
    for (const child of this._children) {
      output += '\n' + await child.collect();
    }
    
    output = output.trim();
    
    return output + '\n';
  }
  
  public async render(): Promise<string> {
    return await this.collect();
  }
  
  public get contentType(): string {
    return Registry.PROMETHEUS_CONTENT_TYPE;
  }
}

export type CreateMetricsParams = {
  labels?: Labels;
  defaultMetrics?: boolean;
};

export function createMetrics(params?: CreateMetricsParams): MetricsImpl {
  return new MetricsImpl(params);
}

export function recollect<T>(collect: Collect<T> | undefined): PromCollect<T> | undefined {
  if (!collect) {
    return undefined;
  }
  
  return function () {
    return collect(this);
  };
}
