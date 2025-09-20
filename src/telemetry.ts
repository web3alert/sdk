import {
  type LogLevel,
  type Log,
  type Metrics,
  type Tracing,
  type Telemetry,
} from './types';
import { getString, getBoolean } from './env';
import { type LogFormat, LogImpl, createLog } from './log';
import { createMetrics, MetricsImpl } from './metrics';
import {
  createTracing,
  createTracingNoopBackend,
  createTracingConsoleBackend,
  TracingImpl,
} from './tracing';

export type TelemetryBundle = {
  log: LogImpl;
  metrics: MetricsImpl;
  tracing: TracingImpl;
  telemetry: Telemetry;
};

export type CreateTelemetryParams = {
  logLevel?: LogLevel;
  logFormat?: LogFormat;
  trace?: boolean;
};

export function createTelemetry(params?: CreateTelemetryParams): TelemetryBundle {
  const {
    logLevel = 'info',
    logFormat = 'human',
    trace = false,
  } = params ?? {};
  
  const log = createLog({
    level: logLevel,
    format: logFormat,
  });
  
  const metrics = createMetrics({
    defaultMetrics: true,
  });
  
  const tracing = createTracing({
    backend: (trace)
      ? createTracingConsoleBackend(process.stdout)
      : createTracingNoopBackend()
    ,
  });
  
  const telemetry = createTelemetryBase({
    log,
    metrics,
    tracing,
  });
  
  return {
    log,
    metrics,
    tracing,
    telemetry,
  };
}

export function getTelemetryParams(): CreateTelemetryParams {
  return {
    logLevel: getString('LOG_LEVEL', 'info') as LogLevel,
    logFormat: getString('LOG_FORMAT', 'json') as LogFormat,
    trace: getBoolean('TRACE', false),
  };
}

export type TelemetryBaseParams = {
  log: Log;
  metrics: Metrics;
  tracing: Tracing;
};

export function createTelemetryBase(params: TelemetryBaseParams): Telemetry {
  const {
    log,
    metrics,
    tracing,
  } = params;
  
  const _self: Telemetry = () => {};
  
  _self.log = log;
  _self.metrics = metrics;
  _self.tracing = tracing;
  
  _self.destroy = metrics.destroy.bind(metrics);
  _self.child = (tag, options) => {
    const {
      labels,
      details,
    } = options ?? {};
    
    const taggedLabels = { tag: `${tracing.tag}.${tag}`, ...labels };
    
    return createTelemetryBase({
      log: log.child({ labels: taggedLabels, details }),
      metrics: metrics.child(taggedLabels),
      tracing: tracing.child(tag),
    });
  };
  
  _self.debug = log.debug.bind(log);
  _self.info = log.info.bind(log);
  _self.warn = log.warn.bind(log);
  _self.error = log.error.bind(log);
  
  _self.counter = metrics.counter.bind(metrics);
  _self.gauge = metrics.gauge.bind(metrics);
  _self.histogram = metrics.histogram.bind(metrics);
  _self.summary = metrics.summary.bind(metrics);
  
  _self.span = tracing.span.bind(tracing);
  _self.wrap = tracing.wrap.bind(tracing);
  
  _self.trace = tracing.trace.bind(tracing);
  
  return _self;
}
