import { type Telemetry } from '../types';
import { type Container } from '../container';
import { type Core } from '../core';
import { type Namespace } from '../namespace';

export type BuilderContext = {
  telemetry: Telemetry;
  core: Core;
  container: Container;
  namespace: Namespace;
};
