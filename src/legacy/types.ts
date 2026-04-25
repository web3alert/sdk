export type Event = {
  name: string;
  params: {
    raw: Record<string, any>;
    human?: Record<string, any>;
  };
  payload?: Record<string, any>;
};

export type Notification = {
  params: Record<string, any>;
  event: Event;
};

export type EventV2 = {
  title?: string;
  short?: string;
  long?: string;
  icon?: string | null;
  cover?: string | null;
  avatar?: string | null;
  links?: EventV2Link[];
};

export type Bundle = {
  name: string;
  version?: string;
  types?: Record<string, unknown>;
  events?: unknown[];
  force?: boolean;
};

export type CustomBundleRequest = {
  name: string;
  [key: string]: unknown;
};

export type EventV2Link = {
  title: string;
  url: string;
};

export type SourceAnnounce = {
  customBundles?: string[];
};
