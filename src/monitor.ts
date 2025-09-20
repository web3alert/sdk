export type AlertOptions = {
  warmup: number;
  cooldown: number;
  repeat: number;
};

export type AlertStatus = 'standby' | 'warmup' | 'firing' | 'cooldown';

export type AlertAction = 'none' | 'standby' | 'firing';

export enum ALERT_STATUS {
  STANDBY = 'standby',
  WARMUP = 'warmup',
  FIRING = 'firing',
  COOLDOWN = 'cooldown',
};

export enum ALERT_ACTION {
  NONE = 'none',
  STANDBY = 'standby',
  FIRING = 'firing',
};

export const ALERT_DEFAULT_OPTIONS: AlertOptions = {
  warmup: 0,
  cooldown: 0,
  repeat: 0,
};

export type AlertState = {
  status: AlertStatus;
  changed: number;
};

export type AlertParams = {
  now: number;
  matching: boolean;
  state?: AlertState;
  options?: Partial<AlertOptions>;
};

export type AlertResult = {
  state: AlertState;
  changed: boolean;
  action: AlertAction;
};

export function alert(params: AlertParams): AlertResult {
  const {
    now,
    matching,
  } = params;
  
  // TODO: make cleaner defaults substitution
  const options: AlertOptions = {
    warmup: params.options?.warmup ?? ALERT_DEFAULT_OPTIONS.warmup,
    cooldown: params.options?.cooldown ?? ALERT_DEFAULT_OPTIONS.cooldown,
    repeat: params.options?.repeat ?? ALERT_DEFAULT_OPTIONS.repeat,
  }
  
  let state: AlertState;
  let changed: boolean;
  let action = ALERT_ACTION.NONE;
  
  if (params.state) {
    state = Object.assign({}, params.state);
    changed = false;
  } else {
    state = {
      status: ALERT_STATUS.STANDBY,
      changed: now,
    };
    changed = true;
  }
  
  if (matching) {
    if (state.status == ALERT_STATUS.STANDBY) {
      // Go warmup
      
      state.status = ALERT_STATUS.WARMUP;
      state.changed = now;
      changed = true;
    }
    
    if (state.status == ALERT_STATUS.WARMUP) {
      // Try to go firing
      
      if (now - state.changed >= options.warmup) {
        state.status = ALERT_STATUS.FIRING;
        state.changed = now;
        changed = true;
        action = ALERT_ACTION.FIRING;
      }
    } else if (state.status == ALERT_STATUS.FIRING) {
      // Do nothing
      
      if (options.repeat > 0 && now - state.changed >= options.repeat) {
        state.changed = now;
        changed = true;
        action = ALERT_ACTION.FIRING;
      }
    } else if (state.status == ALERT_STATUS.COOLDOWN) {
      // Come back to firing
      
      state.status = ALERT_STATUS.FIRING;
      state.changed = now;
      changed = true;
    }
  } else {
    if (state.status == ALERT_STATUS.WARMUP) {
      // Come back to standby
      
      state.status = ALERT_STATUS.STANDBY;
      state.changed = now;
      changed = true;
    } else if (state.status == ALERT_STATUS.FIRING) {
      // Go to cooldown
      
      state.status = ALERT_STATUS.COOLDOWN;
      state.changed = now;
      changed = true;
    }
    
    if (state.status == ALERT_STATUS.COOLDOWN) {
      // Try to go to standby
      
      if (now - state.changed >= options.cooldown) {
        state.status = ALERT_STATUS.STANDBY;
        state.changed = now;
        changed = true;
        action = ALERT_ACTION.STANDBY;
      }
    }
  }
  
  return { state, changed, action };
}

export type MultialertInstance = {
  state: AlertState;
  seen: number;
};

export type MultialertState = Record<string, MultialertInstance>;

export type MultialertParams = {
  now: number;
  key: string;
  matching: boolean;
  state?: MultialertState;
  options?: Partial<AlertOptions>;
  ttl: number;
};

export type MultialertResult = {
  state: MultialertState;
  action: AlertAction;
};

export function multialert(params: MultialertParams): MultialertResult {
  const {
    now,
    key,
    matching,
    options,
    ttl,
  } = params;
  
  let state: MultialertState = Object.assign({}, params.state);
  
  let instance = state[key];
  if (!instance) {
    instance = {
      state: {
        status: ALERT_STATUS.STANDBY,
        changed: now,
      },
      seen: now,
    };
    
    state[key] = instance;
  }
  
  const instanceResult = alert({
    now,
    matching,
    state: instance.state,
    options,
  });
  
  instance.state = instanceResult.state;
  instance.seen = now;
  
  for (const key of Object.keys(state)) {
    const instance = state[key];
    
    if (now - instance.seen >= ttl) {
      delete state[key];
    }
  }
  
  return {
    state,
    action: instanceResult.action,
  };
}
