export type Web3alertErrorOptions = ErrorOptions & {
  details?: Record<string, unknown>;
};

export class Web3alertError extends Error {
  public details?: Record<string, unknown>;
  
  constructor(message?: string, options?: Web3alertErrorOptions) {
    super(message, options);
    
    if (options?.details) {
      this.details = options.details;
    }
  }
}

export type Web3alertClientErrorOptions = Web3alertErrorOptions;
export class Web3alertClientError extends Web3alertError {}

export function error(
  message?: string,
  options?: Web3alertClientErrorOptions,
): Web3alertClientError {
  return new Web3alertClientError(message, options);
}

export type Web3alertErrorLike = {
  message: string;
  cause?: unknown;
  details?: Record<string, unknown>;
};

export function isWeb3alertErrorLike(err: unknown): err is Web3alertErrorLike {
  return (!!err && typeof err == 'object' && 'message' in err && typeof err.message == 'string');
}

export function serializeError(err: unknown): unknown {
  if (err instanceof Web3alertClientError) {
    return {
      code: 'EFAIL',
      message: err.message,
      details: err.details,
    };
  }
  
  if (err instanceof Web3alertError) {
    return {
      code: 'EINTERNAL',
      message: err.message,
      details: err.details,
    };
  }
  
  return {
    code: 'EINTERNAL',
    message: 'internal error',
  };
}

export function deserializeError(value: unknown): Web3alertError {
  if (isWeb3alertErrorLike(value) && 'code' in value && typeof value.code === 'string') {
    if (value.code == 'EFAIL') {
      return new Web3alertClientError(value.message, {
        cause: value.cause,
        details: value.details,
      });
    }
    
    return new Web3alertError(value.message, { details: value.details });
  }
  
  return new Web3alertError('unknown error', { details: { value } });
}
