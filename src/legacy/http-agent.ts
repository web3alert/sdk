export type HttpAgentParams = {
  url: string;
  token: string;
};

export type HttpAgentRequestParams<P> = {
  path: string;
  method: 'get' | 'post' | 'put' | 'patch' | 'delete';
  body?: P;
};

export class HttpAgent {
  private url: string;
  private token: string;
  
  constructor(options: HttpAgentParams) {
    const {
      url,
      token,
    } = options;
    
    this.url = url;
    this.token = token;
  }
  
  public async request<P, R>(params: HttpAgentRequestParams<P>): Promise<R> {
    const {
      method,
      path,
      body,
    } = params;
    
    const response = await fetch(this.url + path, {
      method: method.toUpperCase(),
      headers: {
        'authorization': `Bearer ${this.token}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    const responseText = await response.text();
    
    if (response.status < 200 || response.status >= 300) {
      const responseJson = JSON.parse(responseText);
      const error = responseJson?.error;
      const code = responseJson?.error?.code;
      
      if (code && typeof code == 'string') {
        const err = new HttpAgentClientError(error.message, {
          code: error.code,
          payload: error.payload,
        });
        
        throw err;
      }
      
      throw new HttpAgentError(`HTTP Error: ${response.status} - ${response.statusText}`, {
        status: response.status,
        statusMessage: response.statusText,
        body: responseText,
      });
    }
    
    if (response.status == 204) {
      return undefined as R;
    }
    
    return JSON.parse(responseText);
  }
}

export type HttpAgentErrorOptions = ErrorOptions & {
  status: number;
  statusMessage: string;
  body?: string;
};

export class HttpAgentError extends Error {
  public status: number;
  public statusMessage: string;
  public body?: string;
  
  constructor(message: string, options: HttpAgentErrorOptions) {
    super(message, options);
    
    this.status = options.status;
    this.statusMessage = options.statusMessage;
    this.body = options.body;
  }
}

export type HttpAgentClientErrorOptions = ErrorOptions & {
  code: string;
  payload: any;
};

export class HttpAgentClientError extends Error {
  public code: string;
  public payload: any;
  
  constructor(message: string, options: HttpAgentClientErrorOptions) {
    super(message, options);
    
    this.code = options.code;
    this.payload = options.payload;
  }
}
