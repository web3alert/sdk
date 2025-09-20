import * as net from 'net';
import * as http from 'http';
import { type Log } from './types';

export type WebServerConnection = {
  socket: net.Socket;
  idle: boolean;
};

export type WebServerParams = {
  tag?: string;
  log: Log;
  server?: http.Server;
  handler: (req: http.IncomingMessage, res: http.ServerResponse) => void;
  listen?: net.ListenOptions;
};

export class WebServer {
  private destroyed: boolean;
  private readonly connections: Map<net.Socket, WebServerConnection>;
  
  public readonly tag: string;
  public readonly log: Log;
  public readonly server: http.Server;
  public readonly listenOptions?: net.ListenOptions;
  
  constructor(params: WebServerParams) {
    this.destroyed = false;
    this.connections = new Map();
    
    this.tag = params.tag || 'http-server';
    this.log = params.log.child({ labels: { tag: this.tag } });
    
    const server = params.server || http.createServer();
    server.on('request', params.handler);
    this.server = server;
    
    this.listenOptions = params.listen;
  }
  
  private trackConnection(socket: net.Socket): void {
    const conn = { socket, idle: true };
    this.connections.set(socket, conn);
    
    socket.on('close', () => {
      this.connections.delete(socket);
    });
  }
  
  private trackRequest(req: http.IncomingMessage, res: http.ServerResponse): void {
    const conn = this.connections.get(req.socket) as WebServerConnection;
    conn.idle = false;
    
    res.on('finish', () => {
      conn.idle = true;
      this.destroyConnection(conn);
    });
  }
  
  private destroyConnection(conn: WebServerConnection): void {
    if (this.destroyed && conn.idle)
      conn.socket.destroy();
  }
  
  public async init(): Promise<void> {
    const server = this.server;
    
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(this.listenOptions, () => {
        server.removeListener('error', reject);
        
        server.on('connection', socket => this.trackConnection(socket));
        server.on('request', (req: http.IncomingMessage, res: http.ServerResponse) => {
          this.trackRequest(req, res);
        });
        
        const address = server.address();
        
        this.log.info({ address }, 'listening');
        
        resolve();
      });
    });
  }
  
  public async destroy(): Promise<void> {
    await new Promise<void>(resolve => {
      this.server.once('close', resolve);
      this.server.close();
      
      this.destroyed = true;
      for (const conn of this.connections.values())
        this.destroyConnection(conn);
    });
    
    this.log.info('closed');
  }
}
