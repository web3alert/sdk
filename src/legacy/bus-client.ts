import EventEmitter from 'events';
import { type ConfirmChannel } from 'amqplib';
import {
  AmqpConnectionManager,
  ChannelWrapper,
  connect,
} from 'amqp-connection-manager';
import { type Event, type Notification } from './types';

export type MessageHandler<T> = (message: T | T[]) => Promise<void>;
export type EventHandler = MessageHandler<Event>;
export type NotificationHandler = MessageHandler<Notification>;

const DEFAULT_MAX_QUEUE_SIZE: number = 512;
const INBOX_QUEUE_NAME: string = 'inbox';

function getChannelQueueName(channelName: string): string {
  return `channel-${channelName}`;
}

export type BusClientParams = {
  url: string;
  maxQueueLength?: number;
};

export class BusClient extends EventEmitter {
  private connection: AmqpConnectionManager;
  private channel: ChannelWrapper;
  private maxQueueLength: number;
  private assertedQueues: Set<string>;
  
  constructor(params: BusClientParams) {
    super();
    
    const {
      url,
      maxQueueLength,
    } = params;
    
    this.connection = connect([url]);
    
    const channel = this.connection.createChannel({
      json: true,
    });
    channel.on('connect', () => {
      this.emit('connect');
    });
    channel.on('disconnect', () => {
      this.emit('disconnect');
    });
    channel.on('error', err => {
      this.emit('error', err);
    });
    this.channel = channel;
    
    this.maxQueueLength = maxQueueLength ?? DEFAULT_MAX_QUEUE_SIZE;
    this.assertedQueues = new Set();
  }
  
  public async init(): Promise<void> {
    await this.channel.waitForConnect();
  }
  
  public async destroy(): Promise<void> {
    await this.channel.close();
    await this.connection.close();
  }
  
  private async assertQueue(queueName: string): Promise<void> {
    if (this.assertedQueues.has(queueName)) {
      return;
    }
    
    await this.channel.addSetup(async (channel: ConfirmChannel): Promise<void> => {
      channel.assertQueue(queueName, {
        durable: true,
      });
    });
    
    this.assertedQueues.add(queueName);
  }
  
  private async publishMessage<T extends object>(
    queueName: string,
    message: T | T[],
  ): Promise<void> {
    const queueLength = this.channel.queueLength();
    if (queueLength >= this.maxQueueLength) {
      throw new Error('in-memory queue is full');
    }
    
    await this.channel.sendToQueue(queueName, message, {
      persistent: true,
    });
  }
  
  private async subscribeToQueue<T extends object>(
    queueName: string,
    callback: MessageHandler<T>,
  ): Promise<void> {
    await this.assertQueue(queueName);
    
    await this.channel.addSetup(async (channel: ConfirmChannel): Promise<void> => {
      channel.prefetch(4);
      channel.consume(queueName, async msg => {
        if (!msg) {
          this.emit('error', new Error('consumer was cancelled'));
          return;
        }
        
        try {
          const message: T | T[] = JSON.parse(msg.content.toString());
          
          await callback(message);
          
          this.channel.ack(msg);
        } catch (err) {
          this.channel.nack(msg);
        }
      });
    });
  }
  
  public async publishEvent(event: Event | Event[]): Promise<void> {
    await this.publishMessage(INBOX_QUEUE_NAME, event);
  }
  
  public async subscribeForEvents(callback: EventHandler): Promise<void> {
    await this.subscribeToQueue(INBOX_QUEUE_NAME, callback);
  }
  
  public async publishNotification(
    channelName: string,
    notification: Notification | Notification[],
  ): Promise<void> {
    await this.publishMessage(getChannelQueueName(channelName), notification);
  }
  
  public async subscribeForNotifications(
    channelName: string,
    callback: NotificationHandler,
  ): Promise<void> {
    await this.subscribeToQueue(getChannelQueueName(channelName), callback);
  }
}

export type SourceBusClientParams = {
  url: string;
  sourceName: string;
};

export class SourceBusClient {
  public bus: BusClient;
  public sourceName: string;
  
  constructor(params: SourceBusClientParams) {
    const {
      url,
      sourceName,
    } = params;
    
    this.bus = new BusClient({ url });
    this.sourceName = sourceName;
  }
  
  public async init(): Promise<void> {
    await this.bus.init();
  }
  
  public async destroy(): Promise<void> {
    await this.bus.destroy();
  }
  
  public async publishEvent(bundle: string, event: Event | Event[]): Promise<void> {
    const events = Array.isArray(event) ? event : [event];
    const fullnameEvents: Event[] = events.map(event => ({
      name: `${this.sourceName}.${bundle}.${event.name}`,
      params: event.params,
      payload: event.payload,
    }));
    
    await this.bus.publishEvent(fullnameEvents);
  }
}
