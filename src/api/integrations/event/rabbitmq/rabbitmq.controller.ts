import { PrismaRepository } from '@api/repository/repository.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { configService, Log, Rabbitmq } from '@config/env.config';
import { Logger } from '@config/logger.config';
import * as amqp from 'amqplib/callback_api';

import { EmitData, EventController, EventControllerInterface } from '../event.controller';

export class RabbitmqController extends EventController implements EventControllerInterface {
  public amqpChannel: amqp.Channel | null = null;
  private amqpConnection: amqp.Connection | null = null;
  private readonly logger = new Logger('RabbitmqController');
  private isConnecting: boolean = false;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private readonly RECONNECT_INTERVAL = 5000; // 5 seconds
  private readonly MAX_RECONNECT_ATTEMPTS = 10;
  private reconnectAttempts = 0;

  constructor(prismaRepository: PrismaRepository, waMonitor: WAMonitoringService) {
    super(prismaRepository, waMonitor, configService.get<Rabbitmq>('RABBITMQ')?.ENABLED, 'rabbitmq');
  }

  public async init(): Promise<void> {
    if (!this.status) {
      return;
    }

    await this.connect();
  }

  private async connect(): Promise<void> {
    if (this.isConnecting) {
      return;
    }

    this.isConnecting = true;

    try {
      await this.establishConnection();
    } catch (error) {
      this.logger.error(`Failed to connect to RabbitMQ: ${error.message}`);
      this.scheduleReconnect();
    } finally {
      this.isConnecting = false;
    }
  }

  private async establishConnection(): Promise<void> {
    const uri = configService.get<Rabbitmq>('RABBITMQ').URI;

    return new Promise<void>((resolve, reject) => {
      amqp.connect(uri, (error, connection) => {
        if (error) {
          reject(error);
          return;
        }

        this.setupConnectionListeners(connection);
        this.createChannel(connection, resolve, reject);
      });
    }).then(() => {
      if (configService.get<Rabbitmq>('RABBITMQ')?.GLOBAL_ENABLED) {
        this.initGlobalQueues();
      }
      this.reconnectAttempts = 0;
    });
  }

  private setupConnectionListeners(connection: amqp.Connection): void {
    this.amqpConnection = connection;

    connection.on('error', (error) => {
      this.logger.error(`RabbitMQ connection error: ${error.message}`);
      this.handleConnectionFailure();
    });

    connection.on('close', () => {
      this.logger.warn('RabbitMQ connection closed');
      this.handleConnectionFailure();
    });
  }

  private createChannel(
    connection: amqp.Connection,
    resolve: () => void,
    reject: (error: Error) => void
  ): void {
    const rabbitmqExchangeName = configService.get<Rabbitmq>('RABBITMQ').EXCHANGE_NAME;

    connection.createChannel((channelError, channel) => {
      if (channelError) {
        reject(channelError);
        return;
      }

      this.setupChannelListeners(channel);

      channel.assertExchange(rabbitmqExchangeName, 'topic', {
        durable: true,
        autoDelete: false,
      });

      this.amqpChannel = channel;
      this.logger.info('AMQP initialized successfully');
      resolve();
    });
  }

  private setupChannelListeners(channel: amqp.Channel): void {
    channel.on('error', (error) => {
      this.logger.error(`RabbitMQ channel error: ${error.message}`);
      this.handleChannelFailure();
    });

    channel.on('close', () => {
      this.logger.warn('RabbitMQ channel closed');
      this.handleChannelFailure();
    });
  }

  private handleConnectionFailure(): void {
    this.amqpChannel = null;
    this.amqpConnection = null;
    this.scheduleReconnect();
  }

  private handleChannelFailure(): void {
    this.amqpChannel = null;
    if (this.amqpConnection && this.amqpConnection.connection) {
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
    }

    this.reconnectAttempts++;
    if (this.reconnectAttempts > this.MAX_RECONNECT_ATTEMPTS) {
      this.logger.error('Max reconnection attempts reached. Manual intervention required.');
      return;
    }

    const delay = this.RECONNECT_INTERVAL * Math.min(this.reconnectAttempts, 5);
    this.logger.info(`Scheduling reconnection attempt ${this.reconnectAttempts} in ${delay}ms`);

    this.reconnectTimeout = setTimeout(() => {
      this.connect().catch((error) => {
        this.logger.error(`Reconnection attempt failed: ${error.message}`);
      });
    }, delay);
  }

  private set channel(channel: amqp.Channel) {
    this.amqpChannel = channel;
  }

  public get channel(): amqp.Channel {
    return this.amqpChannel;
  }

  public async emit({
    instanceName,
    origin,
    event,
    data,
    serverUrl,
    dateTime,
    sender,
    apiKey,
    integration,
  }: EmitData): Promise<void> {
    if (integration && !integration.includes('rabbitmq')) {
      return;
    }

    if (!this.status) {
      return;
    }

    // Ensure we have a connection before proceeding
    if (!this.amqpChannel) {
      this.logger.warn('No RabbitMQ channel available. Attempting to reconnect...');
      await this.connect();
      if (!this.amqpChannel) {
        throw new Error('Failed to establish RabbitMQ connection');
      }
    }

    const instanceRabbitmq = await this.get(instanceName);
    const rabbitmqLocal = instanceRabbitmq?.events;
    const rabbitmqGlobal = configService.get<Rabbitmq>('RABBITMQ').GLOBAL_ENABLED;
    const rabbitmqEvents = configService.get<Rabbitmq>('RABBITMQ').EVENTS;
    const rabbitmqExchangeName = configService.get<Rabbitmq>('RABBITMQ').EXCHANGE_NAME;
    const we = event.replace(/[.-]/gm, '_').toUpperCase();
    const logEnabled = configService.get<Log>('LOG').LEVEL.includes('WEBHOOKS');

    const message = {
      event,
      instance: instanceName,
      data,
      server_url: serverUrl,
      date_time: dateTime,
      sender,
      apikey: apiKey,
    };

    if (instanceRabbitmq?.enabled && this.amqpChannel) {
      if (Array.isArray(rabbitmqLocal) && rabbitmqLocal.includes(we)) {
        await this.publishWithRetry(instanceName ?? rabbitmqExchangeName, event, message, origin, logEnabled);
      }
    }

    if (rabbitmqGlobal && rabbitmqEvents[we] && this.amqpChannel) {
      await this.publishWithRetry(rabbitmqExchangeName, event, message, origin, logEnabled, true);
    }
  }

  private async publishWithRetry(
    exchangeName: string,
    event: string,
    message: any,
    origin: string,
    logEnabled: boolean,
    isGlobal: boolean = false
  ): Promise<void> {
    let retry = 0;
    const maxRetries = 3;

    while (retry < maxRetries) {
      try {
        await this.amqpChannel.assertExchange(exchangeName, 'topic', {
          durable: true,
          autoDelete: false,
        });

        const eventName = event.replace(/_/g, '.').toLowerCase();
        const queueName = isGlobal ? event : `${exchangeName}.${eventName}`;

        await this.amqpChannel.assertQueue(queueName, {
          durable: true,
          autoDelete: false,
          arguments: {
            'x-queue-type': 'quorum',
          },
        });

        await this.amqpChannel.bindQueue(queueName, exchangeName, isGlobal ? event : eventName);
        await this.amqpChannel.publish(exchangeName, isGlobal ? event : eventName, Buffer.from(JSON.stringify(message)));

        if (logEnabled) {
          const logData = {
            local: `${origin}.sendData-RabbitMQ${isGlobal ? '-Global' : ''}`,
            ...message,
          };
          this.logger.log(logData);
        }
        break;
      } catch (error) {
        retry++;
        if (retry === maxRetries) {
          this.logger.error(`Failed to publish message after ${maxRetries} attempts: ${error.message}`);
          throw error;
        }
        await new Promise(resolve => setTimeout(resolve, 1000 * retry));
      }
    }
  }

  private async initGlobalQueues(): Promise<void> {
    if (!this.amqpChannel) {
      this.logger.warn('No channel available for initializing global queues');
      return;
    }

    this.logger.info('Initializing global queues');

    const rabbitmqExchangeName = configService.get<Rabbitmq>('RABBITMQ').EXCHANGE_NAME;
    const events = configService.get<Rabbitmq>('RABBITMQ').EVENTS;

    if (!events) {
      this.logger.warn('No events to initialize on AMQP');
      return;
    }

    const eventKeys = Object.keys(events);

    for (const event of eventKeys) {
      if (events[event] === false) continue;

      try {
        const queueName = `${event.replace(/_/g, '.').toLowerCase()}`;
        const exchangeName = rabbitmqExchangeName;

        await this.amqpChannel.assertExchange(exchangeName, 'topic', {
          durable: true,
          autoDelete: false,
        });

        await this.amqpChannel.assertQueue(queueName, {
          durable: true,
          autoDelete: false,
          arguments: {
            'x-queue-type': 'quorum',
          },
        });

        await this.amqpChannel.bindQueue(queueName, exchangeName, event);
      } catch (error) {
        this.logger.error(`Failed to initialize queue for event ${event}: ${error.message}`);
      }
    }
  }
}