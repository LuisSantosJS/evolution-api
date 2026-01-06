import { PrismaRepository } from '@api/repository/repository.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { configService, Log, Rabbitmq } from '@config/env.config';
import { Logger } from '@config/logger.config';
import * as amqp from 'amqplib/callback_api.js';

import { EmitData, EventController, EventControllerInterface } from '../event.controller';

export class RabbitmqController extends EventController implements EventControllerInterface {
  public amqpChannel: amqp.Channel | null = null;
  private amqpConnection: amqp.Connection | null = null;
  private readonly logger = new Logger('RabbitmqController');
  private reconnectAttempts = 0;
  private reconnectDelay = 5000; // 5 seconds
  private maxReconnectDelay = 60000; // 60 seconds max delay
  private isReconnecting = false;

  // Event listener callbacks and timeout tracking for cleanup
  private connectionErrorListener: ((err: Error) => void) | null = null;
  private connectionCloseListener: (() => void) | null = null;
  private channelErrorListener: ((err: Error) => void) | null = null;
  private channelCloseListener: (() => void) | null = null;
  private reconnectTimeoutId: NodeJS.Timeout | null = null;

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
    return new Promise<void>((resolve, reject) => {
      const uri = configService.get<Rabbitmq>('RABBITMQ').URI;
      const frameMax = configService.get<Rabbitmq>('RABBITMQ').FRAME_MAX;
      const rabbitmqExchangeName = configService.get<Rabbitmq>('RABBITMQ').EXCHANGE_NAME;

      const url = new URL(uri);
      const connectionOptions = {
        protocol: url.protocol.slice(0, -1),
        hostname: url.hostname,
        port: url.port || 5672,
        username: url.username || 'guest',
        password: url.password || 'guest',
        vhost: url.pathname.slice(1) || '/',
        frameMax: frameMax,
        heartbeat: 30, // Add heartbeat of 30 seconds
      };

      amqp.connect(connectionOptions, (error: Error, connection: amqp.Connection) => {
        if (error) {
          this.logger.error({
            local: 'RabbitmqController.connect',
            message: 'Failed to connect to RabbitMQ',
            error: error.message || error,
          });
          reject(error);
          return;
        }

        // Store callback references for cleanup
        this.connectionErrorListener = (err: Error) => {
          this.logger.error({
            local: 'RabbitmqController.connectionError',
            message: 'RabbitMQ connection error',
            error: err.message || err,
          });
          this.handleConnectionLoss();
        };
        connection.on('error', this.connectionErrorListener);

        this.connectionCloseListener = () => {
          this.logger.warn('RabbitMQ connection closed');
          this.handleConnectionLoss();
        };
        connection.on('close', this.connectionCloseListener);

        connection.createChannel((channelError: Error, channel: amqp.Channel) => {
          if (channelError) {
            this.logger.error({
              local: 'RabbitmqController.createChannel',
              message: 'Failed to create RabbitMQ channel',
              error: channelError.message || channelError,
            });
            reject(channelError);
            return;
          }

          // Store channel callback references for cleanup
          this.channelErrorListener = (err: Error) => {
            this.logger.error({
              local: 'RabbitmqController.channelError',
              message: 'RabbitMQ channel error',
              error: err.message || err,
            });
            this.handleConnectionLoss();
          };
          channel.on('error', this.channelErrorListener);

          this.channelCloseListener = () => {
            this.logger.warn('RabbitMQ channel closed');
            this.handleConnectionLoss();
          };
          channel.on('close', this.channelCloseListener);

          const exchangeName = rabbitmqExchangeName;

          channel.assertExchange(exchangeName, 'topic', {
            durable: true,
            autoDelete: false,
          });

          this.amqpConnection = connection;
          this.amqpChannel = channel;
          this.reconnectAttempts = 0; // Reset reconnect attempts on successful connection
          this.isReconnecting = false;

          this.logger.info('AMQP initialized successfully');

          resolve();
        });
      });
    })
      .then(() => {
        if (configService.get<Rabbitmq>('RABBITMQ')?.GLOBAL_ENABLED) {
          this.initGlobalQueues();
        }
      })
      .catch((error) => {
        this.logger.error({
          local: 'RabbitmqController.init',
          message: 'Failed to initialize AMQP',
          error: error.message || error,
        });
        this.scheduleReconnect();
        throw error;
      });
  }

  private handleConnectionLoss(): void {
    if (this.isReconnecting) {
      return; // Already attempting to reconnect
    }

    this.cleanup();
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.isReconnecting) {
      return; // Already scheduled
    }

    this.isReconnecting = true;
    this.reconnectAttempts++;

    // Exponential backoff with maximum delay cap - no limit on attempts
    const delay = Math.min(
      this.reconnectDelay * Math.pow(2, Math.min(this.reconnectAttempts - 1, 6)),
      this.maxReconnectDelay,
    );

    this.logger.info(`Scheduling RabbitMQ reconnection attempt ${this.reconnectAttempts} in ${delay}ms`);

    // Clear any existing timeout
    if (this.reconnectTimeoutId) {
      clearTimeout(this.reconnectTimeoutId);
    }

    this.reconnectTimeoutId = setTimeout(async () => {
      try {
        this.logger.info(`Attempting to reconnect to RabbitMQ (attempt ${this.reconnectAttempts})`);
        await this.connect();
        this.logger.info('Successfully reconnected to RabbitMQ');
        this.reconnectTimeoutId = null;
      } catch (error) {
        this.logger.error({
          local: 'RabbitmqController.scheduleReconnect',
          message: `Reconnection attempt ${this.reconnectAttempts} failed`,
          error: error.message || error,
        });
        this.isReconnecting = false;
        this.reconnectTimeoutId = null;
        this.scheduleReconnect(); // Continue trying indefinitely
      }
    }, delay);
  }

  private set channel(channel: amqp.Channel) {
    this.amqpChannel = channel;
  }

  public get channel(): amqp.Channel {
    return this.amqpChannel;
  }

  private async ensureConnection(): Promise<boolean> {
    if (!this.amqpChannel) {
      this.logger.warn('AMQP channel is not available, attempting to reconnect...');
      if (!this.isReconnecting) {
        try {
          // Try immediate reconnection first
          await this.connect();
          this.logger.info('Immediate reconnection successful');
          return true;
        } catch (error) {
          this.logger.warn('Immediate reconnection failed, scheduling reconnection...');
          this.scheduleReconnect();
        }
      }
      return false;
    }
    return true;
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
    extra,
  }: EmitData): Promise<void> {
    if (integration && !integration.includes('rabbitmq')) {
      return;
    }

    if (!this.status) {
      return;
    }

    // Store connection status but don't return early - try each section independently
    const hasConnection = await this.ensureConnection();
    if (!hasConnection) {
      this.logger.warn(`No AMQP connection available for event ${event} for instance ${instanceName}`);
    }

    const instanceRabbitmq = await this.get(instanceName);
    const rabbitmqLocal = instanceRabbitmq?.events;
    const rabbitmqGlobal = configService.get<Rabbitmq>('RABBITMQ').GLOBAL_ENABLED;
    const rabbitmqEvents = configService.get<Rabbitmq>('RABBITMQ').EVENTS;
    const prefixKey = configService.get<Rabbitmq>('RABBITMQ').PREFIX_KEY;
    const rabbitmqExchangeName = configService.get<Rabbitmq>('RABBITMQ').EXCHANGE_NAME;
    const we = event.replace(/[.-]/gm, '_').toUpperCase();
    const logEnabled = configService.get<Log>('LOG').LEVEL.includes('WEBHOOKS');

    const message = {
      ...(extra ?? {}),
      event,
      instance: instanceName,
      data,
      server_url: serverUrl,
      date_time: dateTime,
      sender,
      apikey: apiKey,
    };

    // Try local RabbitMQ sending
    if (instanceRabbitmq?.enabled && Array.isArray(rabbitmqLocal) && rabbitmqLocal.includes(we)) {
      // Ensure connection before attempting local send
      if (!this.amqpChannel && !(await this.ensureConnection())) {
        this.logger.warn(`Cannot send local RabbitMQ message for ${instanceName}: No connection`);
      } else {
        const exchangeName = instanceName ?? rabbitmqExchangeName;

        let retry = 0;
        const maxRetries = 10;

        while (retry < maxRetries) {
          try {
            await this.amqpChannel.assertExchange(exchangeName, 'topic', {
              durable: true,
              autoDelete: false,
            });

            const eventName = event.replace(/_/g, '.').toLowerCase();

            const queueName = `${instanceName}.${eventName}`;

            await this.amqpChannel.assertQueue(queueName, {
              durable: true,
              autoDelete: false,
              arguments: {
                'x-queue-type': 'quorum',
              },
            });

            await this.amqpChannel.bindQueue(queueName, exchangeName, eventName);

            await this.amqpChannel.publish(exchangeName, event, Buffer.from(JSON.stringify(message)));

            if (logEnabled) {
              const logData = {
                local: `${origin}.sendData-RabbitMQ`,
                ...message,
              };

              this.logger.log(logData);
            }

            break;
          } catch (error) {
            this.logger.error({
              local: 'RabbitmqController.emit',
              message: `Error publishing local RabbitMQ message (attempt ${retry + 1}/${maxRetries})`,
              error: error.message || error,
            });
            retry++;

            // Add small delay between retries
            if (retry < maxRetries) {
              await new Promise((resolve) => setTimeout(resolve, 1000 * retry));
            } else {
              this.handleConnectionLoss();
            }
          }
        }
      }
    }

    // Try global RabbitMQ sending
    if (rabbitmqGlobal && rabbitmqEvents[we]) {
      // Ensure connection before attempting global send
      if (!this.amqpChannel && !(await this.ensureConnection())) {
        this.logger.warn(`Cannot send global RabbitMQ message for event ${event}: No connection`);
      } else {
        const exchangeName = rabbitmqExchangeName;

        let retry = 0;
        const maxRetries = 10;

        while (retry < maxRetries) {
          try {
            await this.amqpChannel.assertExchange(exchangeName, 'topic', {
              durable: true,
              autoDelete: false,
            });

            const queueName = prefixKey
              ? `${prefixKey}.${event.replace(/_/g, '.').toLowerCase()}`
              : event.replace(/_/g, '.').toLowerCase();

            await this.amqpChannel.assertQueue(queueName, {
              durable: true,
              autoDelete: false,
              arguments: {
                'x-queue-type': 'quorum',
              },
            });

            await this.amqpChannel.bindQueue(queueName, exchangeName, event);

            await this.amqpChannel.publish(exchangeName, event, Buffer.from(JSON.stringify(message)));

            if (logEnabled) {
              const logData = {
                local: `${origin}.sendData-RabbitMQ-Global`,
                ...message,
              };

              this.logger.log(logData);
            }

            break;
          } catch (error) {
            this.logger.error({
              local: 'RabbitmqController.emit',
              message: `Error publishing global RabbitMQ message (attempt ${retry + 1}/${maxRetries})`,
              error: error.message || error,
            });
            retry++;

            // Add small delay between retries
            if (retry < maxRetries) {
              await new Promise((resolve) => setTimeout(resolve, 1000 * retry));
            } else {
              this.handleConnectionLoss();
            }
          }
        }
      }
    }
  }

  private async initGlobalQueues(): Promise<void> {
    this.logger.info('Initializing global queues');

    // Try to ensure connection with multiple attempts
    let connectionAttempts = 0;
    const maxConnectionAttempts = 5;

    while (connectionAttempts < maxConnectionAttempts) {
      if (await this.ensureConnection()) {
        break;
      }
      connectionAttempts++;
      if (connectionAttempts < maxConnectionAttempts) {
        this.logger.warn(
          `Global queues connection attempt ${connectionAttempts}/${maxConnectionAttempts} failed, retrying...`,
        );
        await new Promise((resolve) => setTimeout(resolve, 2000 * connectionAttempts));
      }
    }

    if (connectionAttempts >= maxConnectionAttempts) {
      this.logger.error('Cannot initialize global queues: No AMQP connection after multiple attempts');
      return;
    }

    const rabbitmqExchangeName = configService.get<Rabbitmq>('RABBITMQ').EXCHANGE_NAME;
    const events = configService.get<Rabbitmq>('RABBITMQ').EVENTS;
    const prefixKey = configService.get<Rabbitmq>('RABBITMQ').PREFIX_KEY;

    if (!events) {
      this.logger.warn('No events to initialize on AMQP');
      return;
    }

    const eventKeys = Object.keys(events);

    for (const event of eventKeys) {
      if (events[event] === false) continue;

      try {
        const queueName =
          prefixKey !== ''
            ? `${prefixKey}.${event.replace(/_/g, '.').toLowerCase()}`
            : `${event.replace(/_/g, '.').toLowerCase()}`;
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

        this.logger.info(`Global queue initialized: ${queueName}`);
      } catch (error) {
        this.logger.error({
          local: 'RabbitmqController.initGlobalQueues',
          message: `Failed to initialize global queue for event ${event}`,
          error: error.message || error,
        });
        this.handleConnectionLoss();
        break;
      }
    }
  }

  public async cleanup(): Promise<void> {
    try {
      // Clear reconnection timeout
      if (this.reconnectTimeoutId) {
        clearTimeout(this.reconnectTimeoutId);
        this.reconnectTimeoutId = null;
      }

      // Remove event listeners from channel
      if (this.amqpChannel) {
        if (this.channelErrorListener) {
          this.amqpChannel.removeListener('error', this.channelErrorListener);
          this.channelErrorListener = null;
        }
        if (this.channelCloseListener) {
          this.amqpChannel.removeListener('close', this.channelCloseListener);
          this.channelCloseListener = null;
        }
        await this.amqpChannel.close();
        this.amqpChannel = null;
      }

      // Remove event listeners from connection
      if (this.amqpConnection) {
        if (this.connectionErrorListener) {
          this.amqpConnection.removeListener('error', this.connectionErrorListener);
          this.connectionErrorListener = null;
        }
        if (this.connectionCloseListener) {
          this.amqpConnection.removeListener('close', this.connectionCloseListener);
          this.connectionCloseListener = null;
        }
        await this.amqpConnection.close();
        this.amqpConnection = null;
      }
    } catch (error) {
      this.logger.warn({
        local: 'RabbitmqController.cleanup',
        message: 'Error during cleanup',
        error: error.message || error,
      });
      // Ensure cleanup even if error occurs
      this.amqpChannel = null;
      this.amqpConnection = null;
      this.channelErrorListener = null;
      this.channelCloseListener = null;
      this.connectionErrorListener = null;
      this.connectionCloseListener = null;
    }
  }
}
