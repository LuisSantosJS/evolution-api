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
  private reconnectAttempts = 0;
  private reconnectDelay = 5000; // 5 seconds
  private maxReconnectDelay = 60000; // 60 seconds max delay
  private isReconnecting = false;
  private healthCheckInterval: NodeJS.Timeout | null = null;
  
  // Métricas de monitoramento
  private publishMetrics = {
    success: 0,
    failed: 0,
    reconnections: 0,
    lastError: null as string | null,
    lastSuccess: null as Date | null,
  };

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
        heartbeat: 60, // Aumentar heartbeat para 60 segundos para melhor estabilidade
        connectionTimeout: 30000, // 30 segundos timeout de conexão
        handshakeTimeout: 10000, // 10 segundos timeout de handshake
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

        // Connection event handlers
        connection.on('error', (err: Error) => {
          this.logger.error({
            local: 'RabbitmqController.connectionError',
            message: 'RabbitMQ connection error',
            error: err.message || err,
          });
          this.handleConnectionLoss();
        });

        connection.on('close', () => {
          this.logger.warn('RabbitMQ connection closed');
          this.handleConnectionLoss();
        });

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

          // Channel event handlers
          channel.on('error', (err: Error) => {
            this.logger.error({
              local: 'RabbitmqController.channelError',
              message: 'RabbitMQ channel error',
              error: err.message || err,
            });
            this.handleConnectionLoss();
          });

          channel.on('close', () => {
            this.logger.warn('RabbitMQ channel closed');
            this.handleConnectionLoss();
          });

          const exchangeName = rabbitmqExchangeName;

          channel.assertExchange(exchangeName, 'topic', {
            durable: true,
            autoDelete: false,
          });

          this.amqpConnection = connection;
          this.amqpChannel = channel;
          this.reconnectAttempts = 0; // Reset reconnect attempts on successful connection
          this.isReconnecting = false;

          // Habilitar confirmações de publicação
          channel.confirmSelect();

          // Iniciar monitoramento de saúde da conexão
          this.startHealthCheck();

          this.logger.info('AMQP initialized successfully with confirmations enabled');

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

    // Parar monitoramento de saúde
    this.stopHealthCheck();
    
    // Incrementar métrica de reconexões
    this.publishMetrics.reconnections++;

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

    setTimeout(async () => {
      try {
        this.logger.info(`Attempting to reconnect to RabbitMQ (attempt ${this.reconnectAttempts})`);
        await this.connect();
        this.logger.info('Successfully reconnected to RabbitMQ');
      } catch (error) {
        this.logger.error({
          local: 'RabbitmqController.scheduleReconnect',
          message: `Reconnection attempt ${this.reconnectAttempts} failed`,
          error: error.message || error,
        });
        this.isReconnecting = false;
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

  // Método para iniciar monitoramento de saúde da conexão
  private startHealthCheck(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }

    this.healthCheckInterval = setInterval(async () => {
      if (this.amqpChannel) {
        try {
          // Verificar se a conexão está funcionando
          await this.amqpChannel.checkExchange('amq.default');
        } catch (error) {
          this.logger.warn({
            local: 'RabbitmqController.healthCheck',
            message: 'Connection health check failed, triggering reconnection',
            error: error.message || error,
          });
          this.handleConnectionLoss();
        }
      }
    }, 30000); // Check a cada 30 segundos
  }

  // Método para parar monitoramento de saúde
  private stopHealthCheck(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }
  }

  // Método para publicar com confirmação
  private async publishWithConfirmation(
    exchangeName: string,
    routingKey: string,
    message: Buffer,
    options?: amqp.Options.Publish
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (!this.amqpChannel) {
        reject(new Error('No channel available'));
        return;
      }

      const timeout = setTimeout(() => {
        reject(new Error('Publish confirmation timeout'));
      }, 5000);

      try {
        const confirmed = this.amqpChannel.publish(
          exchangeName,
          routingKey,
          message,
          { persistent: true, ...options },
          (error) => {
            clearTimeout(timeout);
            if (error) {
              reject(error);
            } else {
              resolve();
            }
          }
        );

        if (!confirmed) {
          clearTimeout(timeout);
          reject(new Error('Channel buffer full'));
        }
      } catch (error) {
        clearTimeout(timeout);
        reject(error);
      }
    });
  }

  // Método para log de métricas de publicação
  private logPublishMetrics(event: string, success: boolean, error?: Error): void {
    if (success) {
      this.publishMetrics.success++;
      this.publishMetrics.lastSuccess = new Date();
    } else {
      this.publishMetrics.failed++;
      this.publishMetrics.lastError = error?.message || 'Unknown error';
    }

    this.logger.debug({
      local: 'RabbitmqController.publishMetrics',
      message: 'Publish attempt result',
      event,
      success,
      metrics: this.publishMetrics,
      connectionStatus: !!this.amqpChannel
    });
  }

  // Método melhorado para publicação com retry robusto
  private async publishWithRetry(
    exchangeName: string,
    routingKey: string,
    message: Buffer,
    maxRetries = 15,
    eventName = 'unknown'
  ): Promise<void> {
    let retry = 0;
    
    while (retry < maxRetries) {
      try {
        // Verificar conexão antes de cada tentativa
        if (!this.amqpChannel) {
          await this.ensureConnection();
          if (!this.amqpChannel) {
            throw new Error('No connection available');
          }
        }

        // Tentar publicar com confirmação
        await this.publishWithConfirmation(exchangeName, routingKey, message);
        
        // Sucesso - log e retornar
        this.logPublishMetrics(eventName, true);
        return;
        
      } catch (error) {
        retry++;
        this.logPublishMetrics(eventName, false, error as Error);
        
        this.logger.error({
          local: 'RabbitmqController.publishWithRetry',
          message: `Publish attempt ${retry}/${maxRetries} failed for event ${eventName}`,
          error: error.message || error,
        });

        if (retry >= maxRetries) {
          throw error;
        }

        // Delay exponencial com jitter
        const delay = Math.min(1000 * Math.pow(2, retry - 1), 30000);
        const jitter = Math.random() * 1000;
        await new Promise(resolve => setTimeout(resolve, delay + jitter));

        // Forçar reconexão em caso de erro de conexão
        if (error.message.includes('connection') || error.message.includes('channel')) {
          this.handleConnectionLoss();
        }
      }
    }
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

    // Log para debug - ajuda a identificar quando configuração não é encontrada
    this.logger.debug({
      local: 'RabbitmqController.emit',
      message: 'Instance RabbitMQ configuration check',
      instanceName,
      hasInstanceConfig: !!instanceRabbitmq,
      isEnabled: instanceRabbitmq?.enabled,
      events: instanceRabbitmq?.events,
      eventToSend: we,
      shouldSendLocal: instanceRabbitmq?.enabled && Array.isArray(rabbitmqLocal) && rabbitmqLocal.includes(we)
    });
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

            // Usar método melhorado com retry e confirmação
            await this.publishWithRetry(
              exchangeName,
              event,
              Buffer.from(JSON.stringify(message)),
              15,
              `${origin}.sendData-RabbitMQ`
            );

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

            // Usar método melhorado com retry e confirmação
            await this.publishWithRetry(
              exchangeName,
              event,
              Buffer.from(JSON.stringify(message)),
              15,
              `${origin}.sendData-RabbitMQ-Global`
            );

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
      // Parar monitoramento de saúde
      this.stopHealthCheck();

      if (this.amqpChannel) {
        await this.amqpChannel.close();
        this.amqpChannel = null;
      }
      if (this.amqpConnection) {
        await this.amqpConnection.close();
        this.amqpConnection = null;
      }
    } catch (error) {
      this.logger.warn({
        local: 'RabbitmqController.cleanup',
        message: 'Error during cleanup',
        error: error.message || error,
      });
      this.amqpChannel = null;
      this.amqpConnection = null;
    }
  }
}
