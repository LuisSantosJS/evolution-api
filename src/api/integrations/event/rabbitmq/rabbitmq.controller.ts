import { PrismaRepository } from '@api/repository/repository.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { configService, Log, Rabbitmq } from '@config/env.config';
import { Logger } from '@config/logger.config';
import * as amqp from 'amqplib/callback_api';

import { EmitData, EventController, EventControllerInterface } from '../event.controller';

interface ConnectionState {
  isConnected: boolean;
  isConnecting: boolean;
  consecutiveFailures: number;
}

interface QueueCache {
  [queueName: string]: boolean;
}

interface CircuitBreakerState {
  isOpen: boolean;
  failures: number;
  lastFailureTime: number;
  nextAttemptTime: number;
}

export class RabbitmqController extends EventController implements EventControllerInterface {
  public amqpChannel: amqp.Channel | null = null;
  private amqpConnection: amqp.Connection | null = null;
  private readonly logger = new Logger('RabbitmqController');

  // Mantém compatibilidade com variáveis originais
  private isConnecting: boolean = false;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private readonly RECONNECT_INTERVAL = 5000; // Mantém valor original
  private readonly MAX_RECONNECT_ATTEMPTS = 10; // Mantém valor original
  private reconnectAttempts = 0; // Mantém variável original

  // Novos recursos opcionais (não interferem com comportamento atual)
  private connectionState: ConnectionState = {
    isConnected: false,
    isConnecting: false,
    consecutiveFailures: 0,
  };

  private circuitBreaker: CircuitBreakerState = {
    isOpen: false,
    failures: 0,
    lastFailureTime: 0,
    nextAttemptTime: 0,
  };
  private readonly CIRCUIT_BREAKER_THRESHOLD = 5;
  private readonly CIRCUIT_BREAKER_TIMEOUT = 60000;

  private queueCache: QueueCache = {};
  private exchangeCache: Set<string> = new Set();

  private healthCheckInterval: NodeJS.Timeout | null = null;
  private readonly HEALTH_CHECK_INTERVAL = 30000;
  private isShuttingDown: boolean = false;

  constructor(prismaRepository: PrismaRepository, waMonitor: WAMonitoringService) {
    super(prismaRepository, waMonitor, configService.get<Rabbitmq>('RABBITMQ')?.ENABLED, 'rabbitmq');
  }

  public async init(): Promise<void> {
    if (!this.status) {
      return;
    }

    await this.connect();

    // Inicia health check apenas se conectou com sucesso
    if (this.amqpChannel) {
      this.startHealthCheck();
    }
  }

  private async connect(): Promise<void> {
    if (this.isConnecting) {
      return;
    }

    // Verifica circuit breaker (novo recurso não invasivo)
    if (this.isCircuitBreakerOpen()) {
      this.logger.debug('Circuit breaker is open, skipping connection attempt');
      return;
    }

    this.isConnecting = true;
    this.connectionState.isConnecting = true;

    try {
      await this.establishConnection();
      // Reset contadores em caso de sucesso
      this.reconnectAttempts = 0;
      this.connectionState.consecutiveFailures = 0;
      this.connectionState.isConnected = true;
      this.resetCircuitBreaker();
    } catch (error) {
      this.logger.error(`Failed to connect to RabbitMQ: ${error.message}`);
      this.connectionState.consecutiveFailures++;
      this.updateCircuitBreaker();
      this.scheduleReconnect();
    } finally {
      this.isConnecting = false;
      this.connectionState.isConnecting = false;
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

  private createChannel(connection: amqp.Connection, resolve: () => void, reject: (error: Error) => void): void {
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
    this.connectionState.isConnected = false;
    this.clearCaches(); // Limpa caches para força recriar queues
    this.scheduleReconnect();
  }

  private handleChannelFailure(): void {
    this.amqpChannel = null;
    this.clearCaches(); // Limpa caches para força recriar queues
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

    // Mantém lógica original + melhoria com jitter
    const baseDelay = this.RECONNECT_INTERVAL * Math.min(this.reconnectAttempts, 5);
    const jitter = Math.random() * 1000; // Adiciona até 1s de jitter
    const delay = baseDelay + jitter;

    this.logger.info(`Scheduling reconnection attempt ${this.reconnectAttempts} in ${Math.round(delay)}ms`);

    this.reconnectTimeout = setTimeout(() => {
      this.connect().catch((error) => {
        this.logger.error(`Reconnection attempt failed: ${error.message}`);
      });
    }, delay);
  }

  // Novos métodos para circuit breaker (não invasivos)
  private isCircuitBreakerOpen(): boolean {
    if (!this.circuitBreaker.isOpen) {
      return false;
    }

    const now = Date.now();
    if (now >= this.circuitBreaker.nextAttemptTime) {
      return false;
    }

    return true;
  }

  private updateCircuitBreaker(): void {
    this.circuitBreaker.failures++;
    this.circuitBreaker.lastFailureTime = Date.now();

    if (this.circuitBreaker.failures >= this.CIRCUIT_BREAKER_THRESHOLD) {
      this.circuitBreaker.isOpen = true;
      this.circuitBreaker.nextAttemptTime = Date.now() + this.CIRCUIT_BREAKER_TIMEOUT;
      this.logger.warn(`Circuit breaker opened after ${this.circuitBreaker.failures} failures`);
    }
  }

  private resetCircuitBreaker(): void {
    this.circuitBreaker = {
      isOpen: false,
      failures: 0,
      lastFailureTime: 0,
      nextAttemptTime: 0,
    };
  }

  private clearCaches(): void {
    this.queueCache = {};
    this.exchangeCache.clear();
  }

  private startHealthCheck(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }

    this.healthCheckInterval = setInterval(() => {
      this.performHealthCheck();
    }, this.HEALTH_CHECK_INTERVAL);
  }

  private performHealthCheck(): void {
    if (!this.connectionState.isConnected || !this.amqpChannel) {
      if (!this.isConnecting) {
        this.connect().catch((error) => {
          this.logger.debug(`Health check reconnection failed: ${error.message}`);
        });
      }
    }
  }

  // Método auxiliar para garantir que queue existe (novo recurso)
  private async ensureQueueExists(queueName: string, exchangeName: string, routingKey: string): Promise<void> {
    if (!this.amqpChannel) {
      return; // Se não tem channel, vai falhar na publicação mesmo
    }

    const cacheKey = `${queueName}:${exchangeName}:${routingKey}`;
    if (this.queueCache[cacheKey]) {
      return; // Já verificado anteriormente
    }

    try {
      // Garante que exchange existe
      if (!this.exchangeCache.has(exchangeName)) {
        await new Promise<void>((resolve, reject) => {
          this.amqpChannel!.assertExchange(
            exchangeName,
            'topic',
            {
              durable: true,
              autoDelete: false,
            },
            (error) => {
              if (error) reject(error);
              else resolve();
            },
          );
        });
        this.exchangeCache.add(exchangeName);
      }

      // Cria queue se não existir (usa configuração compatível)
      await new Promise<void>((resolve, reject) => {
        this.amqpChannel!.assertQueue(
          queueName,
          {
            durable: true,
            autoDelete: false,
            arguments: {
              'x-queue-type': 'quorum', // Mantém quorum que é mais robusto
            },
          },
          (error) => {
            if (error) reject(error);
            else resolve();
          },
        );
      });

      // Bind queue ao exchange
      await new Promise<void>((resolve, reject) => {
        this.amqpChannel!.bindQueue(queueName, exchangeName, routingKey, {}, (error) => {
          if (error) reject(error);
          else resolve();
        });
      });

      this.queueCache[cacheKey] = true;
      this.logger.debug(`Queue ${queueName} ensured and bound`);
    } catch (error) {
      this.logger.warn(`Failed to ensure queue ${queueName}: ${error.message}`);
      // Não lança erro para manter compatibilidade - vai tentar publicar mesmo assim
    }
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

    // Melhoria: tenta reconectar se não tem conexão (novo comportamento robusto)
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

    // Lógica original mantida exatamente igual
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
    isGlobal: boolean = false,
  ): Promise<void> {
    let retry = 0;
    const maxRetries = 3;

    while (retry < maxRetries) {
      try {
        // Garante que exchange existe (mantém comportamento original)
        await this.amqpChannel.assertExchange(exchangeName, 'topic', {
          durable: true,
          autoDelete: false,
        });

        const eventName = event.replace(/_/g, '.').toLowerCase();
        const queueName = isGlobal ? event : `${exchangeName}.${eventName}`;
        const routingKey = isGlobal ? event : eventName;

        // NOVA FUNCIONALIDADE: Garante que queue existe antes de publicar
        await this.ensureQueueExists(queueName, exchangeName, routingKey);

        // Mantém lógica original de publicação
        await this.amqpChannel.assertQueue(queueName, {
          durable: true,
          autoDelete: false,
          arguments: {
            'x-queue-type': 'quorum',
          },
        });

        await this.amqpChannel.bindQueue(queueName, exchangeName, routingKey);

        // Publicação com persistência (melhoria sutil)
        await this.amqpChannel.publish(
          exchangeName,
          routingKey,
          Buffer.from(JSON.stringify(message)),
          { persistent: true }, // Garante que mensagem não é perdida
        );

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

        // Backoff exponencial suave em caso de erro
        await new Promise((resolve) => setTimeout(resolve, 1000 * retry));

        // Tenta reconectar se o erro parece ser de conexão
        if (error.message.includes('Channel closed') || error.message.includes('Connection closed')) {
          await this.connect();
        }
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

  // Novos métodos para monitoramento (opcionais)
  public isConnected(): boolean {
    return this.connectionState.isConnected && !!this.amqpChannel;
  }

  public getConnectionStats() {
    return {
      isConnected: this.connectionState.isConnected,
      isConnecting: this.connectionState.isConnecting,
      consecutiveFailures: this.connectionState.consecutiveFailures,
      reconnectAttempts: this.reconnectAttempts,
      circuitBreakerOpen: this.circuitBreaker.isOpen,
      queuesCached: Object.keys(this.queueCache).length,
    };
  }

  // Método para graceful shutdown (opcional - não interfere se não usado)
  public async shutdown(): Promise<void> {
    this.isShuttingDown = true;

    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
    }
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }

    try {
      if (this.amqpChannel) {
        await new Promise<void>((resolve) => {
          this.amqpChannel!.close(() => resolve());
        });
      }
      if (this.amqpConnection) {
        await new Promise<void>((resolve) => {
          this.amqpConnection!.close(() => resolve());
        });
      }
    } catch (error) {
      this.logger.warn(`Error during shutdown: ${error.message}`);
    }

    this.amqpChannel = null;
    this.amqpConnection = null;
    this.connectionState.isConnected = false;
  }
}
