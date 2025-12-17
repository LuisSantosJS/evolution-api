import { InstanceDto, SetPresenceDto } from '@api/dto/instance.dto';
import { ChatwootService } from '@api/integrations/chatbot/chatwoot/services/chatwoot.service';
import { ProviderFiles } from '@api/provider/sessions';
import { PrismaRepository } from '@api/repository/repository.service';
import { channelController, eventManager } from '@api/server.module';
import { CacheService } from '@api/services/cache.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { SettingsService } from '@api/services/settings.service';
import { Events, Integration, wa } from '@api/types/wa.types';
import { Auth, Chatwoot, ConfigService, HttpServer, WaBusiness } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { BadRequestException, InternalServerErrorException, UnauthorizedException } from '@exceptions';
import { delay } from '@whiskeysockets/baileys';
import { isArray, isURL } from 'class-validator';
import EventEmitter2 from 'eventemitter2';
import { v4 } from 'uuid';

import { ProxyController } from './proxy.controller';

export class InstanceController {
  constructor(
    private readonly waMonitor: WAMonitoringService,
    private readonly configService: ConfigService,
    private readonly prismaRepository: PrismaRepository,
    private readonly eventEmitter: EventEmitter2,
    private readonly chatwootService: ChatwootService,
    private readonly settingsService: SettingsService,
    private readonly proxyService: ProxyController,
    private readonly cache: CacheService,
    private readonly chatwootCache: CacheService,
    private readonly baileysCache: CacheService,
    private readonly providerFiles: ProviderFiles,
  ) {}

  private readonly logger = new Logger('InstanceController');

  public async createInstance(instanceData: InstanceDto) {
    try {
      // Test proxy BEFORE creating instance
      if (instanceData.proxyHost && instanceData.proxyPort && instanceData.proxyProtocol) {
        this.logger.verbose('Testing proxy configuration before creating instance...');
        const testProxy = await this.proxyService.testProxy({
          host: instanceData.proxyHost,
          port: instanceData.proxyPort,
          protocol: instanceData.proxyProtocol,
          username: instanceData.proxyUsername,
          password: instanceData.proxyPassword,
        });
        if (!testProxy) {
          throw new BadRequestException('Invalid proxy configuration. Please verify your proxy settings.');
        }
        this.logger.verbose('Proxy configuration validated successfully');
      }

      const instance = channelController.init(instanceData, {
        configService: this.configService,
        eventEmitter: this.eventEmitter,
        prismaRepository: this.prismaRepository,
        cache: this.cache,
        chatwootCache: this.chatwootCache,
        baileysCache: this.baileysCache,
        providerFiles: this.providerFiles,
      });

      if (!instance) {
        throw new BadRequestException('Invalid integration');
      }

      const instanceId = v4();

      instanceData.instanceId = instanceId;

      let hash: string;

      if (!instanceData.token) hash = v4().toUpperCase();
      else hash = instanceData.token;

      await this.waMonitor.saveInstance({
        instanceId,
        integration: instanceData.integration,
        instanceName: instanceData.instanceName,
        ownerJid: instanceData.ownerJid,
        profileName: instanceData.profileName,
        profilePicUrl: instanceData.profilePicUrl,
        hash,
        number: instanceData.number,
        businessId: instanceData.businessId,
        status: instanceData.status,
      });

      instance.setInstance({
        instanceName: instanceData.instanceName,
        instanceId,
        integration: instanceData.integration,
        token: hash,
        number: instanceData.number,
        businessId: instanceData.businessId,
      });

      this.waMonitor.waInstances[instance.instanceName] = instance;
      this.waMonitor.delInstanceTime(instance.instanceName);

      // set events
      await eventManager.setInstance(instance.instanceName, instanceData);

      instance.sendDataWebhook(Events.INSTANCE_CREATE, {
        instanceName: instanceData.instanceName,
        instanceId: instanceId,
      });

      // Configure proxy (already validated above)
      if (instanceData.proxyHost && instanceData.proxyPort && instanceData.proxyProtocol) {
        await this.proxyService.createProxy(instance, {
          enabled: true,
          host: instanceData.proxyHost,
          port: instanceData.proxyPort,
          protocol: instanceData.proxyProtocol,
          username: instanceData.proxyUsername,
          password: instanceData.proxyPassword,
        });
      }

      // Default settings to ensure correct behavior for notifications and presence
      // alwaysOnline: false - Allows status to change to offline, enabling phone notifications
      // readMessages: false - Doesn't auto-read messages
      // readStatus: false - Doesn't auto-read status/stories
      const settings: wa.LocalSettings = {
        rejectCall: instanceData.rejectCall === true,
        msgCall: instanceData.msgCall || '',
        groupsIgnore: instanceData.groupsIgnore === true,
        alwaysOnline: instanceData.alwaysOnline === true, // Default: false
        readMessages: instanceData.readMessages === true, // Default: false
        readStatus: instanceData.readStatus === true, // Default: false
        syncFullHistory: instanceData.syncFullHistory === true,
        wavoipToken: instanceData.wavoipToken || '',
      };

      await this.settingsService.create(instance, settings);

      let webhookWaBusiness = null,
        accessTokenWaBusiness = '';

      if (instanceData.integration === Integration.WHATSAPP_BUSINESS) {
        if (!instanceData.number) {
          throw new BadRequestException('number is required');
        }
        const urlServer = this.configService.get<HttpServer>('SERVER').URL;
        webhookWaBusiness = `${urlServer}/webhook/meta`;
        accessTokenWaBusiness = this.configService.get<WaBusiness>('WA_BUSINESS').TOKEN_WEBHOOK;
      }

      if (!instanceData.chatwootAccountId || !instanceData.chatwootToken || !instanceData.chatwootUrl) {
        let getQrcode: wa.QrCode;

        if (instanceData.qrcode && instanceData.integration === Integration.WHATSAPP_BAILEYS) {
          await instance.connectToWhatsapp(instanceData.number);

          // Polling com retry para aguardar geração do QR code (5 segundos)
          const maxAttempts = 10; // 5 segundos total (500ms * 10)
          let attempts = 0;

          while (attempts < maxAttempts) {
            getQrcode = instance.qrCode;
            if (getQrcode?.base64) {
              // QR code gerado com sucesso
              break;
            }
            await delay(500);
            attempts++;
          }

          // Se não conseguiu gerar, retorna o que tiver disponível
          getQrcode = instance.qrCode;
        }

        const result = {
          instance: {
            instanceName: instance.instanceName,
            instanceId: instanceId,
            integration: instanceData.integration,
            webhookWaBusiness,
            accessTokenWaBusiness,
            status: instance.connectionStatus.state,
          },
          hash,
          webhook: {
            webhookUrl: instanceData?.webhook?.url,
            webhookHeaders: instanceData?.webhook?.headers,
            webhookByEvents: instanceData?.webhook?.byEvents,
            webhookBase64: instanceData?.webhook?.base64,
          },
          websocket: {
            enabled: instanceData?.websocket?.enabled,
          },
          rabbitmq: {
            enabled: instanceData?.rabbitmq?.enabled,
          },
          nats: {
            enabled: instanceData?.nats?.enabled,
          },
          sqs: {
            enabled: instanceData?.sqs?.enabled,
          },
          settings,
          qrcode: getQrcode,
        };

        return result;
      }

      if (!this.configService.get<Chatwoot>('CHATWOOT').ENABLED)
        throw new BadRequestException('Chatwoot is not enabled');

      if (!instanceData.chatwootAccountId) {
        throw new BadRequestException('accountId is required');
      }

      if (!instanceData.chatwootToken) {
        throw new BadRequestException('token is required');
      }

      if (!instanceData.chatwootUrl) {
        throw new BadRequestException('url is required');
      }

      if (!isURL(instanceData.chatwootUrl, { require_tld: false })) {
        throw new BadRequestException('Invalid "url" property in chatwoot');
      }

      if (instanceData.chatwootSignMsg !== true && instanceData.chatwootSignMsg !== false) {
        throw new BadRequestException('signMsg is required');
      }

      if (instanceData.chatwootReopenConversation !== true && instanceData.chatwootReopenConversation !== false) {
        throw new BadRequestException('reopenConversation is required');
      }

      if (instanceData.chatwootConversationPending !== true && instanceData.chatwootConversationPending !== false) {
        throw new BadRequestException('conversationPending is required');
      }

      const urlServer = this.configService.get<HttpServer>('SERVER').URL;

      try {
        this.chatwootService.create(instance, {
          enabled: true,
          accountId: instanceData.chatwootAccountId,
          token: instanceData.chatwootToken,
          url: instanceData.chatwootUrl,
          signMsg: instanceData.chatwootSignMsg || false,
          nameInbox: instanceData.chatwootNameInbox ?? instance.instanceName.split('-cwId-')[0],
          number: instanceData.number,
          reopenConversation: instanceData.chatwootReopenConversation || false,
          conversationPending: instanceData.chatwootConversationPending || false,
          importContacts: instanceData.chatwootImportContacts ?? true,
          mergeBrazilContacts: instanceData.chatwootMergeBrazilContacts ?? false,
          importMessages: instanceData.chatwootImportMessages ?? true,
          daysLimitImportMessages: instanceData.chatwootDaysLimitImportMessages ?? 60,
          organization: instanceData.chatwootOrganization,
          logo: instanceData.chatwootLogo,
          autoCreate: instanceData.chatwootAutoCreate !== false,
        });
      } catch (error) {
        this.logger.log(error);
      }

      return {
        instance: {
          instanceName: instance.instanceName,
          instanceId: instanceId,
          integration: instanceData.integration,
          webhookWaBusiness,
          accessTokenWaBusiness,
          status: instance.connectionStatus.state,
        },
        hash,
        webhook: {
          webhookUrl: instanceData?.webhook?.url,
          webhookHeaders: instanceData?.webhook?.headers,
          webhookByEvents: instanceData?.webhook?.byEvents,
          webhookBase64: instanceData?.webhook?.base64,
        },
        websocket: {
          enabled: instanceData?.websocket?.enabled,
        },
        rabbitmq: {
          enabled: instanceData?.rabbitmq?.enabled,
        },
        nats: {
          enabled: instanceData?.nats?.enabled,
        },
        sqs: {
          enabled: instanceData?.sqs?.enabled,
        },
        settings,
        chatwoot: {
          enabled: true,
          accountId: instanceData.chatwootAccountId,
          token: instanceData.chatwootToken,
          url: instanceData.chatwootUrl,
          signMsg: instanceData.chatwootSignMsg || false,
          reopenConversation: instanceData.chatwootReopenConversation || false,
          conversationPending: instanceData.chatwootConversationPending || false,
          mergeBrazilContacts: instanceData.chatwootMergeBrazilContacts ?? false,
          importContacts: instanceData.chatwootImportContacts ?? true,
          importMessages: instanceData.chatwootImportMessages ?? true,
          daysLimitImportMessages: instanceData.chatwootDaysLimitImportMessages || 60,
          number: instanceData.number,
          nameInbox: instanceData.chatwootNameInbox ?? instance.instanceName,
          webhookUrl: `${urlServer}/chatwoot/webhook/${encodeURIComponent(instance.instanceName)}`,
        },
      };
    } catch (error) {
      this.waMonitor.deleteInstance(instanceData.instanceName);
      this.logger.error(isArray(error.message) ? error.message[0] : error.message);
      throw new BadRequestException(isArray(error.message) ? error.message[0] : error.message);
    }
  }

  public async connectToWhatsapp({ instanceName, number = null }: InstanceDto) {
    try {
      const instance = this.waMonitor.waInstances[instanceName];
      const state = instance?.connectionStatus?.state;

      if (!state) {
        throw new BadRequestException('The "' + instanceName + '" instance does not exist');
      }

      // Test proxy before connecting if proxy is configured
      try {
        const proxyConfig = await this.proxyService.findProxy({ instanceName });
        if (proxyConfig && proxyConfig.enabled) {
          this.logger.verbose(`Testing proxy configuration before connecting instance ${instanceName}...`);
          const testProxy = await this.proxyService.testProxy({
            host: proxyConfig.host,
            port: proxyConfig.port,
            protocol: proxyConfig.protocol,
            username: proxyConfig.username,
            password: proxyConfig.password,
          });
          if (!testProxy) {
            throw new BadRequestException('Invalid proxy configuration. Please verify your proxy settings before connecting.');
          }
          this.logger.verbose('Proxy configuration validated successfully');
        }
      } catch (error) {
        // If proxy validation fails, throw the error
        if (error instanceof BadRequestException) {
          throw error;
        }
        // If proxy doesn't exist or other errors, continue (proxy is optional)
        this.logger.verbose('No proxy configured or error checking proxy, continuing connection...');
      }

      if (state == 'open') {
        // Já está conectado - retornar informação clara
        return {
          instance: {
            instanceName: instanceName,
            status: 'open',
          },
          connectionStatus: 'open',
          message: 'Instance is already connected',
          connected: true,
          alreadyConnected: true,
        };
      }

      if (state == 'connecting') {
        // Check if QR code already exists
        const currentQr = instance.qrCode;
        if (currentQr?.base64) {
          // Calculate age and add metadata
          const age = currentQr.timestamp ? Date.now() - currentQr.timestamp : null;
          const expiresIn = age !== null ? Math.max(0, 20000 - age) : null; // ~20s expiry

          this.logger.verbose(`Returning existing QR code (age: ${age ? Math.round(age / 1000) : 'unknown'}s)`);

          return {
            ...currentQr,
            // Add helpful metadata for frontend
            age: age, // milliseconds since generation
            expiresIn: expiresIn, // milliseconds until expiry (0 if expired)
            isExpired: expiresIn !== null && expiresIn <= 0,
            connected: false, // Still connecting
            connectionStatus: 'connecting',
          };
        }

        // No QR code yet, wait for it
        const maxAttempts = 30; // 15 segundos total (500ms * 30)
        let attempts = 0;
        let qrCode: wa.QrCode;

        this.logger.verbose(`Waiting for QR code generation (state: connecting)...`);

        while (attempts < maxAttempts) {
          qrCode = instance.qrCode;
          if (qrCode?.base64) {
            const age = qrCode.timestamp ? Date.now() - qrCode.timestamp : null;
            const expiresIn = age !== null ? Math.max(0, 20000 - age) : null;

            this.logger.verbose(`QR code generated after ${attempts * 500}ms`);

            return {
              ...qrCode,
              age: age,
              expiresIn: expiresIn,
              isExpired: false, // Just generated, definitely not expired
              connected: false, // Still connecting
              connectionStatus: 'connecting',
            };
          }
          await delay(500);
          attempts++;
        }

        // If QR code still not ready after 15 seconds, return error
        this.logger.error(`QR code not generated after ${maxAttempts * 500}ms - connection may have timed out`);
        return {
          error: true,
          message: 'QR code generation timeout. Please try reconnecting the instance.',
          connected: false,
          instance: {
            instanceName: instanceName,
            status: state,
          },
          connectionStatus: state,
        };
      }

      if (state == 'close') {
        this.logger.verbose(`Initiating connection for instance ${instanceName}...`);
        await instance.connectToWhatsapp(number);

        // Wait for QR code with extended timeout (15 seconds total)
        const maxAttempts = 30; // 15 segundos total (500ms * 30)
        let attempts = 0;
        let qrCode: wa.QrCode;

        this.logger.verbose(`Waiting for QR code generation (state: close -> connecting)...`);

        while (attempts < maxAttempts) {
          qrCode = instance.qrCode;
          if (qrCode?.base64) {
            const age = qrCode.timestamp ? Date.now() - qrCode.timestamp : null;
            const expiresIn = age !== null ? Math.max(0, 20000 - age) : null;

            this.logger.verbose(`QR code generated successfully after ${attempts * 500}ms`);

            return {
              ...qrCode,
              age: age,
              expiresIn: expiresIn,
              isExpired: false,
              connected: false,
              connectionStatus: 'connecting',
            };
          }
          await delay(500);
          attempts++;
        }

        // If QR code still not ready, return error
        this.logger.error(`QR code not generated after ${maxAttempts * 500}ms`);
        return {
          error: true,
          message: 'QR code generation timeout. The connection may be taking longer than expected.',
          connected: false,
          instance: {
            instanceName: instanceName,
            status: instance.connectionStatus?.state || 'unknown',
          },
          connectionStatus: instance.connectionStatus?.state || 'close',
        };
      }

      return {
        instance: {
          instanceName: instanceName,
          status: state,
        },
        connected: state === 'open',
        qrcode: instance?.qrCode,
        connectionStatus: state,
      };
    } catch (error) {
      this.logger.error(error);
      return { error: true, message: error.toString() };
    }
  }

  public async restartInstance({ instanceName }: InstanceDto) {
    try {
      const instance = this.waMonitor.waInstances[instanceName];
      const state = instance?.connectionStatus?.state;

      if (!state) {
        throw new BadRequestException('The "' + instanceName + '" instance does not exist');
      }

      if (state == 'close') {
        throw new BadRequestException('The "' + instanceName + '" instance is not connected');
      } else if (state == 'open') {
        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) instance.clearCacheChatwoot();
        this.logger.info('restarting instance' + instanceName);

        // Calls controller.connectToWhatsapp → instance.connectToWhatsapp (service) → createClient
        // createClient will handle: close old client, reset messageProcessor, create new client
        return await this.connectToWhatsapp({ instanceName });
      } else if (state == 'connecting') {
        // Calls controller.connectToWhatsapp → instance.connectToWhatsapp (service) → createClient
        // createClient will handle: close old client, reset messageProcessor, create new client
        return await this.connectToWhatsapp({ instanceName });
      }
    } catch (error) {
      this.logger.error(error);
      return { error: true, message: error.toString() };
    }
  }

  /**
   * Retorna o estado da conexão de forma confiável
   * SEMPRE retorna estado in-memory quando disponível (fonte única da verdade)
   */
  public async connectionState({ instanceName }: InstanceDto) {
    const instance = this.waMonitor.waInstances[instanceName];

    if (!instance) {
      // Instância não encontrada em memória - buscar do banco
      const dbInstance = await this.prismaRepository.instance.findFirst({
        where: { name: instanceName },
      });

      if (!dbInstance) {
        throw new BadRequestException(`Instance "${instanceName}" not found`);
      }

      return {
        instance: {
          instanceName: instanceName,
          state: dbInstance.connectionStatus,
          statusSource: 'database',
          isReady: false,
        },
      };
    }

    // Instância encontrada em memória - usar estado real
    const state = instance.stateConnection?.state || instance.connectionStatus?.state || 'unknown';
    const isReady = typeof instance.isConnectionReady === 'function' ? instance.isConnectionReady() : false;

    return {
      instance: {
        instanceName: instanceName,
        state: state,
        statusSource: 'live',
        isReady: isReady,
        statusReason: instance.stateConnection?.statusReason,
      },
    };
  }

  public async fetchInstances({ instanceName, instanceId, number }: InstanceDto, key: string) {
    const env = this.configService.get<Auth>('AUTHENTICATION').API_KEY;

    if (env.KEY !== key) {
      const instancesByKey = await this.prismaRepository.instance.findMany({
        where: {
          token: key,
          name: instanceName || undefined,
          id: instanceId || undefined,
        },
      });

      if (instancesByKey.length > 0) {
        const names = instancesByKey.map((instance) => instance.name);

        return this.waMonitor.instanceInfo(names);
      } else {
        throw new UnauthorizedException();
      }
    }

    if (instanceId || number) {
      return this.waMonitor.instanceInfoById(instanceId, number);
    }

    const instanceNames = instanceName ? [instanceName] : null;

    return this.waMonitor.instanceInfo(instanceNames);
  }

  public async setPresence({ instanceName }: InstanceDto, data: SetPresenceDto) {
    return await this.waMonitor.waInstances[instanceName].setPresence(data);
  }

  public async logout({ instanceName }: InstanceDto) {
    const { instance } = await this.connectionState({ instanceName });

    if (instance.state === 'close') {
      throw new BadRequestException('The "' + instanceName + '" instance is not connected');
    }

    try {
      await this.waMonitor.waInstances[instanceName]?.logoutInstance();

      return { status: 'SUCCESS', error: false, response: { message: 'Instance logged out' } };
    } catch (error) {
      throw new InternalServerErrorException(error.toString());
    }
  }

  public async deleteInstance({ instanceName }: InstanceDto) {
    const { instance } = await this.connectionState({ instanceName });
    try {
      const waInstances = this.waMonitor.waInstances[instanceName];
      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) waInstances?.clearCacheChatwoot();

      if (instance.state === 'connecting' || instance.state === 'open') {
        await this.logout({ instanceName });
      }

      try {
        waInstances?.sendDataWebhook(Events.INSTANCE_DELETE, {
          instanceName,
          instanceId: waInstances.instanceId,
        });
      } catch (error) {
        this.logger.error(error);
      }

      this.eventEmitter.emit('remove.instance', instanceName, 'inner');
      return { status: 'SUCCESS', error: false, response: { message: 'Instance deleted' } };
    } catch (error) {
      throw new BadRequestException(error.toString());
    }
  }
}
