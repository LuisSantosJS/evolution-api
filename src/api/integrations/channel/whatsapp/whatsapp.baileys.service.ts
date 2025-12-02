import { getCollectionsDto } from '@api/dto/business.dto';
import { OfferCallDto } from '@api/dto/call.dto';
import {
  ArchiveChatDto,
  BlockUserDto,
  DeleteMessage,
  getBase64FromMediaMessageDto,
  LastMessage,
  MarkChatUnreadDto,
  NumberBusiness,
  OnWhatsAppDto,
  PrivacySettingDto,
  ReadMessageDto,
  SendPresenceDto,
  SyncMessagesDto,
  UpdateMessageDto,
  WhatsAppNumberDto,
} from '@api/dto/chat.dto';
import {
  AcceptGroupInvite,
  CreateGroupDto,
  GetParticipant,
  GroupDescriptionDto,
  GroupInvite,
  GroupJid,
  GroupPictureDto,
  GroupSendInvite,
  GroupSubjectDto,
  GroupToggleEphemeralDto,
  GroupUpdateParticipantDto,
  GroupUpdateSettingDto,
} from '@api/dto/group.dto';
import { InstanceDto, SetPresenceDto } from '@api/dto/instance.dto';
import { HandleLabelDto, LabelDto } from '@api/dto/label.dto';
import {
  Button,
  ContactMessage,
  KeyType,
  MediaMessage,
  Options,
  SendAudioDto,
  SendButtonsDto,
  SendContactDto,
  SendListDto,
  SendLocationDto,
  SendMediaDto,
  SendPollDto,
  SendPtvDto,
  SendReactionDto,
  SendStatusDto,
  SendStickerDto,
  SendTextDto,
  StatusMessage,
  TypeButton,
} from '@api/dto/sendMessage.dto';
import { chatwootImport } from '@api/integrations/chatbot/chatwoot/utils/chatwoot-import-helper';
import * as s3Service from '@api/integrations/storage/s3/libs/minio.server';
import { ProviderFiles } from '@api/provider/sessions';
import { PrismaRepository, Query } from '@api/repository/repository.service';
import { chatbotController, waMonitor } from '@api/server.module';
import { CacheService } from '@api/services/cache.service';
import { ChannelStartupService } from '@api/services/channel.service';
import { LidMappingService } from '@api/services/lid-mapping.service';
import { Events, MessageSubtype, TypeMediaMessage, wa } from '@api/types/wa.types';
import { CacheEngine } from '@cache/cacheengine';
import {
  CacheConf,
  Chatwoot,
  ConfigService,
  configService,
  ConfigSessionPhone,
  Database,
  Log,
  Openai,
  ProviderSession,
  QrCode,
  S3,
} from '@config/env.config';
import { BadRequestException, InternalServerErrorException, NotFoundException } from '@exceptions';
import { Boom } from '@hapi/boom';
import { createId as cuid } from '@paralleldrive/cuid2';
import { Instance, Message } from '@prisma/client';
import { createJid } from '@utils/createJid';
import { fetchLatestWaWebVersion } from '@utils/fetchLatestWaWebVersion';
import { makeProxyAgent } from '@utils/makeProxyAgent';
import { getOnWhatsappCache, saveOnWhatsappCache } from '@utils/onWhatsappCache';
import { status } from '@utils/renderStatus';
import useMultiFileAuthStatePrisma from '@utils/use-multi-file-auth-state-prisma';
import { AuthStateProvider } from '@utils/use-multi-file-auth-state-provider-files';
import { useMultiFileAuthStateRedisDb } from '@utils/use-multi-file-auth-state-redis-db';
import axios from 'axios';
import makeWASocket, {
  AnyMessageContent,
  BufferedEventData,
  BufferJSON,
  CacheStore,
  CatalogCollection,
  Chat,
  ConnectionState,
  Contact,
  delay,
  DisconnectReason,
  downloadContentFromMessage,
  downloadMediaMessage,
  generateWAMessageFromContent,
  getAggregateVotesInPollMessage,
  GetCatalogOptions,
  getContentType,
  getDevice,
  GroupMetadata,
  GroupParticipant,
  isJidBroadcast,
  isJidGroup,
  isJidNewsletter,
  isLidUser,
  makeCacheableSignalKeyStore,
  MessageUpsertType,
  MessageUserReceiptUpdate,
  MiscMessageGenerationOptions,
  ParticipantAction,
  prepareWAMessageMedia,
  Product,
  proto,
  UserFacingSocketConfig,
  WABrowserDescription,
  WAMediaUpload,
  WAMessage,
  WAMessageKey,
  WAPresence,
  WASocket,
} from '@whiskeysockets/baileys';
import { Label } from '@whiskeysockets/baileys/lib/Types/Label';
import { LabelAssociation } from '@whiskeysockets/baileys/lib/Types/LabelAssociation';
import { spawn } from 'child_process';
import { isArray, isBase64, isURL } from 'class-validator';
import { randomBytes } from 'crypto';
import EventEmitter2 from 'eventemitter2';
import ffmpeg from 'fluent-ffmpeg';
import FormData from 'form-data';
import Long from 'long';
import mimeTypes from 'mime-types';
import NodeCache from 'node-cache';
import cron from 'node-cron';
import { release } from 'os';
import { join } from 'path';
import P from 'pino';
import qrcode, { QRCodeToDataURLOptions } from 'qrcode';
import qrcodeTerminal from 'qrcode-terminal';
import sharp from 'sharp';
import { PassThrough, Readable } from 'stream';
import { v4 } from 'uuid';

import { BaileysMessageProcessor } from './baileysMessage.processor';
import { useVoiceCallsBaileys } from './voiceCalls/useVoiceCallsBaileys';

const groupMetadataCache = new CacheService(new CacheEngine(configService, 'groups').getEngine());

// Configure ffmpeg path - try npm package first, fall back to system ffmpeg for Alpine Linux
let ffmpegBinaryPath = '/usr/bin/ffmpeg'; // Default to system ffmpeg (installed via apk in Docker)
try {
  // Try to load the npm package (works on most platforms)
  const ffmpegInstaller = require('@ffmpeg-installer/ffmpeg');
  if (ffmpegInstaller?.path) {
    ffmpegBinaryPath = ffmpegInstaller.path;
  }
} catch (error) {
  // Npm package not available (e.g., Alpine Linux musl), use system ffmpeg
  console.log('[FFmpeg] Using system ffmpeg at /usr/bin/ffmpeg (npm package not available)');
}

// Configure fluent-ffmpeg to use the correct binary
ffmpeg.setFfmpegPath(ffmpegBinaryPath);

// Helper function to check if a JID is a phone number user (compatibility with older Baileys versions)
const isPnUser = (jid: string): boolean => {
  return jid?.endsWith('@s.whatsapp.net') || false;
};

// Adicione a função getVideoDuration no início do arquivo
async function getVideoDuration(input: Buffer | string | Readable): Promise<number> {
  const MediaInfoFactory = (await import('mediainfo.js')).default;
  const mediainfo = await MediaInfoFactory({ format: 'JSON' });

  let fileSize: number;
  let readChunk: (size: number, offset: number) => Promise<Buffer>;

  if (Buffer.isBuffer(input)) {
    fileSize = input.length;
    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      return input.slice(offset, offset + size);
    };
  } else if (typeof input === 'string') {
    const fs = await import('fs');
    const stat = await fs.promises.stat(input);
    fileSize = stat.size;
    const fd = await fs.promises.open(input, 'r');

    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      const buffer = Buffer.alloc(size);
      await fd.read(buffer, 0, size, offset);
      return buffer;
    };

    try {
      const result = await mediainfo.analyzeData(() => fileSize, readChunk);
      const jsonResult = JSON.parse(result);

      const generalTrack = jsonResult.media.track.find((t: any) => t['@type'] === 'General');
      const duration = generalTrack.Duration;

      return Math.round(parseFloat(duration));
    } finally {
      await fd.close();
    }
  } else if (input instanceof Readable) {
    const chunks: Buffer[] = [];
    for await (const chunk of input) {
      chunks.push(chunk);
    }
    const data = Buffer.concat(chunks);
    fileSize = data.length;

    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      return data.slice(offset, offset + size);
    };
  } else {
    throw new Error('Tipo de entrada não suportado');
  }

  const result = await mediainfo.analyzeData(() => fileSize, readChunk);
  const jsonResult = JSON.parse(result);

  const generalTrack = jsonResult.media.track.find((t: any) => t['@type'] === 'General');
  const duration = generalTrack.Duration;

  return Math.round(parseFloat(duration));
}

export class BaileysStartupService extends ChannelStartupService {
  private messageProcessor = new BaileysMessageProcessor();
  private lidMappingService: LidMappingService;

  constructor(
    public readonly configService: ConfigService,
    public readonly eventEmitter: EventEmitter2,
    public readonly prismaRepository: PrismaRepository,
    public readonly cache: CacheService,
    public readonly chatwootCache: CacheService,
    public readonly baileysCache: CacheService,
    private readonly providerFiles: ProviderFiles,
  ) {
    super(configService, eventEmitter, prismaRepository, chatwootCache);
    this.instance.qrcode = { count: 0 };
    this.messageProcessor.mount({
      onMessageReceive: this.messageHandle['messages.upsert'].bind(this), // Bind the method to the current context
    });

    this.authStateProvider = new AuthStateProvider(this.providerFiles);
    this.lidMappingService = new LidMappingService(this.cache);
  }

  private authStateProvider: AuthStateProvider;
  // Message retry counter cache - TTL of 60s to prevent stale counters after reconnection
  private readonly msgRetryCounterCache: CacheStore = new NodeCache({ stdTTL: 60, useClones: false });
  private readonly userDevicesCache: CacheStore = new NodeCache({ stdTTL: 300000, useClones: false });
  private readonly messageStubRetryCache: Map<string, WAMessage> = new Map();
  private readonly sessionErrorCache: Map<string, { count: number; lastError: number }> = new Map();
  private endSession = false;
  private logBaileys = this.configService.get<Log>('LOG').BAILEYS;
  private lastActivity: number = Date.now();

  // Settings cache - Background refresh to avoid blocking event loop
  private settingsCache: any = null;
  private settingsCacheInterval: NodeJS.Timeout | null = null;
  private readonly SETTINGS_CACHE_REFRESH_INTERVAL = 60000; // 1 minute - refresh in background

  // Message processor health check
  private streamHealthCheckInterval: NodeJS.Timeout | null = null;
  private readonly STREAM_HEALTH_CHECK_INTERVAL = 30000; // 30 seconds - check if stream is alive

  // Event listener callbacks stored for cleanup
  private wsCallListener: ((packet: any) => void) | null = null;
  private wsCallAckListener: ((packet: any) => void) | null = null;
  private wsErrorListener: ((error: Error) => void) | null = null;
  private wsCloseListener: ((code: number, reason: Buffer) => void) | null = null;
  private wsPongListener: (() => void) | null = null;

  // Reconnection lock to prevent simultaneous reconnections
  private reconnectLock = false;
  private reconnectAttempts = 0;
  private readonly MAX_RECONNECT_DELAY = 30000; // Max 30 seconds
  private readonly BASE_RECONNECT_DELAY = 2000; // Start with 2 seconds
  private isClientReady = false;
  private clientReadyTimeout: NodeJS.Timeout | null = null; // NOVO: Para limpar timeout

  // Status synchronization
  private statusUpdateLock = false;
  private lastDatabaseStatusUpdate: { state: string; timestamp: number } | null = null;
  private readonly STATUS_SYNC_DEBOUNCE_MS = 1000; // Debounce database updates

  // Constants for cache limits
  private readonly MAX_MESSAGE_STUB_RETRY_CACHE = 1000;
  private readonly MAX_SESSION_ERROR_CACHE = 500;
  private readonly SESSION_ERROR_TTL_MS = 3600000; // 1 hour

  public stateConnection: wa.StateConnection = { state: 'close' };

  public phoneNumber: string;

  public get connectionStatus() {
    return this.stateConnection;
  }

  /**
   * Verifica se a conexão está realmente pronta para enviar mensagens e mídias
   * Implementa verificações robustas além do simples state === 'open'
   */
  public isConnectionReady(): boolean {
    try {
      // Verificação 1: Estado básico da conexão
      if (this.stateConnection.state !== 'open') {
        this.logger.warn(`[isConnectionReady] FAILED - Check 1: state is '${this.stateConnection.state}' (expected 'open')`);
        return false;
      }

      // Verificação 2: Cliente existe e não está marcado para encerramento
      if (!this.client || this.endSession) {
        this.logger.warn(`[isConnectionReady] FAILED - Check 2: client=${!!this.client}, endSession=${this.endSession}`);
        return false;
      }

      // Verificação 3: User ID está definido (autenticação completa)
      if (!this.client.user || !this.client.user.id) {
        this.logger.warn(`[isConnectionReady] FAILED - Check 3: user=${!!this.client.user}, userId=${this.client.user?.id ?? 'undefined'}`);
        return false;
      }

      // Verificação 4: Flag de cliente pronto está ativada
      if (!this.isClientReady) {
        this.logger.warn(`[isConnectionReady] FAILED - Check 4: isClientReady=${this.isClientReady} (client initialization not complete)`);
        return false;
      }

      // Verificação 5 (opcional): WebSocket está conectado
      // NOTA: Esta verificação é informativa mas não bloqueante, pois o Baileys
      // pode encapsular o WebSocket de forma que o readyState não seja diretamente acessível
      const wsReadyState = (this.client.ws as any)?.readyState;
      if (this.client.ws && wsReadyState !== undefined && wsReadyState !== 1) {
        // Se conseguimos ler o readyState e ele NÃO é OPEN (1), isso pode ser um problema
        this.logger.warn(`[isConnectionReady] WARNING - WebSocket readyState=${wsReadyState} (not OPEN=1). States: 0=CONNECTING, 2=CLOSING, 3=CLOSED`);
        // Apenas bloquear se o WebSocket está explicitamente CLOSING ou CLOSED
        if (wsReadyState === 2 || wsReadyState === 3) {
          this.logger.warn(`[isConnectionReady] FAILED - Check 5: WebSocket is closing or closed`);
          return false;
        }
      }

      this.logger.verbose(`[isConnectionReady] SUCCESS - All checks passed (wsReadyState=${wsReadyState ?? 'N/A'})`);
      return true;
    } catch (error: any) {
      this.logger.error(`[isConnectionReady] ERROR: ${error?.message || error}`);
      return false;
    }
  }

  /**
   * Atualiza o status da conexão de forma atômica e sincronizada
   * - Atualiza memória imediatamente (in-memory state)
   * - Atualiza banco de dados de forma assíncrona com debounce
   * - Garante consistência entre todas as fontes de status
   *
   * @param newState - Novo estado da conexão
   * @param additionalData - Dados adicionais para atualizar no banco (opcional)
   */
  private async updateConnectionStatus(
    newState: 'open' | 'close' | 'connecting',
    additionalData?: {
      ownerJid?: string;
      profileName?: string;
      profilePicUrl?: string;
      disconnectionAt?: Date;
      disconnectionReasonCode?: number;
      disconnectionObject?: string;
    },
  ) {
    const previousState = this.stateConnection.state;

    // 1. Atualizar estado em memória IMEDIATAMENTE (fonte única da verdade)
    this.stateConnection.state = newState;
    this.logger.verbose(`Connection status updated in memory: ${previousState} -> ${newState}`);

    // 2. Atualizar banco de dados de forma assíncrona e com debounce
    // Não bloquear a operação principal
    this.updateDatabaseStatusAsync(newState, additionalData).catch((err) => {
      this.logger.error(`Failed to sync connection status to database: ${err.message}`);
      // Não fazer rollback do estado em memória - memória é a fonte da verdade
    });
  }

  /**
   * Atualiza o status no banco de dados de forma assíncrona com debounce
   * Previne atualizações excessivas ao banco durante mudanças rápidas de estado
   */
  private async updateDatabaseStatusAsync(
    state: string,
    additionalData?: {
      ownerJid?: string;
      profileName?: string;
      profilePicUrl?: string;
      disconnectionAt?: Date;
      disconnectionReasonCode?: number;
      disconnectionObject?: string;
    },
  ) {
    // Debounce: Se última atualização foi recente e para o mesmo estado, skip
    const now = Date.now();
    if (
      this.lastDatabaseStatusUpdate &&
      this.lastDatabaseStatusUpdate.state === state &&
      now - this.lastDatabaseStatusUpdate.timestamp < this.STATUS_SYNC_DEBOUNCE_MS
    ) {
      this.logger.verbose(`Debouncing database status update for state: ${state}`);
      return;
    }

    // CORREÇÃO: Prevenir atualizações simultâneas com retry limitado
    const maxRetries = 5;
    const retryDelay = 100;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      if (!this.statusUpdateLock) {
        break; // Lock está livre, pode prosseguir
      }

      if (attempt === maxRetries - 1) {
        // Última tentativa - skip update para evitar bloqueio
        this.logger.warn(`Status update skipped after ${maxRetries} attempts - lock still held`);
        return;
      }

      this.logger.verbose(`Status update lock held, waiting... (attempt ${attempt + 1}/${maxRetries})`);
      await delay(retryDelay);
    }

    try {
      this.statusUpdateLock = true;

      const updateData: any = {
        connectionStatus: state,
        ...additionalData,
      };

      await this.prismaRepository.instance.update({
        where: { id: this.instanceId },
        data: updateData,
      });

      this.lastDatabaseStatusUpdate = { state, timestamp: now };
      this.logger.verbose(`Database status synced successfully: ${state}`);
    } catch (error) {
      this.logger.error(`Database status sync failed: ${error.message}`);
      throw error;
    } finally {
      this.statusUpdateLock = false;
    }
  }

  /**
   * Retorna o estado atual da conexão de forma confiável
   * Sempre retorna o estado em memória (fonte única da verdade)
   */
  public getConnectionState(): wa.StateConnection {
    return {
      state: this.stateConnection.state,
      statusReason: this.stateConnection.statusReason,
    };
  }

  public async logoutInstance() {
    this.cleanupEventListeners();
    this.messageProcessor.onDestroy();
    await this.client?.logout('Log out instance: ' + this.instanceName);

    this.client?.ws?.close();

    const sessionExists = await this.prismaRepository.session.findFirst({ where: { sessionId: this.instanceId } });
    if (sessionExists) {
      await this.prismaRepository.session.delete({ where: { sessionId: this.instanceId } });
    }
  }


  /**
   * Starts background settings cache refresh to avoid blocking event loop
   * Critical for high-volume instances to prevent message loss
   */
  private async startSettingsCacheRefresh() {
    // Stop any existing interval
    this.stopSettingsCacheRefresh();

    // Initial cache load
    try {
      this.settingsCache = await this.findSettings();
      this.logger.verbose('Settings cache initialized');
    } catch (error) {
      this.logger.error('Failed to initialize settings cache:');
      this.logger.error(error);
      this.settingsCache = null;
    }

    // Refresh cache in background every minute
    this.settingsCacheInterval = setInterval(async () => {
      try {
        this.settingsCache = await this.findSettings();
        this.logger.verbose('Settings cache refreshed');
      } catch (error) {
        this.logger.warn('Failed to refresh settings cache (using stale cache):');
        this.logger.warn(error);
        // Keep using old cache on error
      }
    }, this.SETTINGS_CACHE_REFRESH_INTERVAL);
  }

  /**
   * Stops background settings cache refresh
   */
  private stopSettingsCacheRefresh() {
    if (this.settingsCacheInterval) {
      clearInterval(this.settingsCacheInterval);
      this.settingsCacheInterval = null;
    }
  }

  /**
   * Starts background health check for message processor stream
   * Automatically recreates the stream if it dies
   */
  private startStreamHealthCheck() {
    // Clear any existing interval
    this.stopStreamHealthCheck();

    this.streamHealthCheckInterval = setInterval(() => {
      try {
        if (!this.messageProcessor.isStreamHealthy()) {
          const stats = this.messageProcessor.getStreamStats();
          this.logger.error('Message processor stream is DEAD! Recreating...');
          this.logger.error(`Stream stats before recreation: ${JSON.stringify(stats)}`);

          // Recreate the message processor
          this.messageProcessor.onDestroy();
          this.messageProcessor = new BaileysMessageProcessor();
          this.messageProcessor.mount({
            onMessageReceive: this.messageHandle['messages.upsert'].bind(this),
          });

          this.logger.warn('Message processor stream RECREATED successfully');
        }
      } catch (error) {
        this.logger.error('Error during stream health check:');
        this.logger.error(error);
      }
    }, this.STREAM_HEALTH_CHECK_INTERVAL);

    this.logger.verbose(`Stream health check started (interval: ${this.STREAM_HEALTH_CHECK_INTERVAL}ms)`);
  }

  /**
   * Stops background stream health check
   */
  private stopStreamHealthCheck() {
    if (this.streamHealthCheckInterval) {
      clearInterval(this.streamHealthCheckInterval);
      this.streamHealthCheckInterval = null;
    }
  }

  private cleanupEventListeners() {
    try {
      // Remove WebSocket event listeners
      if (this.client?.ws) {
        if (this.wsCallListener) {
          this.client.ws.removeListener('CB:call', this.wsCallListener);
          this.wsCallListener = null;
        }
        if (this.wsCallAckListener) {
          this.client.ws.removeListener('CB:ack,class:call', this.wsCallAckListener);
          this.wsCallAckListener = null;
        }
        // CORREÇÃO: Remover novos listeners também
        if (this.wsErrorListener) {
          this.client.ws.removeListener('error', this.wsErrorListener);
          this.wsErrorListener = null;
        }
        if (this.wsCloseListener) {
          this.client.ws.removeListener('close', this.wsCloseListener);
          this.wsCloseListener = null;
        }
        if (this.wsPongListener) {
          this.client.ws.removeListener('pong', this.wsPongListener);
          this.wsPongListener = null;
        }
      }

      // Clear cache maps with size limits
      this.messageStubRetryCache.clear();
      this.sessionErrorCache.clear();

      // Stop background intervals
      this.stopSettingsCacheRefresh();
      this.stopStreamHealthCheck();
      this.settingsCache = null;
    } catch (error) {
      this.logger.warn('Error during event listener cleanup:');
      this.logger.warn(error);
    }
  }

  private pruneMapCache<K, V>(map: Map<K, V>, maxSize: number) {
    if (map.size >= maxSize) {
      // Remove oldest entries (first 10% of max size)
      const entriesToRemove = Math.floor(maxSize * 0.1);
      const keysToDelete = Array.from(map.keys()).slice(0, entriesToRemove);
      keysToDelete.forEach((key) => map.delete(key));
      this.logger.verbose(`Pruned ${entriesToRemove} entries from cache (size was ${map.size + entriesToRemove})`);
    }
  }

  private addToSessionErrorCache(jid: string) {
    this.pruneMapCache(this.sessionErrorCache, this.MAX_SESSION_ERROR_CACHE);

    const now = Date.now();
    const existing = this.sessionErrorCache.get(jid);

    if (existing && now - existing.lastError < this.SESSION_ERROR_TTL_MS) {
      this.sessionErrorCache.set(jid, {
        count: existing.count + 1,
        lastError: now,
      });
    } else {
      this.sessionErrorCache.set(jid, {
        count: 1,
        lastError: now,
      });
    }
  }


  private markActivity() {
    this.lastActivity = Date.now();
  }

  public async getProfileName() {
    let profileName = this.client.user?.name ?? this.client.user?.verifiedName;
    if (!profileName) {
      const data = await this.prismaRepository.session.findUnique({ where: { sessionId: this.instanceId } });

      if (data) {
        const creds = JSON.parse(JSON.stringify(data.creds), BufferJSON.reviver);
        profileName = creds.me?.name || creds.me?.verifiedName;
      }
    }

    return profileName;
  }

  public async getProfileStatus() {
    const status = await this.client.fetchStatus(this.instance.wuid);

    return status[0]?.status;
  }

  public get profilePictureUrl() {
    return this.instance.profilePictureUrl;
  }

  public get qrCode(): wa.QrCode {
    return {
      pairingCode: this.instance.qrcode?.pairingCode,
      code: this.instance.qrcode?.code,
      base64: this.instance.qrcode?.base64,
      count: this.instance.qrcode?.count,
    };
  }

  private async connectionUpdate({ qr, connection, lastDisconnect }: Partial<ConnectionState>) {
    if (qr) {
      if (this.instance.qrcode.count === this.configService.get<QrCode>('QRCODE').LIMIT) {
        this.sendDataWebhook(Events.QRCODE_UPDATED, {
          message: 'QR code limit reached, please login again',
          statusCode: DisconnectReason.badSession,
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
            Events.QRCODE_UPDATED,
            { instanceName: this.instance.name, instanceId: this.instanceId },
            { message: 'QR code limit reached, please login again', statusCode: DisconnectReason.badSession },
          );
        }

        this.sendDataWebhook(Events.CONNECTION_UPDATE, {
          instance: this.instance.name,
          state: 'refused',
          statusReason: DisconnectReason.connectionClosed,
          wuid: this.instance.wuid,
          profileName: await this.getProfileName(),
          profilePictureUrl: this.instance.profilePictureUrl,
        });

        this.endSession = true;

        return this.eventEmitter.emit('no.connection', this.instance.name);
      }

      this.instance.qrcode.count++;

      const color = this.configService.get<QrCode>('QRCODE').COLOR;

      const optsQrcode: QRCodeToDataURLOptions = {
        margin: 3,
        scale: 4,
        errorCorrectionLevel: 'H',
        color: { light: '#ffffff', dark: color },
      };

      if (this.phoneNumber) {
        await delay(1000);
        this.instance.qrcode.pairingCode = await this.client.requestPairingCode(this.phoneNumber);
      } else {
        this.instance.qrcode.pairingCode = null;
      }

      qrcode.toDataURL(qr, optsQrcode, (error, base64) => {
        if (error) {
          this.logger.error('Qrcode generate failed:' + error.toString());
          return;
        }

        this.instance.qrcode.base64 = base64;
        this.instance.qrcode.code = qr;

        this.sendDataWebhook(Events.QRCODE_UPDATED, {
          qrcode: { instance: this.instance.name, pairingCode: this.instance.qrcode.pairingCode, code: qr, base64 },
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
            Events.QRCODE_UPDATED,
            { instanceName: this.instance.name, instanceId: this.instanceId },
            {
              qrcode: { instance: this.instance.name, pairingCode: this.instance.qrcode.pairingCode, code: qr, base64 },
            },
          );
        }
      });

      qrcodeTerminal.generate(qr, { small: true }, (qrcode) =>
        this.logger.log(
          `\n{ instance: ${this.instance.name} pairingCode: ${this.instance.qrcode.pairingCode}, qrcodeCount: ${this.instance.qrcode.count} }\n` +
            qrcode,
        ),
      );

      // Usar método atômico para atualizar status
      await this.updateConnectionStatus('connecting');
    }

    // Atualizar statusReason ANTES de processar mudanças de estado
    if (connection) {
      this.stateConnection.statusReason = (lastDisconnect?.error as Boom)?.output?.statusCode ?? 200;
    }

    // Processar mudança de estado baseado no valor de 'connection'
    if (connection === 'close') {
      // Mark client as not ready when connection closes
      this.isClientReady = false;

      // Clear any pending clientReady timeout
      if (this.clientReadyTimeout) {
        clearTimeout(this.clientReadyTimeout);
        this.clientReadyTimeout = null;
      }

      const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;

      // Códigos que NÃO devem reconnectar automaticamente:
      // - loggedOut: usuário fez logout manualmente
      // - forbidden: conta banida/bloqueada pelo WhatsApp
      // - 402: Payment Required (conta com problemas de pagamento)
      // - 406: Not Acceptable (versão incompatível)
      const codesToNotReconnect = [DisconnectReason.loggedOut, DisconnectReason.forbidden, 402, 406];
      const shouldReconnect = !codesToNotReconnect.includes(statusCode);

      if (shouldReconnect && !this.reconnectLock) {
        this.reconnectLock = true;
        this.reconnectAttempts++;

        // Calculate exponential backoff delay: 2s, 4s, 8s, 16s, 30s (max)
        const exponentialDelay = Math.min(
          this.BASE_RECONNECT_DELAY * Math.pow(2, this.reconnectAttempts - 1),
          this.MAX_RECONNECT_DELAY,
        );

        this.logger.info(
          `Initiating reconnection with lock... (attempt #${this.reconnectAttempts}, delay: ${exponentialDelay}ms)`,
        );

        // Atualizar status para 'close' atomicamente
        await this.updateConnectionStatus('close', {
          disconnectionAt: new Date(),
          disconnectionReasonCode: statusCode,
          disconnectionObject: JSON.stringify(lastDisconnect),
        });

        try {
          // Exponential backoff delay to give WhatsApp server time to clear session state
          // Critical for preventing "Bad MAC" and "MessageCounterError" after reconnection
          await delay(exponentialDelay);
          await this.connectToWhatsapp(this.phoneNumber);

          // Reset reconnect attempts on successful connection (will be set to 0 when connection opens)
        } catch (error) {
          this.logger.error(`Reconnection failed (attempt #${this.reconnectAttempts}): ${error.message}`);
          // Don't reset attempts - let exponential backoff continue
        } finally {
          this.reconnectLock = false;
        }
      } else if (this.reconnectLock) {
        this.logger.warn('Reconnection already in progress, skipping duplicate reconnection attempt');
        // Atualizar status mesmo se reconexão já está em progresso
        await this.updateConnectionStatus('close', {
          disconnectionAt: new Date(),
          disconnectionReasonCode: statusCode,
          disconnectionObject: JSON.stringify(lastDisconnect),
        });
      } else {
        // Código padrão para erros que não devem reconnectar
        this.sendDataWebhook(Events.STATUS_INSTANCE, {
          instance: this.instance.name,
          status: 'closed',
          disconnectionAt: new Date(),
          disconnectionReasonCode: statusCode,
          disconnectionObject: JSON.stringify(lastDisconnect),
        });

        // Usar método atômico para atualizar status
        await this.updateConnectionStatus('close', {
          disconnectionAt: new Date(),
          disconnectionReasonCode: statusCode,
          disconnectionObject: JSON.stringify(lastDisconnect),
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
            Events.STATUS_INSTANCE,
            { instanceName: this.instance.name, instanceId: this.instanceId },
            { instance: this.instance.name, status: 'closed' },
          );
        }

        this.eventEmitter.emit('logout.instance', this.instance.name, 'inner');
        this.client?.ws?.close();
        this.client.end(new Error('Close connection'));

        this.sendDataWebhook(Events.CONNECTION_UPDATE, {
          instance: this.instance.name,
          state: 'close', // Usar valor explícito
          statusReason: this.stateConnection.statusReason,
        });
      }
    }

    if (connection === 'open') {
      this.instance.wuid = this.client.user.id.replace(/:\d+/, '');
      try {
        const profilePic = await this.profilePicture(this.instance.wuid);
        this.instance.profilePictureUrl = profilePic.profilePictureUrl;
      } catch (error) {
        this.instance.profilePictureUrl = null;
      }
      const formattedWuid = this.instance.wuid.split('@')[0].padEnd(30, ' ');
      const formattedName = this.instance.name;
      this.logger.info(
        `
        ┌──────────────────────────────┐
        │    CONNECTED TO WHATSAPP     │
        └──────────────────────────────┘`.replace(/^ +/gm, '  '),
      );
      this.logger.info(
        `
        wuid: ${formattedWuid}
        name: ${formattedName}
      `,
      );

      // Usar método atômico para atualizar status + dados adicionais
      await this.updateConnectionStatus('open', {
        ownerJid: this.instance.wuid,
        profileName: (await this.getProfileName()) as string,
        profilePicUrl: this.instance.profilePictureUrl,
      });

      // Reset reconnect attempts counter on successful connection
      this.reconnectAttempts = 0;
      this.logger.verbose('Reconnection attempts counter reset');

      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
        this.chatwootService.eventWhatsapp(
          Events.CONNECTION_UPDATE,
          { instanceName: this.instance.name, instanceId: this.instanceId },
          { instance: this.instance.name, status: 'open' },
        );
        this.syncChatwootLostMessages();
      }

      this.sendDataWebhook(Events.CONNECTION_UPDATE, {
        instance: this.instance.name,
        wuid: this.instance.wuid,
        profileName: await this.getProfileName(),
        profilePictureUrl: this.instance.profilePictureUrl,
        state: 'open', // Usar valor explícito
        statusReason: this.stateConnection.statusReason,
      });

      // Mark client as ready after all initialization is complete
      // Small delay to ensure all async operations are done
      // CORREÇÃO: Armazenar timeout para permitir limpeza
      if (this.clientReadyTimeout) {
        clearTimeout(this.clientReadyTimeout);
      }
      this.clientReadyTimeout = setTimeout(() => {
        this.isClientReady = true;
        this.logger.info('Client marked as ready for operations');
        this.clientReadyTimeout = null;
      }, 1500);
    }

    if (connection === 'connecting') {
      // CORREÇÃO: Atualizar state para 'connecting' se ainda não foi atualizado via QR
      // Isso garante que state está correto mesmo se evento connecting vier sem QR
      if (this.stateConnection.state !== 'connecting') {
        await this.updateConnectionStatus('connecting');
      }

      this.sendDataWebhook(Events.CONNECTION_UPDATE, {
        instance: this.instance.name,
        state: 'connecting', // Usar valor explícito ao invés de spread
        statusReason: this.stateConnection.statusReason,
      });
    }
  }

  private async getMessage(key: proto.IMessageKey, full = false) {
    try {
      const webMessageInfo = (await this.prismaRepository.message.findMany({
        where: { instanceId: this.instanceId, key: { path: ['id'], equals: key.id } },
      })) as unknown as proto.IWebMessageInfo[];
      if (full) {
        return webMessageInfo[0];
      }
      if (webMessageInfo[0].message?.pollCreationMessage) {
        const messageSecretBase64 = webMessageInfo[0].message?.messageContextInfo?.messageSecret;

        if (typeof messageSecretBase64 === 'string') {
          const messageSecret = Buffer.from(messageSecretBase64, 'base64');

          const msg = {
            messageContextInfo: { messageSecret },
            pollCreationMessage: webMessageInfo[0].message?.pollCreationMessage,
          };

          return msg;
        }
      }

      return webMessageInfo[0].message;
    } catch (error) {
      return { conversation: '' };
    }
  }

  private async defineAuthState() {
    const db = this.configService.get<Database>('DATABASE');
    const cache = this.configService.get<CacheConf>('CACHE');

    const provider = this.configService.get<ProviderSession>('PROVIDER');

    if (provider?.ENABLED) {
      return await this.authStateProvider.authStateProvider(this.instance.id);
    }

    if (cache?.REDIS.ENABLED && cache?.REDIS.SAVE_INSTANCES) {
      this.logger.info('Redis enabled');
      return await useMultiFileAuthStateRedisDb(this.instance.id, this.cache);
    }

    if (db.SAVE_DATA.INSTANCE) {
      return await useMultiFileAuthStatePrisma(this.instance.id, this.cache);
    }
  }

  private async createClient(number?: string): Promise<WASocket> {
    this.instance.authState = await this.defineAuthState();

    const session = this.configService.get<ConfigSessionPhone>('CONFIG_SESSION_PHONE');

    let browserOptions = {};

    if (number || this.phoneNumber) {
      this.phoneNumber = number;

      this.logger.info(`Phone number: ${number}`);
    } else {
      const browser: WABrowserDescription = [session.CLIENT, session.NAME, release()];
      browserOptions = { browser };

      this.logger.info(`Browser: ${browser}`);
    }

    const baileysVersion = await fetchLatestWaWebVersion({});
    const version = baileysVersion.version;
    const log = `Baileys version: ${version.join('.')}`;

    // if (session.VERSION) {
    //   version = session.VERSION.split('.');
    //   log = `Baileys version env: ${version}`;
    // } else {
    //   const baileysVersion = await fetchLatestWaWebVersion({});
    //   version = baileysVersion.version;
    //   log = `Baileys version: ${version}`;
    // }

    this.logger.info(log);

    this.logger.info(`Group Ignore: ${this.localSettings.groupsIgnore}`);

    let options;

    if (this.localProxy?.enabled) {
      this.logger.info('Proxy enabled: ' + this.localProxy?.host);

      if (this.localProxy?.host?.includes('proxyscrape')) {
        try {
          const response = await axios.get(this.localProxy?.host);
          const text = response.data;
          const proxyUrls = text.split('\r\n');
          const rand = Math.floor(Math.random() * Math.floor(proxyUrls.length));
          const proxyUrl = 'http://' + proxyUrls[rand];
          options = { agent: makeProxyAgent(proxyUrl), fetchAgent: makeProxyAgent(proxyUrl) };
        } catch (error) {
          this.localProxy.enabled = false;
        }
      } else {
        options = {
          agent: makeProxyAgent({
            host: this.localProxy.host,
            port: this.localProxy.port,
            protocol: this.localProxy.protocol,
            username: this.localProxy.username,
            password: this.localProxy.password,
          }),
          fetchAgent: makeProxyAgent({
            host: this.localProxy.host,
            port: this.localProxy.port,
            protocol: this.localProxy.protocol,
            username: this.localProxy.username,
            password: this.localProxy.password,
          }),
        };
      }
    }

    const socketConfig: UserFacingSocketConfig = {
      ...options,
      version,
      logger: P({ level: this.logBaileys }),
      printQRInTerminal: false,
      auth: {
        creds: this.instance.authState.state.creds,
        keys: makeCacheableSignalKeyStore(this.instance.authState.state.keys, P({ level: 'error' }) as any),
      },
      msgRetryCounterCache: this.msgRetryCounterCache,
      generateHighQualityLinkPreview: true,
      getMessage: async (key) => (await this.getMessage(key)) as Promise<proto.IMessage>,
      ...browserOptions,
      markOnlineOnConnect: false, // FORÇADO: Bot nunca deve aparecer como online
      retryRequestDelayMs: 1000, // AUMENTADO: 350ms -> 1000ms para maior estabilidade
      maxMsgRetryCount: 6, // AUMENTADO: 4 -> 6 tentativas para maior confiabilidade
      fireInitQueries: false, // DESABILITADO: Evita sincronizações automáticas que bloqueiam mensagens em tempo real
      connectTimeoutMs: 60_000, // AUMENTADO: 30s -> 60s para redes lentas
      keepAliveIntervalMs: 25_000, // REDUZIDO: 30s -> 25s para detectar desconexões mais rápido
      qrTimeout: 60_000, // AUMENTADO: 45s -> 60s para dar mais tempo ao usuário
      emitOwnEvents: false,
      defaultQueryTimeoutMs: 60_000, // ADICIONADO: timeout para queries
      shouldIgnoreJid: (jid) => {
        if (this.localSettings.syncFullHistory && isJidGroup(jid)) {
          return false;
        }

        const isGroupJid = this.localSettings.groupsIgnore && isJidGroup(jid);
        const isBroadcast = !this.localSettings.readStatus && isJidBroadcast(jid);
        const isNewsletter = isJidNewsletter(jid);

        return isGroupJid || isBroadcast || isNewsletter;
      },
      syncFullHistory: this.localSettings.syncFullHistory,
      shouldSyncHistoryMessage: (msg: proto.Message.IHistorySyncNotification) => {
        return this.historySyncNotification(msg);
      },
      cachedGroupMetadata: this.getGroupMetadataCache,
      userDevicesCache: this.userDevicesCache,
      transactionOpts: { maxCommitRetries: 5, delayBetweenTriesMs: 2000 }, // REDUZIDO: 10->5 retries, 3s->2s delay
      patchMessageBeforeSending(message) {
        if (
          message.deviceSentMessage?.message?.listMessage?.listType === proto.Message.ListMessage.ListType.PRODUCT_LIST
        ) {
          message = JSON.parse(JSON.stringify(message));

          message.deviceSentMessage.message.listMessage.listType = proto.Message.ListMessage.ListType.SINGLE_SELECT;
        }

        if (message.listMessage?.listType == proto.Message.ListMessage.ListType.PRODUCT_LIST) {
          message = JSON.parse(JSON.stringify(message));

          message.listMessage.listType = proto.Message.ListMessage.ListType.SINGLE_SELECT;
        }

        return message;
      },
    };

    this.endSession = false;
    this.isClientReady = false;

    // Clean up old client before creating new one with robust error handling
    if (this.client) {
      this.logger.info('Cleaning up old client before creating new connection...');

      try {
        // Step 1: Stop background intervals
        this.stopSettingsCacheRefresh();
        this.stopStreamHealthCheck();

        // Step 1.6: Clear any pending clientReady timeout
        if (this.clientReadyTimeout) {
          clearTimeout(this.clientReadyTimeout);
          this.clientReadyTimeout = null;
        }

        // Step 2: Remove event listeners
        this.cleanupEventListeners();

        // Step 3: Force WebSocket closure if still open
        if (this.client.ws) {
          const wsState = (this.client.ws as any).readyState;
          this.logger.verbose(`WebSocket state before cleanup: ${wsState} (0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED)`);

          if (wsState !== 3) {
            // 3 = CLOSED
            try {
              // Remove all listeners before closing to prevent events during shutdown
              this.client.ws.removeAllListeners();
              this.client.ws.close();
              this.logger.verbose('WebSocket closed and listeners removed');
            } catch (wsError) {
              this.logger.warn(`Error closing WebSocket: ${wsError.message}`);
            }
          }
        }

        // Step 4: End client connection properly
        this.client.end(new Error('Reconnecting - cleaning old client'));
        this.logger.verbose('Client ended');

        // Step 5: Small delay to ensure cleanup completes
        await delay(500);

        // Step 6: Destroy and recreate message processor to reset RxJS streams
        this.messageProcessor.onDestroy();
        this.messageProcessor = new BaileysMessageProcessor();
        this.messageProcessor.mount({
          onMessageReceive: this.messageHandle['messages.upsert'].bind(this),
        });
        this.logger.verbose('Message processor reset');

        // Step 6.5: Clear message retry counter cache to prevent "Key used already" errors
        // This is CRITICAL: after reconnection, WhatsApp server resets message counters
        // but our cache still has old counters, causing MessageCounterError
        this.msgRetryCounterCache.flushAll();
        this.logger.verbose('Message retry counter cache cleared');

        // Step 7: Clear client reference
        this.client = null;

        this.logger.info('Old client cleanup completed successfully');
      } catch (error) {
        this.logger.error('Error during client cleanup (continuing anyway):');
        this.logger.error(error);
        // Force clear the client even on error
        this.client = null;
      }

      // Additional delay to ensure complete cleanup
      await delay(500);
    }

    this.logger.info('Creating new WhatsApp client...');
    this.client = makeWASocket(socketConfig);

    if (this.localSettings.wavoipToken && this.localSettings.wavoipToken.length > 0) {
      useVoiceCallsBaileys(this.localSettings.wavoipToken, this.client, this.connectionStatus.state as any, true);
    }

    // Setup event handler FIRST
    this.eventHandler();

    // Setup WebSocket event listeners with error handling
    this.setupWebSocketListeners();

    // Start background settings cache refresh (critical for high-volume)
    await this.startSettingsCacheRefresh();

    // Start background stream health check (prevents message loss)
    this.startStreamHealthCheck();

    this.phoneNumber = number;

    return this.client;
  }

  /**
   * Configura listeners de WebSocket com tratamento de erros robusto
   * Previne memory leaks e garante limpeza adequada
   */
  private setupWebSocketListeners() {
    if (!this.client || !this.client.ws) {
      this.logger.warn('Cannot setup WebSocket listeners: client or ws not available');
      return;
    }

    try {
      // Store callback references for later cleanup
      this.wsCallListener = (packet) => {
        console.log('CB:call', packet);
        const payload = { event: 'CB:call', packet: packet };
        this.sendDataWebhook(Events.CALL, payload, true, ['websocket']);
      };
      this.client.ws.on('CB:call', this.wsCallListener);

      this.wsCallAckListener = (packet) => {
        console.log('CB:ack,class:call', packet);
        const payload = { event: 'CB:ack,class:call', packet: packet };
        this.sendDataWebhook(Events.CALL, payload, true, ['websocket']);
      };
      this.client.ws.on('CB:ack,class:call', this.wsCallAckListener);

      // CORREÇÃO: Armazenar referências para permitir limpeza
      this.wsErrorListener = (error) => {
        this.logger.error(`WebSocket error: ${error.message}`);
        this.logger.error(error);
      };
      this.client.ws.on('error', this.wsErrorListener);

      this.wsCloseListener = (code, reason) => {
        this.logger.warn(`WebSocket closed: code=${code}, reason=${reason?.toString()}`);
        // O baileys já gerencia reconexão, apenas logar
      };
      this.client.ws.on('close', this.wsCloseListener);

      this.wsPongListener = () => {
        this.logger.verbose('WebSocket pong received (connection alive)');
      };
      this.client.ws.on('pong', this.wsPongListener);

      this.logger.verbose('WebSocket listeners configured successfully');
    } catch (error) {
      this.logger.error(`Error setting up WebSocket listeners: ${error.message}`);
    }
  }

  public async connectToWhatsapp(number?: string): Promise<WASocket> {
    try {
      this.loadChatwoot();
      this.loadSettings();
      this.loadWebhook();
      this.loadProxy();

      return await this.createClient(number);
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString());
    }
  }

  public async reloadConnection(): Promise<WASocket> {
    try {
      return await this.createClient(this.phoneNumber);
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString());
    }
  }

  private readonly chatHandle = {
    'chats.upsert': async (chats: Chat[]) => {
      // Resolve LIDs em todos os chats ANTES de processar
      await Promise.all(chats.map((chat) => this.resolveLIDsInChat(chat)));

      const existingChatIds = await this.prismaRepository.chat.findMany({
        where: { instanceId: this.instanceId },
        select: { remoteJid: true },
      });

      const existingChatIdSet = new Set(existingChatIds.map((chat) => chat.remoteJid));

      const chatsToInsert = chats
        .filter((chat) => !existingChatIdSet?.has(chat.id))
        .map((chat) => ({
          remoteJid: chat.id,
          instanceId: this.instanceId,
          name: chat.name,
          unreadMessages: chat.unreadCount !== undefined ? chat.unreadCount : 0,
        }));

      this.sendDataWebhook(Events.CHATS_UPSERT, chatsToInsert);

      if (chatsToInsert.length > 0) {
        if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS)
          await this.prismaRepository.chat.createMany({ data: chatsToInsert, skipDuplicates: true });
      }
    },

    'chats.update': async (
      chats: Partial<
        proto.IConversation & { lastMessageRecvTimestamp?: number } & {
          conditional: (bufferedData: BufferedEventData) => boolean;
        }
      >[],
    ) => {
      // Resolve LIDs em todos os chats ANTES de processar
      await Promise.all(chats.map((chat) => this.resolveLIDsInChat(chat as Chat)));

      const chatsRaw = chats.map((chat) => {
        return { remoteJid: chat.id, instanceId: this.instanceId };
      });

      this.sendDataWebhook(Events.CHATS_UPDATE, chatsRaw);

      for (const chat of chats) {
        await this.prismaRepository.chat.updateMany({
          where: { instanceId: this.instanceId, remoteJid: chat.id, name: chat.name },
          data: { remoteJid: chat.id },
        });
      }
    },

    'chats.delete': async (chats: string[]) => {
      chats.forEach(
        async (chat) =>
          await this.prismaRepository.chat.deleteMany({ where: { instanceId: this.instanceId, remoteJid: chat } }),
      );

      this.sendDataWebhook(Events.CHATS_DELETE, [...chats]);
    },
  };

  private readonly contactHandle = {
    'contacts.upsert': async (contacts: Contact[]) => {
      try {
        // Resolve LIDs em todos os contatos ANTES de processar
        await Promise.all(contacts.map((contact) => this.resolveLIDsInContact(contact)));

        const contactsRaw: any = contacts.map((contact) => ({
          remoteJid: contact.id,
          pushName: contact?.name || contact?.verifiedName || contact.id.split('@')[0],
          profilePicUrl: null,
          instanceId: this.instanceId,
        }));

        if (contactsRaw.length > 0) {
          this.sendDataWebhook(Events.CONTACTS_UPSERT, contactsRaw);

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS)
            await this.prismaRepository.contact.createMany({ data: contactsRaw, skipDuplicates: true });

          const usersContacts = contactsRaw.filter((c) => c.remoteJid.includes('@s.whatsapp'));
          if (usersContacts) {
            await saveOnWhatsappCache(usersContacts.map((c) => ({ remoteJid: c.remoteJid })));
          }
        }

        if (
          this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
          this.localChatwoot?.enabled &&
          this.localChatwoot.importContacts &&
          contactsRaw.length
        ) {
          this.chatwootService.addHistoryContacts(
            { instanceName: this.instance.name, instanceId: this.instance.id },
            contactsRaw,
          );
          chatwootImport.importHistoryContacts(
            { instanceName: this.instance.name, instanceId: this.instance.id },
            this.localChatwoot,
          );
        }

        const updatedContacts = await Promise.all(
          contacts.map(async (contact) => ({
            remoteJid: contact.id,
            pushName: contact?.name || contact?.verifiedName || contact.id.split('@')[0],
            profilePicUrl: (await this.profilePicture(contact.id)).profilePictureUrl,
            instanceId: this.instanceId,
          })),
        );

        if (updatedContacts.length > 0) {
          const usersContacts = updatedContacts.filter((c) => c.remoteJid.includes('@s.whatsapp'));
          if (usersContacts) {
            await saveOnWhatsappCache(usersContacts.map((c) => ({ remoteJid: c.remoteJid })));
          }

          this.sendDataWebhook(Events.CONTACTS_UPDATE, updatedContacts);
          await Promise.all(
            updatedContacts.map(async (contact) => {
              const update = this.prismaRepository.contact.updateMany({
                where: { remoteJid: contact.remoteJid, instanceId: this.instanceId },
                data: { profilePicUrl: contact.profilePicUrl },
              });

              if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
                const instance = { instanceName: this.instance.name, instanceId: this.instance.id };

                const findParticipant = await this.chatwootService.findContact(
                  instance,
                  contact.remoteJid.split('@')[0],
                );

                if (!findParticipant) {
                  return;
                }

                this.chatwootService.updateContact(instance, findParticipant.id, {
                  name: contact.pushName,
                  avatar_url: contact.profilePicUrl,
                });
              }

              return update;
            }),
          );
        }
      } catch (error) {
        console.error(error);
        this.logger.error(`Error: ${error.message}`);
      }
    },

    'contacts.update': async (contacts: Partial<Contact>[]) => {
      // Resolve LIDs em todos os contatos ANTES de processar
      await Promise.all(contacts.map((contact) => this.resolveLIDsInContact(contact)));

      const contactsRaw: { remoteJid: string; pushName?: string; profilePicUrl?: string; instanceId: string }[] = [];
      for await (const contact of contacts) {
        contactsRaw.push({
          remoteJid: contact.id,
          pushName: contact?.name ?? contact?.verifiedName,
          profilePicUrl: (await this.profilePicture(contact.id)).profilePictureUrl,
          instanceId: this.instanceId,
        });
      }

      this.sendDataWebhook(Events.CONTACTS_UPDATE, contactsRaw);

      const updateTransactions = contactsRaw.map((contact) =>
        this.prismaRepository.contact.upsert({
          where: { remoteJid_instanceId: { remoteJid: contact.remoteJid, instanceId: contact.instanceId } },
          create: contact,
          update: contact,
        }),
      );
      await this.prismaRepository.$transaction(updateTransactions);

      const usersContacts = contactsRaw.filter((c) => c.remoteJid.includes('@s.whatsapp'));
      if (usersContacts) {
        await saveOnWhatsappCache(usersContacts.map((c) => ({ remoteJid: c.remoteJid })));
      }
    },
  };

  private readonly messageHandle = {
    'messaging-history.set': async ({
      messages,
      chats,
      contacts,
      isLatest,
      progress,
      syncType,
    }: {
      chats: Chat[];
      contacts: Contact[];
      messages: WAMessage[];
      isLatest?: boolean;
      progress?: number;
      syncType?: proto.HistorySync.HistorySyncType;
    }) => {
      try {
        if (syncType === proto.HistorySync.HistorySyncType.ON_DEMAND) {
          console.log('received on-demand history sync, messages=', messages);
        }
        console.log(
          `recv ${chats.length} chats, ${contacts.length} contacts, ${messages.length} msgs (is latest: ${isLatest}, progress: ${progress}%), type: ${syncType}`,
        );

        const instance: InstanceDto = { instanceName: this.instance.name };

        let timestampLimitToImport = null;

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
          const daysLimitToImport = this.localChatwoot?.enabled ? this.localChatwoot.daysLimitImportMessages : 1000;

          const date = new Date();
          timestampLimitToImport = new Date(date.setDate(date.getDate() - daysLimitToImport)).getTime() / 1000;

          const maxBatchTimestamp = Math.max(...messages.map((message) => message.messageTimestamp as number));

          const processBatch = maxBatchTimestamp >= timestampLimitToImport;

          if (!processBatch) {
            return;
          }
        }

        const contactsMap = new Map();

        for (const contact of contacts) {
          if (contact.id && (contact.notify || contact.name)) {
            contactsMap.set(contact.id, { name: contact.name ?? contact.notify, jid: contact.id });
          }
        }

        const chatsRaw: { remoteJid: string; instanceId: string; name?: string }[] = [];
        const chatsRepository = new Set(
          (await this.prismaRepository.chat.findMany({ where: { instanceId: this.instanceId } })).map(
            (chat) => chat.remoteJid,
          ),
        );

        for (const chat of chats) {
          if (chatsRepository?.has(chat.id)) {
            continue;
          }

          chatsRaw.push({ remoteJid: chat.id, instanceId: this.instanceId, name: chat.name });
        }

        this.sendDataWebhook(Events.CHATS_SET, chatsRaw);

        if (this.configService.get<Database>('DATABASE').SAVE_DATA.HISTORIC) {
          await this.prismaRepository.chat.createMany({ data: chatsRaw, skipDuplicates: true });
        }

        const messagesRaw: any[] = [];
        const messagesForEvent: any[] = []; // Collect all messages for webhook event

        const messagesRepository: Set<string> = new Set(
          chatwootImport.getRepositoryMessagesCache(instance) ??
            (
              await this.prismaRepository.message.findMany({
                select: { key: true },
                where: { instanceId: this.instanceId },
              })
            ).map((message) => {
              const key = message.key as { id: string };

              return key.id;
            }),
        );

        if (chatwootImport.getRepositoryMessagesCache(instance) === null) {
          chatwootImport.setRepositoryMessagesCache(instance, messagesRepository);
        }

        let skippedNoData = 0;
        let skippedChatwootDate = 0;
        let skippedDuplicate = 0;

        for (const m of messages) {
          if (!m.message || !m.key || !m.messageTimestamp) {
            skippedNoData++;
            continue;
          }

          // LID handling is now done automatically by Baileys 7.0
          // if (m.key.remoteJid?.includes('@lid')) {
          //   // Use remoteJidAlt if needed
          // }

          if (Long.isLong(m?.messageTimestamp)) {
            m.messageTimestamp = m.messageTimestamp?.toNumber();
          }

          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
            if (m.messageTimestamp <= timestampLimitToImport) {
              skippedChatwootDate++;
              continue;
            }
          }

          const isMessageInRepository = messagesRepository?.has(m.key.id);

          if (!m.pushName && !m.key.fromMe) {
            const participantJid = m.participant || m.key.participant || m.key.remoteJid;
            if (participantJid && contactsMap.has(participantJid)) {
              m.pushName = contactsMap.get(participantJid).name;
            } else if (participantJid) {
              m.pushName = participantJid.split('@')[0];
            }
          }

          const preparedMessage = this.prepareMessage(m);

          // Always add to event array (for webhook)
          messagesForEvent.push(preparedMessage);

          // Only add to messagesRaw if it's a new message (for database save)
          if (!isMessageInRepository) {
            messagesRaw.push(preparedMessage);
          } else {
            skippedDuplicate++;
          }
        }

        console.log(
          `Message sync stats: Total=${messages.length}, Event=${messagesForEvent.length}, New=${messagesRaw.length}, ` +
          `Skipped: NoData=${skippedNoData}, ChatwootDate=${skippedChatwootDate}, Duplicate=${skippedDuplicate}`
        );

        // Send webhook event with all messages (including duplicates)
        if (messagesForEvent.length > 0) {
          this.sendDataWebhook(Events.MESSAGES_SET, [...messagesForEvent]);
        }

        if (this.configService.get<Database>('DATABASE').SAVE_DATA.HISTORIC && messagesRaw.length > 0) {
          await this.prismaRepository.message.createMany({ data: messagesRaw, skipDuplicates: true });
        }

        if (
          this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
          this.localChatwoot?.enabled &&
          this.localChatwoot.importMessages &&
          messagesRaw.length > 0
        ) {
          this.chatwootService.addHistoryMessages(
            instance,
            messagesRaw.filter((msg) => !chatwootImport.isIgnorePhoneNumber(msg.key?.remoteJid)),
          );
        }

        await this.contactHandle['contacts.upsert'](
          contacts.filter((c) => !!c.notify || !!c.name).map((c) => ({ id: c.id, name: c.name ?? c.notify })),
        );

        contacts = undefined;
        messages = undefined;
        chats = undefined;
      } catch (error) {
        this.logger.error(error);
      }
    },

    'messages.upsert': async (
      { messages, type, requestId }: { messages: WAMessage[]; type: MessageUpsertType; requestId?: string },
      settings: any,
    ) => {
      try {
        // Mark activity when receiving messages
        this.markActivity();

        // Log message type for diagnostics
        this.logger.verbose(`messages.upsert received: type=${type}, count=${messages.length}, requestId=${requestId}`);

        // Only process 'notify' and 'append' types (real-time messages)
        // Note: History sync comes through 'messaging-history.set' event, not 'messages.upsert'
        // HOWEVER: In some Baileys versions, fetchMessageHistory might send messages through messages.upsert
        if (type !== 'notify' && type !== 'append') {
          this.logger.warn(`Ignoring messages with type: ${type}, count=${messages.length}. First message remoteJid: ${messages[0]?.key?.remoteJid}`);
          return;
        }

        for (const received of messages) {
          // Resolve LIDs ANTES de qualquer processamento (usando cache Redis + timeout)
          await this.resolveLIDsInMessage(received);

          // LID handling is now done automatically by Baileys 7.0
          // if (received.key.remoteJid?.includes('@lid')) {
          //   // Use remoteJidAlt if needed
          // }

          // Handle messageStubType 2 (Message absent from node) - cache for retry
          if ((received as any)?.messageStubType === 2 && received.key?.id) {
            this.logger.warn(`Message with stubType 2 cached for retry: ${received.key.id}`);
            this.pruneMapCache(this.messageStubRetryCache, this.MAX_MESSAGE_STUB_RETRY_CACHE);
            this.messageStubRetryCache.set(received.key.id, received);
            continue;
          }

          if (
            received?.messageStubParameters?.some?.((param) =>
              [
                'No matching sessions found for message',
                'Bad MAC',
                'failed to decrypt message',
                'SessionError',
                'Invalid PreKey ID',
                'No session record',
                'No session found to decrypt message',
              ].some((err) => param?.includes?.(err)),
            )
          ) {
            this.logger.warn(`Message ignored with messageStubParameters: ${JSON.stringify(received, null, 2)}`);

            // Auto-handle session errors
            const jid = received?.key?.remoteJid;
            if (jid) {
              const errorMsg = received.messageStubParameters?.join(', ') || 'Unknown session error';

              // Check if it's a critical session error that should trigger auto-clear
              const isCriticalError = received.messageStubParameters?.some?.((param) =>
                ['Over 2000 messages', 'failed to decrypt message', 'SessionError'].some((err) => param?.includes?.(err)),
              );

              if (isCriticalError) {
                await this.handleSessionError(jid, errorMsg);
              }
            }

            continue;
          }
          if (received.message?.conversation || received.message?.extendedTextMessage?.text) {
            const text = received.message?.conversation || received.message?.extendedTextMessage?.text;

            if (text == 'requestPlaceholder' && !requestId) {
              const messageId = await this.client.requestPlaceholderResend(received.key);

              console.log('requested placeholder resync, id=', messageId);
            } else if (requestId) {
              console.log('Message received from phone, id=', requestId, received);
            }

            if (text == 'onDemandHistSync') {
              const messageId = await this.client.fetchMessageHistory(50, received.key, received.messageTimestamp!);
              console.log('requested on-demand sync, id=', messageId);
            }
          }

          const editedMessage =
            received?.message?.protocolMessage || received?.message?.editedMessage?.message?.protocolMessage;

          if (editedMessage) {
            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled)
              this.chatwootService.eventWhatsapp(
                'messages.edit',
                { instanceName: this.instance.name, instanceId: this.instance.id },
                editedMessage,
              );

            await this.sendDataWebhook(Events.MESSAGES_EDITED, editedMessage);
            const oldMessage = await this.getMessage(editedMessage.key, true);
            if ((oldMessage as any)?.id) {
              const editedMessageTimestamp = Long.isLong(editedMessage?.timestampMs)
                ? Math.floor(editedMessage.timestampMs.toNumber() / 1000)
                : Math.floor((editedMessage.timestampMs as number) / 1000);

              await this.prismaRepository.message.update({
                where: { id: (oldMessage as any).id },
                data: {
                  message: editedMessage.editedMessage as any,
                  messageTimestamp: editedMessageTimestamp,
                  status: 'EDITED',
                },
              });
              await this.prismaRepository.messageUpdate.create({
                data: {
                  fromMe: editedMessage.key.fromMe,
                  keyId: editedMessage.key.id,
                  remoteJid: editedMessage.key.remoteJid,
                  status: 'EDITED',
                  instanceId: this.instanceId,
                  messageId: (oldMessage as any).id,
                },
              });
            }
          }

          // Removed duplicate message check - allowing all messages to be processed
          // const messageKey = `${this.instance.id}_${received.key.id}`;
          // const cached = await this.baileysCache.get(messageKey);
          // if (cached && !editedMessage) {
          //   this.logger.info(`Message duplicated ignored: ${received.key.id}`);
          //   continue;
          // }
          // await this.baileysCache.set(messageKey, true, 5 * 60);

          // Skip edited messages (already processed above), poll updates, and messages without content
          // Note: Type check (notify/append) is redundant here as we already filtered at the top
          if (editedMessage || received.message?.pollUpdateMessage || !received?.message) {
            continue;
          }

          if (Long.isLong(received.messageTimestamp)) {
            received.messageTimestamp = received.messageTimestamp?.toNumber();
          }

          if (settings?.groupsIgnore && received.key.remoteJid.includes('@g.us')) {
            continue;
          }

          // Normaliza o remoteJid para evitar duplicação
          const normalizedRemoteJid = this.normalizeRemoteJid(received.key) || received.key.remoteJid;

          const existingChat = await this.prismaRepository.chat.findFirst({
            where: { instanceId: this.instanceId, remoteJid: normalizedRemoteJid },
            select: { id: true, name: true },
          });

          if (
            existingChat &&
            received.pushName &&
            existingChat.name !== received.pushName &&
            received.pushName.trim().length > 0 &&
            !received.key.fromMe &&
            !received.key.remoteJid.includes('@g.us')
          ) {
            this.sendDataWebhook(Events.CHATS_UPSERT, [{ ...existingChat, name: received.pushName }]);
            if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS) {
              try {
                await this.prismaRepository.chat.update({
                  where: { id: existingChat.id },
                  data: { name: received.pushName },
                });
              } catch (error) {
                console.log(`Chat insert record ignored: ${received.key.remoteJid} - ${this.instanceId}`);
              }
            }
          }

          const messageRaw = this.prepareMessage(received);

          const isMedia =
            received?.message?.imageMessage ||
            received?.message?.videoMessage ||
            received?.message?.stickerMessage ||
            received?.message?.documentMessage ||
            received?.message?.documentWithCaptionMessage ||
            received?.message?.ptvMessage ||
            received?.message?.audioMessage;

          if (this.localSettings.readMessages && received.key.id !== 'status@broadcast') {
            await this.client.readMessages([received.key]);
          }

          if (this.localSettings.readStatus && received.key.id === 'status@broadcast') {
            await this.client.readMessages([received.key]);
          }

          if (
            this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
            this.localChatwoot?.enabled &&
            !received.key.id.includes('@broadcast')
          ) {
            const chatwootSentMessage = await this.chatwootService.eventWhatsapp(
              Events.MESSAGES_UPSERT,
              { instanceName: this.instance.name, instanceId: this.instanceId },
              messageRaw,
            );

            if (chatwootSentMessage?.id) {
              messageRaw.chatwootMessageId = chatwootSentMessage.id;
              messageRaw.chatwootInboxId = chatwootSentMessage.inbox_id;
              messageRaw.chatwootConversationId = chatwootSentMessage.conversation_id;
            }
          }

          if (this.configService.get<Openai>('OPENAI').ENABLED && received?.message?.audioMessage) {
            const openAiDefaultSettings = await this.prismaRepository.openaiSetting.findFirst({
              where: { instanceId: this.instanceId },
              include: { OpenaiCreds: true },
            });

            if (openAiDefaultSettings && openAiDefaultSettings.openaiCredsId && openAiDefaultSettings.speechToText) {
              messageRaw.message.speechToText = `[audio] ${await this.openaiService.speechToText(received, this)}`;
            }
          }

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
            const msg = await this.prismaRepository.message.create({ data: messageRaw });

            // IMPORTANTE: Usar o remoteJid normalizado que foi salvo, não o original
            const { remoteJid } = messageRaw.key; // ✅ Pega do messageRaw (normalizado)
            const timestamp = msg.messageTimestamp;

            // Removed duplicate read message check - allowing all read messages to be processed
            // const fromMe = received.key.fromMe.toString();
            // const messageKey = `${remoteJid}_${timestamp}_${fromMe}`;
            // const cachedTimestamp = await this.baileysCache.get(messageKey);

            // if (!cachedTimestamp) {
            if (!received.key.fromMe) {
              if (msg.status === status[3]) {
                this.logger.log(`Update not read messages ${remoteJid}`);
                await this.updateChatUnreadMessages(remoteJid);
              } else if (msg.status === status[4]) {
                this.logger.log(`Update readed messages ${remoteJid} - ${timestamp}`);
                await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
              }
            } else {
              // is send message by me
              this.logger.log(`Update readed messages ${remoteJid} - ${timestamp}`);
              await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
            }

            // await this.baileysCache.set(messageKey, true, 5 * 60);
            // } else {
            //   this.logger.info(`Update readed messages duplicated ignored [avoid deadlock]: ${messageKey}`);
            // }

            if (isMedia) {
              if (this.configService.get<S3>('S3').ENABLE) {
                try {
                  const message: any = received;

                  // Verificação adicional para garantir que há conteúdo de mídia real
                  const hasRealMedia = this.hasValidMediaContent(message);

                  if (!hasRealMedia) {
                    this.logger.warn('Message detected as media but contains no valid media content');
                  } else {
                    const media = await this.getBase64FromMediaMessage({ message }, true);

                    const { buffer, mediaType, fileName, size } = media;
                    const mimetype = mimeTypes.lookup(fileName).toString();
                    const fullName = join(
                      `${this.instance.id}`,
                      received.key.remoteJid,
                      mediaType,
                      `${Date.now()}_${fileName}`,
                    );
                    await s3Service.uploadFile(fullName, buffer, size.fileLength?.low, { 'Content-Type': mimetype });

                    await this.prismaRepository.media.create({
                      data: {
                        messageId: msg.id,
                        instanceId: this.instanceId,
                        type: mediaType,
                        fileName: fullName,
                        mimetype,
                      },
                    });

                    const mediaUrl = await s3Service.getObjectUrl(fullName);

                    messageRaw.message.mediaUrl = mediaUrl;

                    await this.prismaRepository.message.update({ where: { id: msg.id }, data: messageRaw });
                  }
                } catch (error) {
                  this.logger.error(['Error on upload file to minio', error?.message, error?.stack]);
                }
              }
            }
          }

          if (this.localWebhook.enabled) {
            if (isMedia && this.localWebhook.webhookBase64) {
              try {
                const buffer = await downloadMediaMessage(
                  { key: received.key, message: received?.message },
                  'buffer',
                  {},
                  { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
                );

                if (buffer) {
                  messageRaw.message.base64 = buffer.toString('base64');
                } else {
                  // retry to download media
                  const buffer = await downloadMediaMessage(
                    { key: received.key, message: received?.message },
                    'buffer',
                    {},
                    { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
                  );

                  if (buffer) {
                    messageRaw.message.base64 = buffer.toString('base64');
                  }
                }
              } catch (error) {
                this.logger.error(['Error converting media to base64', error?.message]);
              }
            }
          }

          this.logger.log(messageRaw);

          this.sendDataWebhook(Events.MESSAGES_UPSERT, messageRaw);

          await chatbotController.emit({
            instance: { instanceName: this.instance.name, instanceId: this.instanceId },
            remoteJid: messageRaw.key.remoteJid,
            msg: messageRaw,
            pushName: messageRaw.pushName,
          });

          // Normaliza o remoteJid para evitar duplicação de contatos
          const normalizedContactJid = this.normalizeRemoteJid(received.key) || received.key.remoteJid;

          const contact = await this.prismaRepository.contact.findFirst({
            where: { remoteJid: normalizedContactJid, instanceId: this.instanceId },
          });

          const contactRaw: { remoteJid: string; pushName: string; profilePicUrl?: string; instanceId: string } = {
            remoteJid: normalizedContactJid,
            pushName: received.key.fromMe ? '' : received.key.fromMe == null ? '' : received.pushName,
            profilePicUrl: (await this.profilePicture(normalizedContactJid)).profilePictureUrl,
            instanceId: this.instanceId,
          };

          if (contactRaw.remoteJid === 'status@broadcast') {
            continue;
          }

          if (contact) {
            this.sendDataWebhook(Events.CONTACTS_UPDATE, contactRaw);

            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
              await this.chatwootService.eventWhatsapp(
                Events.CONTACTS_UPDATE,
                { instanceName: this.instance.name, instanceId: this.instanceId },
                contactRaw,
              );
            }

            if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS)
              await this.prismaRepository.contact.upsert({
                where: { remoteJid_instanceId: { remoteJid: contactRaw.remoteJid, instanceId: contactRaw.instanceId } },
                create: contactRaw,
                update: contactRaw,
              });

            continue;
          }

          this.sendDataWebhook(Events.CONTACTS_UPSERT, contactRaw);

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS)
            await this.prismaRepository.contact.upsert({
              where: { remoteJid_instanceId: { remoteJid: contactRaw.remoteJid, instanceId: contactRaw.instanceId } },
              update: contactRaw,
              create: contactRaw,
            });

          if (contactRaw.remoteJid.includes('@s.whatsapp')) {
            await saveOnWhatsappCache([{ remoteJid: contactRaw.remoteJid }]);
          }
        }
      } catch (error) {
        // Log detalhado para debugging de perda de mensagens
        this.logger.error(
          `[messages.upsert] CRITICAL ERROR - Message processing failed: ${error.message}\n` +
            `Stack: ${error.stack}\n` +
            `InstanceId: ${this.instanceId}\n` +
            `Timestamp: ${new Date().toISOString()}`,
        );

        // FAILSAFE: Tentar enviar evento com dados originais mesmo que tenha dado erro
        try {
          if (messages && messages.length > 0) {
            this.logger.warn(`[messages.upsert] FAILSAFE: Sending ${messages.length} events with original data`);

            for (const msg of messages) {
              try {
                // Prepara mensagem mínima com dados originais (sem normalização)
                const fallbackMessage = {
                  key: msg.key,
                  pushName: msg.pushName || 'Unknown',
                  message: msg.message || {},
                  messageType: Object.keys(msg.message || {})[0] || 'unknown',
                  messageTimestamp: msg.messageTimestamp || Date.now(),
                  instanceId: this.instanceId,
                  source: 'FAILSAFE',
                };

                // Envia evento mesmo com dados não processados
                this.sendDataWebhook(Events.MESSAGES_UPSERT, fallbackMessage);

                this.logger.info(`[messages.upsert] FAILSAFE: Event sent for message ${msg.key?.id}`);
              } catch (failsafeError) {
                this.logger.error(`[messages.upsert] FAILSAFE: Failed to send event: ${failsafeError.message}`);
              }
            }
          }
        } catch (outerError) {
          this.logger.error(`[messages.upsert] FAILSAFE: Critical failure: ${outerError.message}`);
        }

        // IMPORTANTE: Não engolir o erro silenciosamente
        // Descomentar linha abaixo em ambiente de desenvolvimento para ver erros:
        // throw error;
      }
    },

    'messages.update': async (args: { update: Partial<WAMessage>; key: WAMessageKey }[], settings: any) => {
      this.logger.log(`Update messages ${JSON.stringify(args, undefined, 2)}`);

      const readChatToUpdate: Record<string, true> = {}; // {remoteJid: true}

      for await (const { key, update } of args) {
        // Check if this message was cached due to stubType 2 and retry it
        if (key.id && this.messageStubRetryCache.has(key.id)) {
          const cachedMessage = this.messageStubRetryCache.get(key.id);
          this.messageStubRetryCache.delete(key.id);

          this.logger.info(`Retrying cached message with stubType 2: ${key.id}`);

          // Re-emit as messages.upsert to process normally
          this.client.ev.emit('messages.upsert', {
            messages: [{ ...cachedMessage, ...update } as WAMessage],
            type: 'notify',
          });
          continue;
        }

        if (settings?.groupsIgnore && key.remoteJid?.includes('@g.us')) {
          continue;
        }

        // LID handling is now done automatically by Baileys 7.0
        // if (key.remoteJid?.includes('@lid')) {
        //   // Use remoteJidAlt if needed
        // }

        // Removed duplicate update check - allowing all updates to be processed
        // const updateKey = `${this.instance.id}_${key.id}_${update.status}`;
        // const cached = await this.baileysCache.get(updateKey);
        // if (cached) {
        //   this.logger.info(`Message duplicated ignored [avoid deadlock]: ${updateKey}`);
        //   continue;
        // }
        // await this.baileysCache.set(updateKey, true, 30 * 60);

        if (status[update.status] === 'READ' && key.fromMe) {
          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
            this.chatwootService.eventWhatsapp(
              'messages.read',
              { instanceName: this.instance.name, instanceId: this.instanceId },
              { key: key },
            );
          }
        }

        if (key.remoteJid !== 'status@broadcast' && key.id !== undefined) {
          let pollUpdates: any;

          if (update.pollUpdates) {
            const pollCreation = await this.getMessage(key);

            if (pollCreation) {
              pollUpdates = getAggregateVotesInPollMessage({
                message: pollCreation as proto.IMessage,
                pollUpdates: update.pollUpdates,
              });
            }
          }

          const message: any = {
            keyId: key.id,
            remoteJid: key?.remoteJid,
            fromMe: key.fromMe,
            participant: key?.remoteJid,
            status: status[update.status] ?? 'DELETED',
            pollUpdates,
            instanceId: this.instanceId,
          };

          let findMessage: any;
          const configDatabaseData = this.configService.get<Database>('DATABASE').SAVE_DATA;
          if (configDatabaseData.HISTORIC || configDatabaseData.NEW_MESSAGE) {
            findMessage = await this.prismaRepository.message.findFirst({
              where: { instanceId: this.instanceId, key: { path: ['id'], equals: key.id } },
            });

            if (findMessage) message.messageId = findMessage.id;
          }

          if (update.message === null && update.status === undefined) {
            this.sendDataWebhook(Events.MESSAGES_DELETE, key);

            if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
              if (message.messageId) {
                await this.prismaRepository.messageUpdate.create({ 
                  data: {
                    ...message,
                    messageId: message.messageId
                  }
                });
              }
            }

            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
              this.chatwootService.eventWhatsapp(
                Events.MESSAGES_DELETE,
                { instanceName: this.instance.name, instanceId: this.instanceId },
                { key: key },
              );
            }

            continue;
          }

          if (findMessage && update.status !== undefined && status[update.status] !== findMessage.status) {
            if (!key.fromMe && key.remoteJid) {
              readChatToUpdate[key.remoteJid] = true;

              const { remoteJid } = key;
              const timestamp = findMessage.messageTimestamp;

              // Removed duplicate message update check - allowing all updates to be processed
              // const fromMe = key.fromMe.toString();
              // const messageKey = `${remoteJid}_${timestamp}_${fromMe}`;
              // const cachedTimestamp = await this.baileysCache.get(messageKey);

              // if (!cachedTimestamp) {
              if (status[update.status] === status[4]) {
                this.logger.log(`Update as read in message.update ${remoteJid} - ${timestamp}`);
                await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
                // await this.baileysCache.set(messageKey, true, 5 * 60);
              }

              await this.prismaRepository.message.update({
                where: { id: findMessage.id },
                data: { status: status[update.status] },
              });
              // } else {
              //   this.logger.info(
              //     `Update readed messages duplicated ignored in message.update [avoid deadlock]: ${messageKey}`,
              //   );
              // }
            }
          }

          this.sendDataWebhook(Events.MESSAGES_UPDATE, message);

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
            if (message.messageId) {
              await this.prismaRepository.messageUpdate.create({ 
                data: {
                  ...message,
                  messageId: message.messageId
                }
              });
            }
          }

          const existingChat = await this.prismaRepository.chat.findFirst({
            where: { instanceId: this.instanceId, remoteJid: message.remoteJid },
          });

          if (existingChat) {
            const chatToInsert = { remoteJid: message.remoteJid, instanceId: this.instanceId, unreadMessages: 0 };

            this.sendDataWebhook(Events.CHATS_UPSERT, [chatToInsert]);
            if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS) {
              try {
                await this.prismaRepository.chat.update({ where: { id: existingChat.id }, data: chatToInsert });
              } catch (error) {
                console.log(`Chat insert record ignored: ${chatToInsert.remoteJid} - ${chatToInsert.instanceId}`);
              }
            }
          }
        }
      }

      await Promise.all(Object.keys(readChatToUpdate).map((remoteJid) => this.updateChatUnreadMessages(remoteJid)));
    },
  };

  private readonly groupHandler = {
    'groups.upsert': (groupMetadata: GroupMetadata[]) => {
      this.sendDataWebhook(Events.GROUPS_UPSERT, groupMetadata);
    },

    'groups.update': (groupMetadataUpdate: Partial<GroupMetadata>[]) => {
      this.sendDataWebhook(Events.GROUPS_UPDATE, groupMetadataUpdate);

      groupMetadataUpdate.forEach((group) => {
        if (isJidGroup(group.id)) {
          this.updateGroupMetadataCache(group.id);
        }
      });
    },

    'group-participants.update': (participantsUpdate: {
      id: string;
      participants: string[];
      action: ParticipantAction;
    }) => {
      this.sendDataWebhook(Events.GROUP_PARTICIPANTS_UPDATE, participantsUpdate);

      this.updateGroupMetadataCache(participantsUpdate.id);
    },
  };

  private readonly labelHandle = {
    [Events.LABELS_EDIT]: async (label: Label) => {
      this.sendDataWebhook(Events.LABELS_EDIT, { ...label, instance: this.instance.name });

      const labelsRepository = await this.prismaRepository.label.findMany({ where: { instanceId: this.instanceId } });

      const savedLabel = labelsRepository.find((l) => l.labelId === label.id);
      if (label.deleted && savedLabel) {
        await this.prismaRepository.label.delete({
          where: { labelId_instanceId: { instanceId: this.instanceId, labelId: label.id } },
        });
        this.sendDataWebhook(Events.LABELS_EDIT, { ...label, instance: this.instance.name });
        return;
      }

      const labelName = label.name.replace(/[^\x20-\x7E]/g, '');
      if (!savedLabel || savedLabel.color !== `${label.color}` || savedLabel.name !== labelName) {
        if (this.configService.get<Database>('DATABASE').SAVE_DATA.LABELS) {
          const labelData = {
            color: `${label.color}`,
            name: labelName,
            labelId: label.id,
            predefinedId: label.predefinedId,
            instanceId: this.instanceId,
          };
          await this.prismaRepository.label.upsert({
            where: { labelId_instanceId: { instanceId: labelData.instanceId, labelId: labelData.labelId } },
            update: labelData,
            create: labelData,
          });
        }
      }
    },

    [Events.LABELS_ASSOCIATION]: async (
      data: { association: LabelAssociation; type: 'remove' | 'add' },
      database: Database,
    ) => {
      this.logger.info(
        `labels association - ${data?.association?.chatId} (${data.type}-${data?.association?.type}): ${data?.association?.labelId}`,
      );
      if (database.SAVE_DATA.CHATS) {
        const instanceId = this.instanceId;
        const chatId = data.association.chatId;
        const labelId = data.association.labelId;

        if (data.type === 'add') {
          await this.addLabel(labelId, instanceId, chatId);
        } else if (data.type === 'remove') {
          await this.removeLabel(labelId, instanceId, chatId);
        }
      }

      this.sendDataWebhook(Events.LABELS_ASSOCIATION, {
        instance: this.instance.name,
        type: data.type,
        chatId: data.association.chatId,
        labelId: data.association.labelId,
      });
    },
  };

  private eventHandler() {
    // IMPORTANT: Settings are now cached in background to NEVER block event processing
    // This prevents message loss in high-volume scenarios
    this.client.ev.process(async (events) => {
      if (!this.endSession) {
        // Debug: Log all event keys to help diagnose missing events
        const eventKeys = Object.keys(events).filter(key => events[key]);
        if (eventKeys.length > 0) {
          this.logger.verbose(`Events received: ${eventKeys.join(', ')}`);
        }

        const database = this.configService.get<Database>('DATABASE');

        // Use background-refreshed cache - NEVER await here to avoid blocking
        const settings = this.settingsCache;

        if (events.call) {
          const call = events.call[0];

          if (settings?.rejectCall && call.status == 'offer') {
            this.client.rejectCall(call.id, call.from);
          }

          if (settings?.msgCall?.trim().length > 0 && call.status == 'offer') {
            const msg = await this.client.sendMessage(call.from, { text: settings.msgCall });

            this.client.ev.emit('messages.upsert', { messages: [msg], type: 'notify' });
          }

          this.sendDataWebhook(Events.CALL, call);
        }

        if (events['connection.update']) {
          this.connectionUpdate(events['connection.update']);
        }

        if (events['creds.update']) {
          this.instance.authState.saveCreds();
        }

        if (events['messaging-history.set']) {
          const payload = events['messaging-history.set'];
          this.logger.verbose(`messaging-history.set event received: ${payload.messages?.length || 0} messages, syncType: ${payload.syncType}`);
          // Process async to not block realtime events
          this.messageHandle['messaging-history.set'](payload).catch((error) => {
            this.logger.error('Error processing messaging-history.set:');
            this.logger.error(error);
          });
        }

        if (events['messages.upsert']) {
          const payload = events['messages.upsert'];

          this.messageProcessor.processMessage(payload, settings);
          // this.messageHandle['messages.upsert'](payload, settings);
        }

        if (events['messages.update']) {
          const payload = events['messages.update'];
          this.messageHandle['messages.update'](payload, settings);
        }

        if (events['message-receipt.update']) {
          const payload = events['message-receipt.update'] as MessageUserReceiptUpdate[];
          const remotesJidMap: Record<string, number> = {};

          for (const event of payload) {
            if (typeof event.key.remoteJid === 'string' && typeof event.receipt.readTimestamp === 'number') {
              remotesJidMap[event.key.remoteJid] = event.receipt.readTimestamp;
            }
          }

          await Promise.all(
            Object.keys(remotesJidMap).map(async (remoteJid) =>
              this.updateMessagesReadedByTimestamp(remoteJid, remotesJidMap[remoteJid]),
            ),
          );
        }

        if (events['presence.update']) {
          const payload = events['presence.update'];

          if (settings?.groupsIgnore && payload.id.includes('@g.us')) {
            return;
          }

          this.sendDataWebhook(Events.PRESENCE_UPDATE, payload);
        }

        if (!settings?.groupsIgnore) {
          if (events['groups.upsert']) {
            const payload = events['groups.upsert'];
            this.groupHandler['groups.upsert'](payload);
          }

          if (events['groups.update']) {
            const payload = events['groups.update'];
            this.groupHandler['groups.update'](payload);
          }

          if (events['group-participants.update']) {
            const payload = events['group-participants.update'];
            // Map GroupParticipant[] to string[] for compatibility
            const mappedPayload = {
              ...payload,
              participants: payload.participants.map((p: GroupParticipant | string) =>
                typeof p === 'string' ? p : p.id
              ),
            };
            this.groupHandler['group-participants.update'](mappedPayload);
          }
        }

        if (events['chats.upsert']) {
          const payload = events['chats.upsert'];
          this.chatHandle['chats.upsert'](payload);
        }

        if (events['chats.update']) {
          const payload = events['chats.update'];
          this.chatHandle['chats.update'](payload);
        }

        if (events['chats.delete']) {
          const payload = events['chats.delete'];
          this.chatHandle['chats.delete'](payload);
        }

        if (events['contacts.upsert']) {
          const payload = events['contacts.upsert'];
          this.contactHandle['contacts.upsert'](payload);
        }

        if (events['contacts.update']) {
          const payload = events['contacts.update'];
          this.contactHandle['contacts.update'](payload);
        }

        if (events[Events.LABELS_ASSOCIATION]) {
          const payload = events[Events.LABELS_ASSOCIATION];
          this.labelHandle[Events.LABELS_ASSOCIATION](payload, database);
          return;
        }

        if (events[Events.LABELS_EDIT]) {
          const payload = events[Events.LABELS_EDIT];
          this.labelHandle[Events.LABELS_EDIT](payload);
          return;
        }
      }
    });
  }

  private historySyncNotification(msg: proto.Message.IHistorySyncNotification) {
    const instance: InstanceDto = { instanceName: this.instance.name };

    if (
      this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
      this.localChatwoot?.enabled &&
      this.localChatwoot.importMessages &&
      this.isSyncNotificationFromUsedSyncType(msg)
    ) {
      if (msg.chunkOrder === 1) {
        this.chatwootService.startImportHistoryMessages(instance);
      }

      if (msg.progress === 100) {
        setTimeout(() => {
          this.chatwootService.importHistoryMessages(instance);
        }, 10000);
      }
    }

    return true;
  }

  private isSyncNotificationFromUsedSyncType(msg: proto.Message.IHistorySyncNotification) {
    return (
      (this.localSettings.syncFullHistory && msg?.syncType === 2) ||
      (!this.localSettings.syncFullHistory && msg?.syncType === 3)
    );
  }

  public async profilePicture(number: string) {
    const jid = createJid(number);

    try {
      const profilePictureUrl = await this.client.profilePictureUrl(jid, 'image');

      return { wuid: jid, profilePictureUrl };
    } catch (error) {
      return { wuid: jid, profilePictureUrl: null };
    }
  }

  public async getStatus(number: string) {
    const jid = createJid(number);

    try {
      return { wuid: jid, status: (await this.client.fetchStatus(jid))[0]?.status };
    } catch (error) {
      return { wuid: jid, status: null };
    }
  }

  public async fetchProfile(instanceName: string, number?: string) {
    const jid = number ? createJid(number) : this.client?.user?.id;

    const onWhatsapp = (await this.whatsappNumber({ numbers: [jid] }))?.shift();

    if (!onWhatsapp.exists) {
      throw new BadRequestException(onWhatsapp);
    }

    try {
      if (number) {
        const info = (await this.whatsappNumber({ numbers: [jid] }))?.shift();
        const picture = await this.profilePicture(info?.jid);
        const status = await this.getStatus(info?.jid);
        const business = await this.fetchBusinessProfile(info?.jid);

        return {
          wuid: info?.jid || jid,
          name: info?.name,
          numberExists: info?.exists,
          picture: picture?.profilePictureUrl,
          status: status?.status,
          isBusiness: business.isBusiness,
          email: business?.email,
          description: business?.description,
          website: business?.website?.shift(),
        };
      } else {
        const instanceNames = instanceName ? [instanceName] : null;
        const info: Instance = await waMonitor.instanceInfo(instanceNames);
        const business = await this.fetchBusinessProfile(jid);

        return {
          wuid: jid,
          name: info?.profileName,
          numberExists: true,
          picture: info?.profilePicUrl,
          status: info?.connectionStatus,
          isBusiness: business.isBusiness,
          email: business?.email,
          description: business?.description,
          website: business?.website?.shift(),
        };
      }
    } catch (error) {
      return { wuid: jid, name: null, picture: null, status: null, os: null, isBusiness: false };
    }
  }

  public async offerCall({ number, isVideo, callDuration }: OfferCallDto) {
    const jid = createJid(number);

    try {
      // const call = await this.client.offerCall(jid, isVideo);
      // setTimeout(() => this.client.terminateCall(call.id, call.to), callDuration * 1000);

      // return call;
      return { id: '123', jid, isVideo, callDuration };
    } catch (error) {
      return error;
    }
  }

  private async sendMessage(
    sender: string,
    message: any,
    mentions: any,
    linkPreview: any,
    quoted: any,
    messageId?: string,
    ephemeralExpiration?: number,
    // participants?: GroupParticipant[],
  ) {
    sender = sender.toLowerCase();

    const option: any = { quoted };

    if (isJidGroup(sender)) {
      option.useCachedGroupMetadata = true;
      // if (participants)
      //   option.cachedGroupMetadata = async () => {
      //     return { participants: participants as GroupParticipant[] };
      //   };
    }

    if (ephemeralExpiration) option.ephemeralExpiration = ephemeralExpiration;

    if (messageId) option.messageId = messageId;
    else option.messageId = '3EB0' + randomBytes(18).toString('hex').toUpperCase();

    if (message['viewOnceMessage']) {
      const m = generateWAMessageFromContent(sender, message, {
        timestamp: new Date(),
        userJid: this.instance.wuid,
        messageId,
        quoted,
      });
      const id = await this.client.relayMessage(sender, message, { messageId });
      m.key = { id: id, remoteJid: sender, participant: (isPnUser(sender) || isLidUser(sender)) ? sender : undefined, fromMe: true };
      for (const [key, value] of Object.entries(m)) {
        if (!value || (isArray(value) && value.length) === 0) {
          delete m[key];
        }
      }
      return m;
    }

    if (
      !message['audio'] &&
      !message['poll'] &&
      !message['sticker'] &&
      !message['conversation'] &&
      sender !== 'status@broadcast'
    ) {
      if (message['reactionMessage']) {
        return await this.client.sendMessage(
          sender,
          {
            react: { text: message['reactionMessage']['text'], key: message['reactionMessage']['key'] },
          } as unknown as AnyMessageContent,
          option as unknown as MiscMessageGenerationOptions,
        );
      }
    }

    if (message['conversation']) {
      return await this.client.sendMessage(
        sender,
        { text: message['conversation'], mentions, linkPreview: linkPreview } as unknown as AnyMessageContent,
        option as unknown as MiscMessageGenerationOptions,
      );
    }

    if (!message['audio'] && !message['poll'] && !message['sticker'] && sender != 'status@broadcast') {
      return await this.client.sendMessage(
        sender,
        { forward: { key: { remoteJid: this.instance.wuid, fromMe: true }, message }, mentions },
        option as unknown as MiscMessageGenerationOptions,
      );
    }

    if (sender === 'status@broadcast') {
      let jidList;
      if (message['status'].option.allContacts) {
        const contacts = await this.prismaRepository.contact.findMany({
          where: { instanceId: this.instanceId, remoteJid: { not: { endsWith: '@g.us' } } },
        });

        jidList = contacts.map((contact) => contact.remoteJid);
      } else {
        jidList = message['status'].option.statusJidList;
      }

      const batchSize = 10;

      const batches = Array.from({ length: Math.ceil(jidList.length / batchSize) }, (_, i) =>
        jidList.slice(i * batchSize, i * batchSize + batchSize),
      );

      let msgId: string | null = null;

      let firstMessage: WAMessage;

      const firstBatch = batches.shift();

      if (firstBatch) {
        firstMessage = await this.client.sendMessage(
          sender,
          message['status'].content as unknown as AnyMessageContent,
          {
            backgroundColor: message['status'].option.backgroundColor,
            font: message['status'].option.font,
            statusJidList: firstBatch,
          } as unknown as MiscMessageGenerationOptions,
        );

        msgId = firstMessage.key.id;
      }

      if (batches.length === 0) return firstMessage;

      await Promise.allSettled(
        batches.map(async (batch) => {
          const messageSent = await this.client.sendMessage(
            sender,
            message['status'].content as unknown as AnyMessageContent,
            {
              backgroundColor: message['status'].option.backgroundColor,
              font: message['status'].option.font,
              statusJidList: batch,
              messageId: msgId,
            } as unknown as MiscMessageGenerationOptions,
          );

          return messageSent;
        }),
      );

      return firstMessage;
    }

    return await this.client.sendMessage(
      sender,
      message as unknown as AnyMessageContent,
      option as unknown as MiscMessageGenerationOptions,
    );
  }

  private async sendMessageWithTyping<T = proto.IMessage>(
    number: string,
    message: T,
    options?: Options,
    isIntegration = false,
  ) {
    const isWA = (await this.whatsappNumber({ numbers: [number] }))?.shift();

    if (!isWA.exists && !isJidGroup(isWA.jid) && !isWA.jid.includes('@broadcast')) {
      throw new BadRequestException(isWA);
    }

    const sender = isWA.jid.toLowerCase();

    this.logger.verbose(`Sending message to ${sender}`);

    try {
      if (options?.delay) {
        this.logger.verbose(`Typing for ${options.delay}ms to ${sender}`);
        if (options.delay > 20000) {
          let remainingDelay = options.delay;
          while (remainingDelay > 20000) {
            await this.client.presenceSubscribe(sender);

            await this.client.sendPresenceUpdate((options.presence as WAPresence) ?? 'composing', sender);

            await delay(20000);

            await this.client.sendPresenceUpdate('paused', sender);

            remainingDelay -= 20000;
          }
          if (remainingDelay > 0) {
            await this.client.presenceSubscribe(sender);

            await this.client.sendPresenceUpdate((options.presence as WAPresence) ?? 'composing', sender);

            await delay(remainingDelay);

            await this.client.sendPresenceUpdate('paused', sender);
          }
        } else {
          await this.client.presenceSubscribe(sender);

          await this.client.sendPresenceUpdate((options.presence as WAPresence) ?? 'composing', sender);

          await delay(options.delay);

          await this.client.sendPresenceUpdate('paused', sender);
        }
      }

      const linkPreview = options?.linkPreview != false ? undefined : false;

      let quoted: WAMessage;

      if (options?.quoted) {
        const m = options?.quoted;

        const msg = m?.message ? m : ((await this.getMessage(m.key, true)) as proto.IWebMessageInfo);

        if (msg) {
          quoted = msg as WAMessage;
        }
      }

      let messageSent: WAMessage;

      let mentions: string[];
      if (isJidGroup(sender)) {
        let group;
        try {
          const cache = this.configService.get<CacheConf>('CACHE');
          if (!cache.REDIS.ENABLED && !cache.LOCAL.ENABLED) group = await this.findGroup({ groupJid: sender }, 'inner');
          else group = await this.getGroupMetadataCache(sender);
          // group = await this.findGroup({ groupJid: sender }, 'inner');
        } catch (error) {
          throw new NotFoundException('Group not found');
        }

        if (!group) {
          throw new NotFoundException('Group not found');
        }

        if (options?.mentionsEveryOne) {
          mentions = group.participants.map((participant) => participant.id);
        } else if (options?.mentioned?.length) {
          mentions = options.mentioned.map((mention) => {
            const jid = createJid(mention);
            if (isJidGroup(jid)) {
              return null;
            }
            return jid;
          });
        }

        messageSent = await this.sendMessage(
          sender,
          message,
          mentions,
          linkPreview,
          quoted,
          null,
          group?.ephemeralDuration,
          // group?.participants,
        );
      } else {
        messageSent = await this.sendMessage(sender, message, mentions, linkPreview, quoted);
      }

      if (Long.isLong(messageSent?.messageTimestamp)) {
        messageSent.messageTimestamp = messageSent.messageTimestamp?.toNumber();
      }

      const messageRaw = this.prepareMessage(messageSent);

      const isMedia =
        messageSent?.message?.imageMessage ||
        messageSent?.message?.videoMessage ||
        messageSent?.message?.stickerMessage ||
        messageSent?.message?.ptvMessage ||
        messageSent?.message?.documentMessage ||
        messageSent?.message?.documentWithCaptionMessage ||
        messageSent?.message?.ptvMessage ||
        messageSent?.message?.audioMessage;

      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled && !isIntegration) {
        this.chatwootService.eventWhatsapp(
          Events.SEND_MESSAGE,
          { instanceName: this.instance.name, instanceId: this.instanceId },
          messageRaw,
        );
      }

      if (this.configService.get<Openai>('OPENAI').ENABLED && messageRaw?.message?.audioMessage) {
        const openAiDefaultSettings = await this.prismaRepository.openaiSetting.findFirst({
          where: { instanceId: this.instanceId },
          include: { OpenaiCreds: true },
        });

        if (openAiDefaultSettings && openAiDefaultSettings.openaiCredsId && openAiDefaultSettings.speechToText) {
          messageRaw.message.speechToText = `[audio] ${await this.openaiService.speechToText(messageRaw, this)}`;
        }
      }

      if (this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
        const msg = await this.prismaRepository.message.create({ data: messageRaw });

        if (isMedia && this.configService.get<S3>('S3').ENABLE) {
          try {
            const message: any = messageRaw;

            // Verificação adicional para garantir que há conteúdo de mídia real
            const hasRealMedia = this.hasValidMediaContent(message);

            if (!hasRealMedia) {
              this.logger.warn('Message detected as media but contains no valid media content');
            } else {
              const media = await this.getBase64FromMediaMessage({ message }, true);

              const { buffer, mediaType, fileName, size } = media;

              const mimetype = mimeTypes.lookup(fileName).toString();

              const fullName = join(
                `${this.instance.id}`,
                messageRaw.key.remoteJid,
                `${messageRaw.key.id}`,
                mediaType,
                fileName,
              );

              await s3Service.uploadFile(fullName, buffer, size.fileLength?.low, { 'Content-Type': mimetype });

              await this.prismaRepository.media.create({
                data: { messageId: msg.id, instanceId: this.instanceId, type: mediaType, fileName: fullName, mimetype },
              });

              const mediaUrl = await s3Service.getObjectUrl(fullName);

              messageRaw.message.mediaUrl = mediaUrl;

              await this.prismaRepository.message.update({ where: { id: msg.id }, data: messageRaw });
            }
          } catch (error) {
            this.logger.error(['Error on upload file to minio', error?.message, error?.stack]);
          }
        }
      }

      if (this.localWebhook.enabled) {
        if (isMedia && this.localWebhook.webhookBase64) {
          try {
            const buffer = await downloadMediaMessage(
              { key: messageRaw.key, message: messageRaw?.message },
              'buffer',
              {},
              { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
            );

            if (buffer) {
              messageRaw.message.base64 = buffer.toString('base64');
            } else {
              // retry to download media
              const buffer = await downloadMediaMessage(
                { key: messageRaw.key, message: messageRaw?.message },
                'buffer',
                {},
                { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
              );

              if (buffer) {
                messageRaw.message.base64 = buffer.toString('base64');
              }
            }
          } catch (error) {
            this.logger.error(['Error converting media to base64', error?.message]);
          }
        }
      }

      this.logger.log(messageRaw);

      this.sendDataWebhook(Events.SEND_MESSAGE, messageRaw);

      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled && isIntegration) {
        await chatbotController.emit({
          instance: { instanceName: this.instance.name, instanceId: this.instanceId },
          remoteJid: messageRaw.key.remoteJid,
          msg: messageRaw,
          pushName: messageRaw.pushName,
          isIntegration,
        });
      }

      return messageRaw;
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Instance Controller
  public async sendPresence(data: SendPresenceDto) {
    try {
      const { number } = data;

      // BLOQUEIO: Bot nunca deve aparecer como disponível/online
      if (data?.presence === 'available') {
        throw new BadRequestException('Bot cannot be marked as available/online. Use composing, recording, or paused instead.');
      }

      const isWA = (await this.whatsappNumber({ numbers: [number] }))?.shift();

      if (!isWA.exists && !isJidGroup(isWA.jid) && !isWA.jid.includes('@broadcast')) {
        throw new BadRequestException(isWA);
      }

      const sender = isWA.jid;

      if (data?.delay && data?.delay > 20000) {
        let remainingDelay = data?.delay;
        while (remainingDelay > 20000) {
          await this.client.presenceSubscribe(sender);

          await this.client.sendPresenceUpdate((data?.presence as WAPresence) ?? 'composing', sender);

          await delay(20000);

          await this.client.sendPresenceUpdate('paused', sender);

          remainingDelay -= 20000;
        }
        if (remainingDelay > 0) {
          await this.client.presenceSubscribe(sender);

          await this.client.sendPresenceUpdate((data?.presence as WAPresence) ?? 'composing', sender);

          await delay(remainingDelay);

          await this.client.sendPresenceUpdate('paused', sender);
        }
      } else {
        await this.client.presenceSubscribe(sender);

        await this.client.sendPresenceUpdate((data?.presence as WAPresence) ?? 'composing', sender);

        await delay(data?.delay);

        await this.client.sendPresenceUpdate('paused', sender);
      }

      return { presence: data.presence };
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Presence Controller
  public async setPresence(data: SetPresenceDto) {
    try {
      // BLOQUEIO: Bot nunca deve aparecer como disponível/online
      if (data?.presence === 'available') {
        throw new BadRequestException('Bot cannot be marked as available/online. Use composing, recording, paused, or unavailable instead.');
      }

      await this.client.sendPresenceUpdate(data.presence);

      return { presence: data.presence };
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Send Message Controller
  public async textMessage(data: SendTextDto, isIntegration = false) {
    const text = data.text;

    if (!text || text.trim().length === 0) {
      throw new BadRequestException('Text is required');
    }

    return await this.sendMessageWithTyping(
      data.number,
      { conversation: data.text },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        linkPreview: data?.linkPreview,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
      isIntegration,
    );
  }

  public async pollMessage(data: SendPollDto) {
    return await this.sendMessageWithTyping(
      data.number,
      { poll: { name: data.name, selectableCount: data.selectableCount, values: data.values } },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        linkPreview: data?.linkPreview,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
    );
  }

  private async formatStatusMessage(status: StatusMessage) {
    if (!status.type) {
      throw new BadRequestException('Type is required');
    }

    if (!status.content) {
      throw new BadRequestException('Content is required');
    }

    if (status.allContacts) {
      const contacts = await this.prismaRepository.contact.findMany({ where: { instanceId: this.instanceId } });

      if (!contacts.length) {
        throw new BadRequestException('Contacts not found');
      }

      status.statusJidList = contacts.filter((contact) => contact.pushName).map((contact) => contact.remoteJid);
    }

    if (!status.statusJidList?.length && !status.allContacts) {
      throw new BadRequestException('StatusJidList is required');
    }

    if (status.type === 'text') {
      if (!status.backgroundColor) {
        throw new BadRequestException('Background color is required');
      }

      if (!status.font) {
        throw new BadRequestException('Font is required');
      }

      return {
        content: { text: status.content },
        option: { backgroundColor: status.backgroundColor, font: status.font, statusJidList: status.statusJidList },
      };
    }
    if (status.type === 'image') {
      return {
        content: { image: { url: status.content }, caption: status.caption },
        option: { statusJidList: status.statusJidList },
      };
    }

    if (status.type === 'video') {
      return {
        content: { video: { url: status.content }, caption: status.caption },
        option: { statusJidList: status.statusJidList },
      };
    }

    if (status.type === 'audio') {
      const convert = await this.processAudioMp4(status.content);
      if (Buffer.isBuffer(convert)) {
        const result = {
          content: { audio: convert, ptt: true, mimetype: 'audio/ogg; codecs=opus' },
          option: { statusJidList: status.statusJidList },
        };

        return result;
      } else {
        throw new InternalServerErrorException(convert);
      }
    }

    throw new BadRequestException('Type not found');
  }

  public async statusMessage(data: SendStatusDto, file?: any) {
    const mediaData: SendStatusDto = { ...data };

    if (file) mediaData.content = file.buffer.toString('base64');

    const status = await this.formatStatusMessage(mediaData);

    const statusSent = await this.sendMessageWithTyping('status@broadcast', { status });

    return statusSent;
  }

  private async prepareMediaMessage(mediaMessage: MediaMessage) {
    const maxRetries = 3;
    const retryDelayMs = 1000;
    let lastError: any;

    // Log inicial
    this.logger.verbose(
      `Preparing media message - Type: ${mediaMessage.mediatype}, Connection ready: ${this.isConnectionReady()}`,
    );

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        // Verificação robusta do estado da conexão usando isConnectionReady()
        if (!this.isConnectionReady()) {
          this.logger.warn(
            `Connection not ready for media upload, attempt ${attempt}/${maxRetries} - State: ${this.stateConnection.state}`,
          );

          if (attempt < maxRetries) {
            // Backoff exponencial: espera mais tempo a cada tentativa
            const waitTime = retryDelayMs * attempt;
            this.logger.verbose(`Waiting ${waitTime}ms before retry...`);
            await delay(waitTime);
            continue;
          }

          throw new BadRequestException(
            `Instance is not ready to send media. Current state: ${this.stateConnection.state}. Please wait for connection to be fully established.`,
          );
        }

        // Verificação adicional: confirmar que o cliente ainda está pronto IMEDIATAMENTE antes do upload
        if (!this.client || !this.client.user) {
          throw new BadRequestException('Client not properly initialized for media upload');
        }

        const type = mediaMessage.mediatype === 'ptv' ? 'video' : mediaMessage.mediatype;

        let mediaInput: any;
        if (mediaMessage.mediatype === 'image') {
          let imageBuffer: Buffer;
          if (isURL(mediaMessage.media)) {
            let config: any = { responseType: 'arraybuffer' };

            if (this.localProxy?.enabled) {
              config = {
                ...config,
                httpsAgent: makeProxyAgent({
                  host: this.localProxy.host,
                  port: this.localProxy.port,
                  protocol: this.localProxy.protocol,
                  username: this.localProxy.username,
                  password: this.localProxy.password,
                }),
              };
            }

            const response = await axios.get(mediaMessage.media, config);
            imageBuffer = Buffer.from(response.data, 'binary');
          } else {
            imageBuffer = Buffer.from(mediaMessage.media, 'base64');
          }

          mediaInput = await sharp(imageBuffer).jpeg().toBuffer();
          mediaMessage.fileName ??= 'image.jpg';
          mediaMessage.mimetype = 'image/jpeg';
        } else {
          mediaInput = isURL(mediaMessage.media)
            ? { url: mediaMessage.media }
            : Buffer.from(mediaMessage.media, 'base64');
        }

        // Log tamanho da mídia
        const mediaSize = Buffer.isBuffer(mediaInput) ? mediaInput.length : 'URL';
        this.logger.verbose(`Media size: ${mediaSize}, attempting upload (${attempt}/${maxRetries})`);

        // Verificação imediatamente antes do upload
        if (!this.isConnectionReady()) {
          throw new BadRequestException('Connection lost before media upload could start');
        }

        const uploadStartTime = Date.now();

        // Executar upload com verificação periódica e tratamento de erro robusto
        let prepareMedia: any;
        try {
          prepareMedia = await prepareWAMessageMedia(
            {
              [type]: mediaInput,
            } as any,
            { upload: this.client.waUploadToServer },
          );
        } catch (uploadError: any) {
          // Tratar especificamente erros de arquivo temporário do Baileys
          if (uploadError?.code === 'ENOENT' && uploadError?.path?.includes('-enc')) {
            this.logger.error(`Baileys temporary file error: ${uploadError.message}`);
            this.logger.error(`Attempted path: ${uploadError.path}`);
            throw new Error('Media upload failed on all hosts');
          }
          // Re-lançar outros erros
          throw uploadError;
        }

        // Verificar se conexão ainda está ativa após upload
        if (!this.isConnectionReady()) {
          this.logger.warn('Connection lost after media upload');
          // Não lançar erro aqui pois o upload já foi concluído
        }

        const uploadDuration = Date.now() - uploadStartTime;
        this.logger.verbose(`Media upload successful on attempt ${attempt}/${maxRetries} (took ${uploadDuration}ms)`);

        const mediaType = mediaMessage.mediatype + 'Message';

        if (mediaMessage.mediatype === 'document' && !mediaMessage.fileName) {
          const regex = new RegExp(/.*\/(.+?)\./);
          const arrayMatch = regex.exec(mediaMessage.media);
          mediaMessage.fileName = arrayMatch[1];
        }

        if (mediaMessage.mediatype === 'image' && !mediaMessage.fileName) {
          mediaMessage.fileName = 'image.jpg';
        }

        if (mediaMessage.mediatype === 'video' && !mediaMessage.fileName) {
          mediaMessage.fileName = 'video.mp4';
        }

        let mimetype: string | false;

        if (mediaMessage.mimetype) {
          mimetype = mediaMessage.mimetype;
        } else {
          mimetype = mimeTypes.lookup(mediaMessage.fileName);

          if (!mimetype && isURL(mediaMessage.media)) {
            let config: any = { responseType: 'arraybuffer' };

            if (this.localProxy?.enabled) {
              config = {
                ...config,
                httpsAgent: makeProxyAgent({
                  host: this.localProxy.host,
                  port: this.localProxy.port,
                  protocol: this.localProxy.protocol,
                  username: this.localProxy.username,
                  password: this.localProxy.password,
                }),
              };
            }

            const response = await axios.get(mediaMessage.media, config);

            mimetype = response.headers['content-type'];
          }
        }

        if (mediaMessage.mediatype === 'ptv') {
          prepareMedia[mediaType] = prepareMedia[type + 'Message'];
          mimetype = 'video/mp4';

          if (!prepareMedia[mediaType]) {
            throw new Error('Failed to prepare video message');
          }

          try {
            let mediaInput;
            if (isURL(mediaMessage.media)) {
              mediaInput = mediaMessage.media;
            } else {
              const mediaBuffer = Buffer.from(mediaMessage.media, 'base64');
              if (!mediaBuffer || mediaBuffer.length === 0) {
                throw new Error('Invalid media buffer');
              }
              mediaInput = mediaBuffer;
            }

            const duration = await getVideoDuration(mediaInput);
            if (!duration || duration <= 0) {
              throw new Error('Invalid media duration');
            }

            this.logger.verbose(`Video duration: ${duration} seconds`);
            prepareMedia[mediaType].seconds = duration;
          } catch (error) {
            this.logger.error('Error getting video duration:');
            this.logger.error(error);
            throw new Error(`Failed to get video duration: ${error.message}`);
          }
        }

        prepareMedia[mediaType].caption = mediaMessage?.caption;
        prepareMedia[mediaType].mimetype = mimetype;
        prepareMedia[mediaType].fileName = mediaMessage.fileName;

        if (mediaMessage.mediatype === 'video') {
          prepareMedia[mediaType].gifPlayback = false;
        }

        return generateWAMessageFromContent(
          '',
          { [mediaType]: { ...prepareMedia[mediaType] } },
          { userJid: this.instance.wuid },
        );
      } catch (error) {
        lastError = error;
        const errorMsg =
          error?.message || error?.toString() || 'Unknown error during media upload';

        this.logger.error(`Media upload attempt ${attempt}/${maxRetries} failed: ${errorMsg}`);

        // Se for erro de conexão e ainda temos tentativas, fazer retry
        if (
          attempt < maxRetries &&
          (errorMsg.includes('upload') ||
            errorMsg.includes('connection') ||
            errorMsg.includes('ECONNRESET') ||
            errorMsg.includes('timeout'))
        ) {
          const delayTime = retryDelayMs * attempt;
          this.logger.warn(`Retrying in ${delayTime}ms...`);
          await delay(delayTime);
          continue;
        }

        // Se não é erro de upload ou acabaram as tentativas, lançar erro
        if (attempt === maxRetries) {
          throw new InternalServerErrorException(
            `Media upload failed after ${maxRetries} attempts: ${errorMsg}`,
          );
        }

        // Lançar erro imediatamente se não for erro de upload
        throw new InternalServerErrorException(errorMsg);
      }
    }

    // Se chegou aqui, todas as tentativas falhar am
    throw new InternalServerErrorException(
      `Media upload failed after ${maxRetries} attempts: ${lastError?.message || lastError}`,
    );
  }

  private async convertToWebP(image: string): Promise<Buffer> {
    try {
      let imageBuffer: Buffer;

      if (isBase64(image)) {
        const base64Data = image.replace(/^data:image\/(jpeg|png|gif);base64,/, '');
        imageBuffer = Buffer.from(base64Data, 'base64');
      } else {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(image);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        let config: any = { responseType: 'arraybuffer' };

        if (this.localProxy?.enabled) {
          config = {
            ...config,
            httpsAgent: makeProxyAgent({
              host: this.localProxy.host,
              port: this.localProxy.port,
              protocol: this.localProxy.protocol,
              username: this.localProxy.username,
              password: this.localProxy.password,
            }),
          };
        }

        const response = await axios.get(url, config);
        imageBuffer = Buffer.from(response.data, 'binary');
      }

      const isAnimated = this.isAnimated(image, imageBuffer);

      if (isAnimated) {
        return await sharp(imageBuffer, { animated: true }).webp({ quality: 80 }).toBuffer();
      } else {
        return await sharp(imageBuffer).webp().toBuffer();
      }
    } catch (error) {
      console.error('Erro ao converter a imagem para WebP:', error);
      throw error;
    }
  }

  private isAnimatedWebp(buffer: Buffer): boolean {
    if (buffer.length < 12) return false;

    return buffer.indexOf(Buffer.from('ANIM')) !== -1;
  }

  private isAnimated(image: string, buffer: Buffer): boolean {
    const lowerCaseImage = image.toLowerCase();

    if (lowerCaseImage.includes('.gif')) return true;

    if (lowerCaseImage.includes('.webp')) return this.isAnimatedWebp(buffer);

    return false;
  }

  public async mediaSticker(data: SendStickerDto, file?: any) {
    const mediaData: SendStickerDto = { ...data };

    if (file) mediaData.sticker = file.buffer.toString('base64');

    const convert = data?.notConvertSticker
      ? Buffer.from(data.sticker, 'base64')
      : await this.convertToWebP(data.sticker);
    const gifPlayback = data.sticker.includes('.gif');
    const result = await this.sendMessageWithTyping(
      data.number,
      { sticker: convert, gifPlayback },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
    );

    return result;
  }

  public async mediaMessage(data: SendMediaDto, file?: any, isIntegration = false) {
    const mediaData: SendMediaDto = { ...data };

    if (file) mediaData.media = file.buffer.toString('base64');

    const generate = await this.prepareMediaMessage(mediaData);

    const mediaSent = await this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
      isIntegration,
    );

    return mediaSent;
  }

  public async ptvMessage(data: SendPtvDto, file?: any, isIntegration = false) {
    const mediaData: SendMediaDto = {
      number: data.number,
      media: data.video,
      mediatype: 'ptv',
      delay: data?.delay,
      quoted: data?.quoted,
      mentionsEveryOne: data?.mentionsEveryOne,
      mentioned: data?.mentioned,
    };

    if (file) mediaData.media = file.buffer.toString('base64');

    const generate = await this.prepareMediaMessage(mediaData);

    const mediaSent = await this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
      isIntegration,
    );

    return mediaSent;
  }

  public async processAudioMp4(audio: string) {
    let inputStream: PassThrough;

    if (isURL(audio)) {
      const response = await axios.get(audio, { responseType: 'stream' });
      inputStream = response.data;
    } else {
      const audioBuffer = Buffer.from(audio, 'base64');
      inputStream = new PassThrough();
      inputStream.end(audioBuffer);
    }

    return new Promise<Buffer>((resolve, reject) => {
      const ffmpegProcess = spawn(ffmpegBinaryPath, [
        '-i',
        'pipe:0',
        '-vn',
        '-ab',
        '128k',
        '-ar',
        '44100',
        '-f',
        'mp4',
        '-movflags',
        'frag_keyframe+empty_moov',
        'pipe:1',
      ]);

      const outputChunks: Buffer[] = [];
      let stderrData = '';

      ffmpegProcess.stdout.on('data', (chunk) => {
        outputChunks.push(chunk);
      });

      ffmpegProcess.stderr.on('data', (data) => {
        stderrData += data.toString();
        this.logger.verbose(`ffmpeg stderr: ${data}`);
      });

      ffmpegProcess.on('error', (error) => {
        console.error('Error in ffmpeg process', error);
        reject(error);
      });

      ffmpegProcess.on('close', (code) => {
        if (code === 0) {
          this.logger.verbose('Audio converted to mp4');
          const outputBuffer = Buffer.concat(outputChunks);
          resolve(outputBuffer);
        } else {
          this.logger.error(`ffmpeg exited with code ${code}`);
          this.logger.error(`ffmpeg stderr: ${stderrData}`);
          reject(new Error(`ffmpeg exited with code ${code}: ${stderrData}`));
        }
      });

      inputStream.pipe(ffmpegProcess.stdin);

      inputStream.on('error', (err) => {
        console.error('Error in inputStream', err);
        ffmpegProcess.stdin.end();
        reject(err);
      });
    });
  }

  public async processAudio(audio: string): Promise<Buffer> {
    if (process.env.API_AUDIO_CONVERTER) {
      this.logger.verbose('Using audio converter API');
      const formData = new FormData();

      if (isURL(audio)) {
        formData.append('url', audio);
      } else {
        formData.append('base64', audio);
      }

      const { data } = await axios.post(process.env.API_AUDIO_CONVERTER, formData, {
        headers: { ...formData.getHeaders(), apikey: process.env.API_AUDIO_CONVERTER_KEY },
      });

      if (!data.audio) {
        throw new InternalServerErrorException('Failed to convert audio');
      }

      this.logger.verbose('Audio converted');
      return Buffer.from(data.audio, 'base64');
    } else {
      let inputAudioStream: PassThrough;

      if (isURL(audio)) {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(audio);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        const config: any = { responseType: 'stream' };

        const response = await axios.get(url, config);
        inputAudioStream = response.data.pipe(new PassThrough());
      } else {
        const audioBuffer = Buffer.from(audio, 'base64');
        inputAudioStream = new PassThrough();
        inputAudioStream.end(audioBuffer);
      }

      const isLpcm = isURL(audio) && /\.lpcm($|\?)/i.test(audio);

      return new Promise((resolve, reject) => {
        const outputAudioStream = new PassThrough();
        const chunks: Buffer[] = [];

        outputAudioStream.on('data', (chunk) => chunks.push(chunk));
        outputAudioStream.on('end', () => {
          const outputBuffer = Buffer.concat(chunks);
          resolve(outputBuffer);
        });

        outputAudioStream.on('error', (error) => {
          console.log('error', error);
          reject(error);
        });

        ffmpeg.setFfmpegPath(ffmpegBinaryPath);

        let command = ffmpeg(inputAudioStream);

        if (isLpcm) {
          this.logger.verbose('Detected LPCM input – applying raw PCM settings');
          command = command.inputFormat('s16le').inputOptions(['-ar', '24000', '-ac', '1']);
        }

        command
          .outputFormat('ogg')
          .noVideo()
          .audioCodec('libopus')
          .addOutputOptions('-avoid_negative_ts make_zero')
          .audioBitrate('128k')
          .audioFrequency(48000)
          .audioChannels(1)
          .outputOptions([
            '-write_xing',
            '0',
            '-compression_level',
            '10',
            '-application',
            'voip',
            '-fflags',
            '+bitexact',
            '-flags',
            '+bitexact',
            '-id3v2_version',
            '0',
            '-map_metadata',
            '-1',
            '-map_chapters',
            '-1',
            '-write_bext',
            '0',
          ])
          .pipe(outputAudioStream, { end: true })
          .on('error', function (error) {
            console.log('error', error);
            reject(error);
          });
      });
    }
  }

  public async audioWhatsapp(data: SendAudioDto, file?: any, isIntegration = false) {
    const mediaData: SendAudioDto = { ...data };

    if (file?.buffer) {
      mediaData.audio = file.buffer.toString('base64');
    } else if (!isURL(data.audio) && !isBase64(data.audio)) {
      console.error('Invalid file or audio source');
      throw new BadRequestException('File buffer, URL, or base64 audio is required');
    }

    if (!data?.encoding && data?.encoding !== false) {
      data.encoding = true;
    }

    if (data?.encoding) {
      const convert = await this.processAudio(mediaData.audio);

      if (Buffer.isBuffer(convert)) {
        const result = this.sendMessageWithTyping<AnyMessageContent>(
          data.number,
          { audio: convert, ptt: true, mimetype: 'audio/ogg; codecs=opus' },
          { presence: 'recording', delay: data?.delay },
          isIntegration,
        );

        return result;
      } else {
        throw new InternalServerErrorException('Failed to convert audio');
      }
    }

    return await this.sendMessageWithTyping<AnyMessageContent>(
      data.number,
      {
        audio: isURL(data.audio) ? { url: data.audio } : Buffer.from(data.audio, 'base64'),
        ptt: true,
        mimetype: 'audio/ogg; codecs=opus',
      },
      { presence: 'recording', delay: data?.delay },
      isIntegration,
    );
  }

  private generateRandomId(length = 11) {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
      result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
  }

  private toJSONString(button: Button): string {
    const toString = (obj: any) => JSON.stringify(obj);

    const json = {
      call: () => toString({ display_text: button.displayText, phone_number: button.phoneNumber }),
      reply: () => toString({ display_text: button.displayText, id: button.id }),
      copy: () => toString({ display_text: button.displayText, copy_code: button.copyCode }),
      url: () => toString({ display_text: button.displayText, url: button.url, merchant_url: button.url }),
      pix: () =>
        toString({
          currency: button.currency,
          total_amount: { value: 0, offset: 100 },
          reference_id: this.generateRandomId(),
          type: 'physical-goods',
          order: {
            status: 'pending',
            subtotal: { value: 0, offset: 100 },
            order_type: 'ORDER',
            items: [
              { name: '', amount: { value: 0, offset: 100 }, quantity: 0, sale_amount: { value: 0, offset: 100 } },
            ],
          },
          payment_settings: [
            {
              type: 'pix_static_code',
              pix_static_code: {
                merchant_name: button.name,
                key: button.key,
                key_type: this.mapKeyType.get(button.keyType),
              },
            },
          ],
          share_payment_status: false,
        }),
    };

    return json[button.type]?.() || '';
  }

  private readonly mapType = new Map<TypeButton, string>([
    ['reply', 'quick_reply'],
    ['copy', 'cta_copy'],
    ['url', 'cta_url'],
    ['call', 'cta_call'],
    ['pix', 'payment_info'],
  ]);

  private readonly mapKeyType = new Map<KeyType, string>([
    ['phone', 'PHONE'],
    ['email', 'EMAIL'],
    ['cpf', 'CPF'],
    ['cnpj', 'CNPJ'],
    ['random', 'EVP'],
  ]);

  public async buttonMessage(data: SendButtonsDto) {
    if (data.buttons.length === 0) {
      throw new BadRequestException('At least one button is required');
    }

    const hasReplyButtons = data.buttons.some((btn) => btn.type === 'reply');

    const hasPixButton = data.buttons.some((btn) => btn.type === 'pix');

    const hasOtherButtons = data.buttons.some((btn) => btn.type !== 'reply' && btn.type !== 'pix');

    if (hasReplyButtons) {
      if (data.buttons.length > 3) {
        throw new BadRequestException('Maximum of 3 reply buttons allowed');
      }
      if (hasOtherButtons) {
        throw new BadRequestException('Reply buttons cannot be mixed with other button types');
      }
    }

    if (hasPixButton) {
      if (data.buttons.length > 1) {
        throw new BadRequestException('Only one PIX button is allowed');
      }
      if (hasOtherButtons) {
        throw new BadRequestException('PIX button cannot be mixed with other button types');
      }

      const message: proto.IMessage = {
        viewOnceMessage: {
          message: {
            interactiveMessage: {
              nativeFlowMessage: {
                buttons: [{ name: this.mapType.get('pix'), buttonParamsJson: this.toJSONString(data.buttons[0]) }],
                messageParamsJson: JSON.stringify({ from: 'api', templateId: v4() }),
              },
            },
          },
        },
      };

      return await this.sendMessageWithTyping(data.number, message, {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      });
    }

    const generate = await (async () => {
      if (data?.thumbnailUrl) {
        return await this.prepareMediaMessage({ mediatype: 'image', media: data.thumbnailUrl });
      }
    })();

    const buttons = data.buttons.map((value) => {
      return { name: this.mapType.get(value.type), buttonParamsJson: this.toJSONString(value) };
    });

    const message: proto.IMessage = {
      viewOnceMessage: {
        message: {
          interactiveMessage: {
            body: {
              text: (() => {
                let t = '*' + data.title + '*';
                if (data?.description) {
                  t += '\n\n';
                  t += data.description;
                  t += '\n';
                }
                return t;
              })(),
            },
            footer: { text: data?.footer },
            header: (() => {
              if (generate?.message?.imageMessage) {
                return {
                  hasMediaAttachment: !!generate.message.imageMessage,
                  imageMessage: generate.message.imageMessage,
                };
              }
            })(),
            nativeFlowMessage: {
              buttons: buttons,
              messageParamsJson: JSON.stringify({ from: 'api', templateId: v4() }),
            },
          },
        },
      },
    };

    return await this.sendMessageWithTyping(data.number, message, {
      delay: data?.delay,
      presence: 'composing',
      quoted: data?.quoted,
      mentionsEveryOne: data?.mentionsEveryOne,
      mentioned: data?.mentioned,
    });
  }

  public async locationMessage(data: SendLocationDto) {
    return await this.sendMessageWithTyping(
      data.number,
      {
        locationMessage: {
          degreesLatitude: data.latitude,
          degreesLongitude: data.longitude,
          name: data?.name,
          address: data?.address,
        },
      },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
    );
  }

  public async listMessage(data: SendListDto) {
    return await this.sendMessageWithTyping(
      data.number,
      {
        listMessage: {
          title: data.title,
          description: data.description,
          buttonText: data?.buttonText,
          footerText: data?.footerText,
          sections: data.sections,
          listType: 2,
        },
      },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
    );
  }

  public async contactMessage(data: SendContactDto) {
    const message: proto.IMessage = {};

    const vcard = (contact: ContactMessage) => {
      let result = 'BEGIN:VCARD\n' + 'VERSION:3.0\n' + `N:${contact.fullName}\n` + `FN:${contact.fullName}\n`;

      if (contact.organization) {
        result += `ORG:${contact.organization};\n`;
      }

      if (contact.email) {
        result += `EMAIL:${contact.email}\n`;
      }

      if (contact.url) {
        result += `URL:${contact.url}\n`;
      }

      if (!contact.wuid) {
        contact.wuid = createJid(contact.phoneNumber);
      }

      result += `item1.TEL;waid=${contact.wuid}:${contact.phoneNumber}\n` + 'item1.X-ABLabel:Celular\n' + 'END:VCARD';

      return result;
    };

    if (data.contact.length === 1) {
      message.contactMessage = { displayName: data.contact[0].fullName, vcard: vcard(data.contact[0]) };
    } else {
      message.contactsArrayMessage = {
        displayName: `${data.contact.length} contacts`,
        contacts: data.contact.map((contact) => {
          return { displayName: contact.fullName, vcard: vcard(contact) };
        }),
      };
    }

    return await this.sendMessageWithTyping(data.number, { ...message }, {});
  }

  public async reactionMessage(data: SendReactionDto) {
    return await this.sendMessageWithTyping(data.key.remoteJid, {
      reactionMessage: { key: data.key, text: data.reaction },
    });
  }

  // Chat Controller
  public async whatsappNumber(data: WhatsAppNumberDto) {
    const jids: {
      groups: { number: string; jid: string }[];
      broadcast: { number: string; jid: string }[];
      users: { number: string; jid: string; name?: string }[];
    } = { groups: [], broadcast: [], users: [] };

    data.numbers.forEach((number) => {
      const jid = createJid(number);

      if (isJidGroup(jid)) {
        jids.groups.push({ number, jid });
      } else if (jid === 'status@broadcast') {
        jids.broadcast.push({ number, jid });
      } else {
        jids.users.push({ number, jid });
      }
    });

    const onWhatsapp: OnWhatsAppDto[] = [];

    // BROADCAST
    onWhatsapp.push(...jids.broadcast.map(({ jid, number }) => new OnWhatsAppDto(jid, false, number)));

    // GROUPS
    const groups = await Promise.all(
      jids.groups.map(async ({ jid, number }) => {
        const group = await this.findGroup({ groupJid: jid }, 'inner');

        if (!group) {
          return new OnWhatsAppDto(jid, false, number);
        }

        return new OnWhatsAppDto(group.id, true, number, group?.subject);
      }),
    );
    onWhatsapp.push(...groups);

    // USERS
    const contacts: any[] = await this.prismaRepository.contact.findMany({
      where: { instanceId: this.instanceId, remoteJid: { in: jids.users.map(({ jid }) => jid) } },
    });

    // Separate @lid numbers from normal numbers
    const lidUsers = jids.users.filter(({ jid }) => jid.includes('@lid'));
    const normalUsers = jids.users.filter(({ jid }) => !jid.includes('@lid'));

    // For normal numbers, use traditional Baileys verification
    let normalVerifiedUsers: OnWhatsAppDto[] = [];
    if (normalUsers.length > 0) {
      console.log('normalUsers', normalUsers);
      const numbersToVerify = normalUsers.map(({ jid }) => jid.replace('+', ''));
      console.log('numbersToVerify', numbersToVerify);

      const cachedNumbers = await getOnWhatsappCache(numbersToVerify);
      console.log('cachedNumbers', cachedNumbers);

      const filteredNumbers = numbersToVerify.filter(
        (jid) => !cachedNumbers.some((cached) => cached.jidOptions.includes(jid)),
      );
      console.log('filteredNumbers', filteredNumbers);

      const verify = await this.client.onWhatsApp(...filteredNumbers);
      console.log('verify', verify);
      normalVerifiedUsers = await Promise.all(
        normalUsers.map(async (user) => {
          let numberVerified: (typeof verify)[0] | null = null;

          const cached = cachedNumbers.find((cached) => cached.jidOptions.includes(user.jid.replace('+', '')));
          if (cached) {
            return new OnWhatsAppDto(
              cached.remoteJid,
              true,
              user.number,
              contacts.find((c) => c.remoteJid === cached.remoteJid)?.pushName,
              cached.lid || (cached.remoteJid.includes('@lid') ? cached.remoteJid.split('@')[1] : undefined),
            );
          }

          // Brazilian numbers
          if (user.number.startsWith('55')) {
            const numberWithDigit =
              user.number.slice(4, 5) === '9' && user.number.length === 13
                ? user.number
                : `${user.number.slice(0, 4)}9${user.number.slice(4)}`;
            const numberWithoutDigit =
              user.number.length === 12 ? user.number : user.number.slice(0, 4) + user.number.slice(5);

            numberVerified = verify.find(
              (v) => v.jid === `${numberWithDigit}@s.whatsapp.net` || v.jid === `${numberWithoutDigit}@s.whatsapp.net`,
            );
          }

          // Mexican/Argentina numbers
          // Ref: https://faq.whatsapp.com/1294841057948784
          if (!numberVerified && (user.number.startsWith('52') || user.number.startsWith('54'))) {
            let prefix = '';
            if (user.number.startsWith('52')) {
              prefix = '1';
            }
            if (user.number.startsWith('54')) {
              prefix = '9';
            }

            const numberWithDigit =
              user.number.slice(2, 3) === prefix && user.number.length === 13
                ? user.number
                : `${user.number.slice(0, 2)}${prefix}${user.number.slice(2)}`;
            const numberWithoutDigit =
              user.number.length === 12 ? user.number : user.number.slice(0, 2) + user.number.slice(3);

            numberVerified = verify.find(
              (v) => v.jid === `${numberWithDigit}@s.whatsapp.net` || v.jid === `${numberWithoutDigit}@s.whatsapp.net`,
            );
          }

          if (!numberVerified) {
            numberVerified = verify.find((v) => v.jid === user.jid);
          }

          const numberJid = numberVerified?.jid || user.jid;
          const lid = numberJid.includes('@lid') ? numberJid.split('@')[0] : undefined;
          return new OnWhatsAppDto(
            numberJid,
            !!numberVerified?.exists,
            user.number,
            contacts.find((c) => c.remoteJid === numberJid)?.pushName,
            lid,
          );
        }),
      );
    }

    // For @lid numbers, always consider them as valid
    const lidVerifiedUsers: OnWhatsAppDto[] = lidUsers.map((user) => {
      return new OnWhatsAppDto(
        user.jid,
        true,
        user.number,
        contacts.find((c) => c.remoteJid === user.jid)?.pushName,
        user.jid.split('@')[1],
      );
    });

    // Combine results
    onWhatsapp.push(...normalVerifiedUsers, ...lidVerifiedUsers);

    // Save to cache only valid numbers
    await saveOnWhatsappCache(
      onWhatsapp
        .filter((user) => user.exists)
        .map((user) => ({
          remoteJid: user.jid,
          jidOptions: user.jid.replace('+', ''),
          lid: user.lid,
        })),
    );

    return onWhatsapp;
  }

  public async markMessageAsRead(data: ReadMessageDto) {
    try {
      const keys: proto.IMessageKey[] = [];
      data.readMessages.forEach((read) => {
        if (isJidGroup(read.remoteJid) || isPnUser(read.remoteJid) || isLidUser(read.remoteJid)) {
          keys.push({ remoteJid: read.remoteJid, fromMe: read.fromMe, id: read.id });
        }
      });
      await this.client.readMessages(keys);
      return { message: 'Read messages', read: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Read messages fail', error.toString());
    }
  }

  public async getLastMessage(number: string) {
    const messages = await this.prismaRepository.message.findMany({
      where: {
        instanceId: this.instanceId,
        key: { path: ['remoteJid'], equals: number },
      },
      orderBy: { messageTimestamp: 'desc' },
      take: 1,
    });

    if (messages.length === 0) {
      throw new NotFoundException('Messages not found');
    }

    const lastMessage = messages[0];

    return lastMessage as unknown as LastMessage;
  }

  public async archiveChat(data: ArchiveChatDto) {
    try {
      let last_message = data.lastMessage;
      let number = data.chat;

      if (!last_message && number) {
        last_message = await this.getLastMessage(number);
      } else {
        last_message = data.lastMessage;
        last_message.messageTimestamp = last_message?.messageTimestamp ?? Date.now();
        number = last_message?.key?.remoteJid;
      }

      if (!last_message || Object.keys(last_message).length === 0) {
        throw new NotFoundException('Last message not found');
      }

      await this.client.chatModify({ archive: data.archive, lastMessages: [last_message] }, createJid(number));

      return { chatId: number, archived: true };
    } catch (error) {
      throw new InternalServerErrorException({
        archived: false,
        message: ['An error occurred while archiving the chat. Open a calling.', error.toString()],
      });
    }
  }

  public async markChatUnread(data: MarkChatUnreadDto) {
    try {
      let last_message = data.lastMessage;
      let number = data.chat;

      if (!last_message && number) {
        last_message = await this.getLastMessage(number);
      } else {
        last_message = data.lastMessage;
        last_message.messageTimestamp = last_message?.messageTimestamp ?? Date.now();
        number = last_message?.key?.remoteJid;
      }

      if (!last_message || Object.keys(last_message).length === 0) {
        throw new NotFoundException('Last message not found');
      }

      await this.client.chatModify({ markRead: false, lastMessages: [last_message] }, createJid(number));

      return { chatId: number, markedChatUnread: true };
    } catch (error) {
      throw new InternalServerErrorException({
        markedChatUnread: false,
        message: ['An error occurred while marked unread the chat. Open a calling.', error.toString()],
      });
    }
  }

  public async deleteMessage(del: DeleteMessage) {
    try {
      const response = await this.client.sendMessage(del.remoteJid, { delete: del });
      if (response) {
        const messageId = response.message?.protocolMessage?.key?.id;
        if (messageId) {
          const isLogicalDeleted = configService.get<Database>('DATABASE').DELETE_DATA.LOGICAL_MESSAGE_DELETE;
          let message = await this.prismaRepository.message.findFirst({
            where: { key: { path: ['id'], equals: messageId } },
          });
          if (isLogicalDeleted) {
            if (!message) return response;
            const existingKey = typeof message?.key === 'object' && message.key !== null ? message.key : {};
            message = await this.prismaRepository.message.update({
              where: { id: message.id },
              data: { key: { ...existingKey, deleted: true }, status: 'DELETED' },
            });
            if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
              const messageUpdate: any = {
                messageId: message.id,
                keyId: messageId,
                remoteJid: response.key.remoteJid,
                fromMe: response.key.fromMe,
                participant: response.key?.remoteJid,
                status: 'DELETED',
                instanceId: this.instanceId,
              };
              await this.prismaRepository.messageUpdate.create({ data: messageUpdate });
            }
          } else {
            if (!message) return response;
            await this.prismaRepository.message.deleteMany({ where: { id: message.id } });
          }
          this.sendDataWebhook(Events.MESSAGES_DELETE, {
            id: message.id,
            instanceId: message.instanceId,
            key: message.key,
            messageType: message.messageType,
            status: 'DELETED',
            source: message.source,
            messageTimestamp: message.messageTimestamp,
            pushName: message.pushName,
            participant: message.participant,
            message: message.message,
          });
        }
      }

      return response;
    } catch (error) {
      throw new InternalServerErrorException('Error while deleting message for everyone', error?.toString());
    }
  }

  public async mapMediaType(mediaType) {
    const map = {
      imageMessage: 'image',
      videoMessage: 'video',
      documentMessage: 'document',
      stickerMessage: 'sticker',
      audioMessage: 'audio',
      ptvMessage: 'video',
    };
    return map[mediaType] || null;
  }

  public async getBase64FromMediaMessage(data: getBase64FromMediaMessageDto, getBuffer = false) {
    try {
      const m = data?.message;
      const convertToMp4 = data?.convertToMp4 ?? false;

      const msg = m?.message ? m : ((await this.getMessage(m.key, true)) as proto.IWebMessageInfo);

      if (!msg) {
        throw 'Message not found';
      }

      for (const subtype of MessageSubtype) {
        if (msg.message[subtype]) {
          msg.message = msg.message[subtype].message;
        }
      }

      if ('messageContextInfo' in msg.message && Object.keys(msg.message).length === 1) {
        throw 'The message is messageContextInfo';
      }

      let mediaMessage: any;
      let mediaType: string;

      if (msg.message?.templateMessage) {
        const template =
          msg.message.templateMessage.hydratedTemplate || msg.message.templateMessage.hydratedFourRowTemplate;

        for (const type of TypeMediaMessage) {
          if (template[type]) {
            mediaMessage = template[type];
            mediaType = type;
            msg.message = { [type]: { ...template[type], url: template[type].staticUrl } };
            break;
          }
        }

        if (!mediaMessage) {
          throw 'Template message does not contain a supported media type';
        }
      } else {
        for (const type of TypeMediaMessage) {
          mediaMessage = msg.message[type];
          if (mediaMessage) {
            mediaType = type;
            break;
          }
        }

        if (!mediaMessage) {
          throw 'The message is not of the media type';
        }
      }

      if (typeof mediaMessage['mediaKey'] === 'object') {
        msg.message = JSON.parse(JSON.stringify(msg.message));
      }

      let buffer: Buffer;

      try {
        buffer = await downloadMediaMessage(
          { key: msg?.key, message: msg?.message },
          'buffer',
          {},
          { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
        );
      } catch (err) {
        this.logger.error('Download Media failed, trying to retry in 5 seconds...');
        await new Promise((resolve) => setTimeout(resolve, 5000));
        const mediaType = Object.keys(msg.message).find((key) => key.endsWith('Message'));
        if (!mediaType) throw new Error('Could not determine mediaType for fallback');

        try {
          const media = await downloadContentFromMessage(
            {
              mediaKey: msg.message?.[mediaType]?.mediaKey,
              directPath: msg.message?.[mediaType]?.directPath,
              url: `https://mmg.whatsapp.net${msg?.message?.[mediaType]?.directPath}`,
            },
            await this.mapMediaType(mediaType),
            {},
          );
          const chunks = [];
          for await (const chunk of media) {
            chunks.push(chunk);
          }
          buffer = Buffer.concat(chunks);
          this.logger.info('Download Media with downloadContentFromMessage was successful!');
        } catch (fallbackErr) {
          this.logger.error('Download Media with downloadContentFromMessage also failed!');
          throw fallbackErr;
        }
      }
      const typeMessage = getContentType(msg.message);

      const ext = mimeTypes.extension(mediaMessage?.['mimetype']);
      const fileName = mediaMessage?.['fileName'] || `${msg.key.id}.${ext}` || `${v4()}.${ext}`;

      if (convertToMp4 && typeMessage === 'audioMessage') {
        try {
          const convert = await this.processAudioMp4(buffer.toString('base64'));

          if (Buffer.isBuffer(convert)) {
            const result = {
              mediaType,
              fileName,
              caption: mediaMessage['caption'],
              size: {
                fileLength: mediaMessage['fileLength'],
                height: mediaMessage['height'],
                width: mediaMessage['width'],
              },
              mimetype: 'audio/mp4',
              base64: convert.toString('base64'),
              buffer: getBuffer ? convert : null,
            };

            return result;
          }
        } catch (error) {
          this.logger.error('Error converting audio to mp4:');
          this.logger.error(error);
          throw new BadRequestException('Failed to convert audio to MP4');
        }
      }

      return {
        mediaType,
        fileName,
        caption: mediaMessage['caption'],
        size: { fileLength: mediaMessage['fileLength'], height: mediaMessage['height'], width: mediaMessage['width'] },
        mimetype: mediaMessage['mimetype'],
        base64: buffer.toString('base64'),
        buffer: getBuffer ? buffer : null,
      };
    } catch (error) {
      this.logger.error('Error processing media message:');
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  public async fetchPrivacySettings() {
    const privacy = await this.client.fetchPrivacySettings();

    return {
      readreceipts: privacy.readreceipts,
      profile: privacy.profile,
      status: privacy.status,
      online: privacy.online,
      last: privacy.last,
      groupadd: privacy.groupadd,
    };
  }

  public async updatePrivacySettings(settings: PrivacySettingDto) {
    try {
      await this.client.updateReadReceiptsPrivacy(settings.readreceipts);
      await this.client.updateProfilePicturePrivacy(settings.profile);
      await this.client.updateStatusPrivacy(settings.status);
      await this.client.updateOnlinePrivacy(settings.online);
      await this.client.updateLastSeenPrivacy(settings.last);
      await this.client.updateGroupsAddPrivacy(settings.groupadd);

      this.reloadConnection();

      return {
        update: 'success',
        data: {
          readreceipts: settings.readreceipts,
          profile: settings.profile,
          status: settings.status,
          online: settings.online,
          last: settings.last,
          groupadd: settings.groupadd,
        },
      };
    } catch (error) {
      throw new InternalServerErrorException('Error updating privacy settings', error.toString());
    }
  }

  public async fetchBusinessProfile(number: string): Promise<NumberBusiness> {
    try {
      const jid = number ? createJid(number) : this.instance.wuid;

      const profile = await this.client.getBusinessProfile(jid);

      if (!profile) {
        const info = await this.whatsappNumber({ numbers: [jid] });

        return { isBusiness: false, message: 'Not is business profile', ...info?.shift() };
      }

      return { isBusiness: true, ...profile };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile name', error.toString());
    }
  }

  public async updateProfileName(name: string) {
    try {
      await this.client.updateProfileName(name);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile name', error.toString());
    }
  }

  public async updateProfileStatus(status: string) {
    try {
      await this.client.updateProfileStatus(status);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile status', error.toString());
    }
  }

  public async updateProfilePicture(picture: string) {
    try {
      let pic: WAMediaUpload;
      if (isURL(picture)) {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(picture);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        let config: any = { responseType: 'arraybuffer' };

        if (this.localProxy?.enabled) {
          config = {
            ...config,
            httpsAgent: makeProxyAgent({
              host: this.localProxy.host,
              port: this.localProxy.port,
              protocol: this.localProxy.protocol,
              username: this.localProxy.username,
              password: this.localProxy.password,
            }),
          };
        }

        pic = (await axios.get(url, config)).data;
      } else if (isBase64(picture)) {
        pic = Buffer.from(picture, 'base64');
      } else {
        throw new BadRequestException('"profilePicture" must be a url or a base64');
      }

      await this.client.updateProfilePicture(this.instance.wuid, pic);

      this.reloadConnection();

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile picture', error.toString());
    }
  }

  public async removeProfilePicture() {
    try {
      await this.client.removeProfilePicture(this.instance.wuid);

      this.reloadConnection();

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error removing profile picture', error.toString());
    }
  }

  public async blockUser(data: BlockUserDto) {
    try {
      const { number } = data;

      const isWA = (await this.whatsappNumber({ numbers: [number] }))?.shift();

      if (!isWA.exists && !isJidGroup(isWA.jid) && !isWA.jid.includes('@broadcast')) {
        throw new BadRequestException(isWA);
      }

      const sender = isWA.jid;

      await this.client.updateBlockStatus(sender, data.status);

      return { block: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error blocking user', error.toString());
    }
  }

  private async getValidRemoteJid(obj: any): Promise<string | null> {
    if (!obj) return null;

    const remoteJid = obj.remoteJid;
    const remoteJidAlt = obj.remoteJidAlt;
    const senderPn = obj.senderPn;
    const id = obj.id;

    // Se remoteJid contém @s.whatsapp.net, usa ele
    if (remoteJid && remoteJid.includes('@s.whatsapp.net')) {
      return remoteJid;
    }

    // Se remoteJidAlt contém @s.whatsapp.net, usa ele
    if (remoteJidAlt && remoteJidAlt.includes('@s.whatsapp.net')) {
      return remoteJidAlt;
    }

    // Se senderPn contém @s.whatsapp.net, usa ele
    if (senderPn && senderPn.includes('@s.whatsapp.net')) {
      return senderPn;
    }

    // Se id contém @s.whatsapp.net, usa ele
    if (id && id.includes('@s.whatsapp.net')) {
      return id;
    }

    // Se remoteJid é um LID, tenta buscar o PN correspondente usando cache Redis + Baileys
    if (remoteJid && remoteJid.includes('@lid')) {
      try {
        const lidValue = remoteJid.replace('@lid', '');
        // Usa o mesmo método de resolução com cache que o processamento em tempo real
        const phoneNumber = await this.resolveLIDToPN(lidValue);

        if (phoneNumber) {
          this.logger.verbose(`[getValidRemoteJid] LID ${remoteJid} convertido para ${phoneNumber}@s.whatsapp.net`);
          return `${phoneNumber}@s.whatsapp.net`;
        } else {
          this.logger.debug(`[getValidRemoteJid] Could not resolve LID ${remoteJid}, using as-is`);
        }
      } catch (error) {
        this.logger.warn(`[getValidRemoteJid] Falha ao obter PN para LID ${remoteJid}: ${error?.message || error}`);
      }
    }

    // Se nenhum contém @s.whatsapp.net, dá preferência ao remoteJid
    if (remoteJid) {
      return remoteJid;
    }

    // Se remoteJid não existe, usa remoteJidAlt
    if (remoteJidAlt) {
      return remoteJidAlt;
    }

    // Se remoteJidAlt não existe, usa senderPn
    if (senderPn) {
      return senderPn;
    }

    // Por último, usa id se disponível
    if (id) {
      return id;
    }

    return null;
  }

  public async syncMessages(data: SyncMessagesDto) {
    const limit = data.limit || 100; // Default to 100 messages per chat

    // If remoteJid is provided, sync only that chat
    if (data.remoteJid) {
      // Validate and get the correct remoteJid
      const validatedNumbers = await this.whatsappNumber({ numbers: [data.remoteJid] });

      if (!validatedNumbers || validatedNumbers.length === 0) {
        throw new NotFoundException('Number not found or invalid');
      }

      const numberInfo = validatedNumbers[0];

      // Check if number exists on WhatsApp
      if (!numberInfo.exists) {
        throw new NotFoundException(`Number ${data.remoteJid} is not on WhatsApp`);
      }

      // Skip groups
      if (numberInfo.jid.includes('@g.us')) {
        throw new BadRequestException('Groups are not supported for synchronization');
      }

      // Only accept @s.whatsapp.net
      if (!numberInfo.jid.includes('@s.whatsapp.net')) {
        throw new BadRequestException('Only individual chats are supported (@s.whatsapp.net)');
      }

      const validRemoteJid = numberInfo.jid;

      // Process in background
      this.syncSingleChat(validRemoteJid, limit).catch((error) => {
        this.logger.error(`Background sync error for ${validRemoteJid}: ${error.toString()}`);
      });

      return {
        success: true,
        message: 'Synchronization started in background',
        validatedNumber: validRemoteJid
      };
    }

    // If no remoteJid provided, sync all individual chats
    // Start background process and return immediately
    this.syncRecentChats(limit).catch((error) => {
      this.logger.error(`Background sync error: ${error.toString()}`);
    });

    return {
      success: true,
      message: 'Synchronization started in background for all individual chats'
    };
  }

  private async syncSingleChat(remoteJid: string, limit: number): Promise<void> {
    try {
      // Get the last message from the chat to use as reference
      const lastMessage = await this.getLastMessage(remoteJid);

      if (!lastMessage || !lastMessage.key) {
        this.logger.warn(`No messages found for chat: ${remoteJid}, skipping...`);
        return;
      }

      // Request message history synchronization from Baileys
      this.logger.verbose(`Requesting message history for ${remoteJid}: limit=${limit}, timestamp=${lastMessage.messageTimestamp}`);

      const syncResult = await this.client.fetchMessageHistory(
        limit,
        lastMessage.key,
        lastMessage.messageTimestamp
      );

      this.logger.info(`Successfully requested sync for chat: ${remoteJid} (${limit} messages), syncId: ${syncResult || 'none'}`);

      // FALLBACK: Also fetch messages from database and send to webhook
      // This ensures messages are sent even if Baileys doesn't emit the messaging-history.set event
      await this.syncMessagesFromDatabase(remoteJid, limit);

    } catch (error) {
      // If no messages found for this chat, skip it silently (this is expected for empty chats)
      if (error?.message?.[0] === 'Messages not found') {
        this.logger.verbose(`Skipping sync for chat ${remoteJid}: no messages found in database`);
        return;
      }

      const errorMessage = error?.message || error?.toString() || JSON.stringify(error);
      this.logger.error(`Error syncing chat ${remoteJid}: ${errorMessage}`);
    }
  }

  private async syncMessagesFromDatabase(remoteJid: string, limit: number): Promise<void> {
    try {
      this.logger.verbose(`Fetching up to ${limit} messages from database for ${remoteJid}`);

      // Fetch messages from database
      const messages = await this.prismaRepository.message.findMany({
        where: {
          instanceId: this.instanceId,
          key: { path: ['remoteJid'], equals: remoteJid },
        },
        orderBy: { messageTimestamp: 'desc' },
        take: limit,
      });

      if (messages.length === 0) {
        this.logger.verbose(`No messages found in database for ${remoteJid}`);
        return;
      }

      this.logger.info(`Found ${messages.length} messages in database for ${remoteJid}, sending to webhook`);

      // Send to webhook as MESSAGES_SET event
      // Sort by timestamp ascending (oldest first) to match the order of messaging-history.set
      const messagesAscending = messages.reverse();
      this.sendDataWebhook(Events.MESSAGES_SET, messagesAscending);

      this.logger.verbose(`Successfully sent ${messages.length} messages to webhook for ${remoteJid}`);
    } catch (error) {
      this.logger.error(`Error syncing messages from database for ${remoteJid}: ${error.toString()}`);
    }
  }

  private async syncRecentChats(limit: number): Promise<void> {
    try {
      // Get all chats, ordered by most recent first
      const recentChats = await this.prismaRepository.chat.findMany({
        where: {
          instanceId: this.instanceId,
        },
        select: {
          remoteJid: true,
          updatedAt: true,
        },
        orderBy: {
          updatedAt: 'desc', // Most recent chats first
        },
      });

      if (!recentChats || recentChats.length === 0) {
        this.logger.warn('No chats found for this instance');
        return;
      }

      // Filter only individual chats (@s.whatsapp.net), ignore groups
      const individualChats = [];
      for (const chat of recentChats) {
        const validRemoteJid = await this.getValidRemoteJid(chat);
        if (validRemoteJid && validRemoteJid.includes('@s.whatsapp.net')) {
          individualChats.push({ ...chat, validRemoteJid });
        }
      }

      if (individualChats.length === 0) {
        this.logger.warn('No individual chats found');
        return;
      }

      this.logger.info(
        `Starting background synchronization for ${individualChats.length} individual chats (ignoring ${recentChats.length - individualChats.length} groups)`
      );

      // Process each chat
      for (const chat of individualChats) {
        if (!chat.validRemoteJid) {
          this.logger.warn(`Invalid remoteJid for chat: ${chat.remoteJid}, skipping...`);
          continue;
        }

        await this.syncSingleChat(chat.validRemoteJid, limit);

        // Add a small delay between requests to avoid overwhelming the server
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      this.logger.info(`Background synchronization completed for ${individualChats.length} chats`);
    } catch (error) {
      this.logger.error(`Error in background sync: ${error.toString()}`);
    }
  }

  private async formatUpdateMessage(data: UpdateMessageDto) {
    try {
      if (!this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
        return data;
      }

      const msg: any = await this.getMessage(data.key, true);

      if (msg?.messageType === 'conversation' || msg?.messageType === 'extendedTextMessage') {
        return { text: data.text };
      }

      if (msg?.messageType === 'imageMessage') {
        return { image: msg?.message?.imageMessage, caption: data.text };
      }

      if (msg?.messageType === 'videoMessage') {
        return { video: msg?.message?.videoMessage, caption: data.text };
      }

      return null;
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  public async updateMessage(data: UpdateMessageDto) {
    const jid = createJid(data.number);

    const options = await this.formatUpdateMessage(data);

    if (!options) {
      this.logger.error('Message not compatible');
      throw new BadRequestException('Message not compatible');
    }

    try {
      const oldMessage: any = await this.getMessage(data.key, true);
      if (this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
        if (!oldMessage) throw new NotFoundException('Message not found');
        if (oldMessage?.key?.remoteJid !== jid) {
          throw new BadRequestException('RemoteJid does not match');
        }
        if (oldMessage?.messageTimestamp > Date.now() + 900000) {
          // 15 minutes in milliseconds
          throw new BadRequestException('Message is older than 15 minutes');
        }
      }

      const messageSent = await this.client.sendMessage(jid, { ...(options as any), edit: data.key });
      if (messageSent) {
        const editedMessage =
          messageSent?.message?.protocolMessage || messageSent?.message?.editedMessage?.message?.protocolMessage;

        if (editedMessage) {
          this.sendDataWebhook(Events.SEND_MESSAGE_UPDATE, editedMessage);
          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled)
            this.chatwootService.eventWhatsapp(
              'send.message.update',
              { instanceName: this.instance.name, instanceId: this.instance.id },
              editedMessage,
            );

          const messageId = messageSent.message?.protocolMessage?.key?.id;
          if (messageId && this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
            let message = await this.prismaRepository.message.findFirst({
              where: { key: { path: ['id'], equals: messageId } },
            });
            if (!message) throw new NotFoundException('Message not found');

            if (!(message.key.valueOf() as any).fromMe) {
              new BadRequestException('You cannot edit others messages');
            }
            if ((message.key.valueOf() as any)?.deleted) {
              new BadRequestException('You cannot edit deleted messages');
            }

            if (oldMessage.messageType === 'conversation' || oldMessage.messageType === 'extendedTextMessage') {
              oldMessage.message.conversation = data.text;
            } else {
              oldMessage.message[oldMessage.messageType].caption = data.text;
            }
            message = await this.prismaRepository.message.update({
              where: { id: message.id },
              data: {
                message: oldMessage.message,
                status: 'EDITED',
                messageTimestamp: Math.floor(Date.now() / 1000), // Convert to int32 by dividing by 1000 to get seconds
              },
            });

            if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
              const messageUpdate: any = {
                messageId: message.id,
                keyId: messageId,
                remoteJid: messageSent.key.remoteJid,
                fromMe: messageSent.key.fromMe,
                participant: messageSent.key?.remoteJid,
                status: 'EDITED',
                instanceId: this.instanceId,
              };
              await this.prismaRepository.messageUpdate.create({ data: messageUpdate });
            }
          }
        }
      }

      return messageSent;
    } catch (error) {
      this.logger.error(error);
      throw error;
    }
  }

  public async fetchLabels(): Promise<LabelDto[]> {
    const labels = await this.prismaRepository.label.findMany({ where: { instanceId: this.instanceId } });

    return labels.map((label) => ({
      color: label.color,
      name: label.name,
      id: label.labelId,
      predefinedId: label.predefinedId,
    }));
  }

  public async handleLabel(data: HandleLabelDto) {
    const whatsappContact = await this.whatsappNumber({ numbers: [data.number] });
    if (whatsappContact.length === 0) {
      throw new NotFoundException('Number not found');
    }
    const contact = whatsappContact[0];
    if (!contact.exists) {
      throw new NotFoundException('Number is not on WhatsApp');
    }

    try {
      if (data.action === 'add') {
        await this.client.addChatLabel(contact.jid, data.labelId);
        await this.addLabel(data.labelId, this.instanceId, contact.jid);

        return { numberJid: contact.jid, labelId: data.labelId, add: true };
      }
      if (data.action === 'remove') {
        await this.client.removeChatLabel(contact.jid, data.labelId);
        await this.removeLabel(data.labelId, this.instanceId, contact.jid);

        return { numberJid: contact.jid, labelId: data.labelId, remove: true };
      }
    } catch (error) {
      throw new BadRequestException(`Unable to ${data.action} label to chat`, error.toString());
    }
  }

  // Group
  private async updateGroupMetadataCache(groupJid: string) {
    try {
      const meta = await this.client.groupMetadata(groupJid);

      const cacheConf = this.configService.get<CacheConf>('CACHE');

      if ((cacheConf?.REDIS?.ENABLED && cacheConf?.REDIS?.URI !== '') || cacheConf?.LOCAL?.ENABLED) {
        this.logger.verbose(`Updating cache for group: ${groupJid}`);
        await groupMetadataCache.set(groupJid, { timestamp: Date.now(), data: meta });
      }

      return meta;
    } catch (error) {
      this.logger.error(error);
      return null;
    }
  }

  private getGroupMetadataCache = async (groupJid: string) => {
    if (!isJidGroup(groupJid)) return null;

    const cacheConf = this.configService.get<CacheConf>('CACHE');

    if ((cacheConf?.REDIS?.ENABLED && cacheConf?.REDIS?.URI !== '') || cacheConf?.LOCAL?.ENABLED) {
      if (await groupMetadataCache?.has(groupJid)) {
        console.log(`Cache request for group: ${groupJid}`);
        const meta = await groupMetadataCache.get(groupJid);

        if (Date.now() - meta.timestamp > 3600000) {
          await this.updateGroupMetadataCache(groupJid);
        }

        return meta.data;
      }

      console.log(`Cache request for group: ${groupJid} - not found`);
      return await this.updateGroupMetadataCache(groupJid);
    }

    return await this.findGroup({ groupJid }, 'inner');
  };

  public async createGroup(create: CreateGroupDto) {
    try {
      const participants = (await this.whatsappNumber({ numbers: create.participants }))
        .filter((participant) => participant.exists)
        .map((participant) => participant.jid);
      const { id } = await this.client.groupCreate(create.subject, participants);

      if (create?.description) {
        await this.client.groupUpdateDescription(id, create.description);
      }

      if (create?.promoteParticipants) {
        await this.updateGParticipant({ groupJid: id, action: 'promote', participants: participants });
      }

      const group = await this.client.groupMetadata(id);

      return group;
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException('Error creating group', error.toString());
    }
  }

  public async updateGroupPicture(picture: GroupPictureDto) {
    try {
      let pic: WAMediaUpload;
      if (isURL(picture.image)) {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(picture.image);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        let config: any = { responseType: 'arraybuffer' };

        if (this.localProxy?.enabled) {
          config = {
            ...config,
            httpsAgent: makeProxyAgent({
              host: this.localProxy.host,
              port: this.localProxy.port,
              protocol: this.localProxy.protocol,
              username: this.localProxy.username,
              password: this.localProxy.password,
            }),
          };
        }

        pic = (await axios.get(url, config)).data;
      } else if (isBase64(picture.image)) {
        pic = Buffer.from(picture.image, 'base64');
      } else {
        throw new BadRequestException('"profilePicture" must be a url or a base64');
      }
      await this.client.updateProfilePicture(picture.groupJid, pic);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error update group picture', error.toString());
    }
  }

  public async updateGroupSubject(data: GroupSubjectDto) {
    try {
      await this.client.groupUpdateSubject(data.groupJid, data.subject);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating group subject', error.toString());
    }
  }

  public async updateGroupDescription(data: GroupDescriptionDto) {
    try {
      await this.client.groupUpdateDescription(data.groupJid, data.description);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating group description', error.toString());
    }
  }

  public async findGroup(id: GroupJid, reply: 'inner' | 'out' = 'out') {
    try {
      const group = await this.client.groupMetadata(id.groupJid);

      if (!group) {
        this.logger.error('Group not found');
        return null;
      }

      const picture = await this.profilePicture(group.id);

      return {
        id: group.id,
        subject: group.subject,
        subjectOwner: group.subjectOwner,
        subjectTime: group.subjectTime,
        pictureUrl: picture.profilePictureUrl,
        size: group.participants.length,
        creation: group.creation,
        owner: group.owner,
        desc: group.desc,
        descId: group.descId,
        restrict: group.restrict,
        announce: group.announce,
        participants: group.participants,
        isCommunity: group.isCommunity,
        isCommunityAnnounce: group.isCommunityAnnounce,
        linkedParent: group.linkedParent,
      };
    } catch (error) {
      if (reply === 'inner') {
        return;
      }
      throw new NotFoundException('Error fetching group', error.toString());
    }
  }

  public async fetchAllGroups(getParticipants: GetParticipant) {
    const fetch = Object.values(await this?.client?.groupFetchAllParticipating());

    let groups = [];
    for (const group of fetch) {
      const picture = await this.profilePicture(group.id);

      const result = {
        id: group.id,
        subject: group.subject,
        subjectOwner: group.subjectOwner,
        subjectTime: group.subjectTime,
        pictureUrl: picture?.profilePictureUrl,
        size: group.participants.length,
        creation: group.creation,
        owner: group.owner,
        desc: group.desc,
        descId: group.descId,
        restrict: group.restrict,
        announce: group.announce,
        isCommunity: group.isCommunity,
        isCommunityAnnounce: group.isCommunityAnnounce,
        linkedParent: group.linkedParent,
      };

      if (getParticipants.getParticipants == 'true') {
        result['participants'] = group.participants;
      }

      groups = [...groups, result];
    }

    return groups;
  }

  public async inviteCode(id: GroupJid) {
    try {
      const code = await this.client.groupInviteCode(id.groupJid);
      return { inviteUrl: `https://chat.whatsapp.com/${code}`, inviteCode: code };
    } catch (error) {
      throw new NotFoundException('No invite code', error.toString());
    }
  }

  public async inviteInfo(id: GroupInvite) {
    try {
      return await this.client.groupGetInviteInfo(id.inviteCode);
    } catch (error) {
      throw new NotFoundException('No invite info', id.inviteCode);
    }
  }

  public async sendInvite(id: GroupSendInvite) {
    try {
      const inviteCode = await this.inviteCode({ groupJid: id.groupJid });

      const inviteUrl = inviteCode.inviteUrl;

      const numbers = id.numbers.map((number) => createJid(number));
      const description = id.description ?? '';

      const msg = `${description}\n\n${inviteUrl}`;

      const message = { conversation: msg };

      for await (const number of numbers) {
        await this.sendMessageWithTyping(number, message);
      }

      return { send: true, inviteUrl };
    } catch (error) {
      throw new NotFoundException('No send invite');
    }
  }

  public async acceptInviteCode(id: AcceptGroupInvite) {
    try {
      const groupJid = await this.client.groupAcceptInvite(id.inviteCode);
      return { accepted: true, groupJid: groupJid };
    } catch (error) {
      throw new NotFoundException('Accept invite error', error.toString());
    }
  }

  public async revokeInviteCode(id: GroupJid) {
    try {
      const inviteCode = await this.client.groupRevokeInvite(id.groupJid);
      return { revoked: true, inviteCode };
    } catch (error) {
      throw new NotFoundException('Revoke error', error.toString());
    }
  }

  public async findParticipants(id: GroupJid) {
    try {
      const participants = (await this.client.groupMetadata(id.groupJid)).participants;
      const contacts = await this.prismaRepository.contact.findMany({
        where: { instanceId: this.instanceId, remoteJid: { in: participants.map((p) => p.id) } },
      });
      const parsedParticipants = participants.map((participant) => {
        const contact = contacts.find((c) => c.remoteJid === participant.id);
        return {
          ...participant,
          name: participant.name ?? contact?.pushName,
          imgUrl: participant.imgUrl ?? contact?.profilePicUrl,
        };
      });

      const usersContacts = parsedParticipants.filter((c) => c.id.includes('@s.whatsapp'));
      if (usersContacts) {
        await saveOnWhatsappCache(usersContacts.map((c) => ({ remoteJid: c.id })));
      }

      return { participants: parsedParticipants };
    } catch (error) {
      console.error(error);
      throw new NotFoundException('No participants', error.toString());
    }
  }

  public async updateGParticipant(update: GroupUpdateParticipantDto) {
    try {
      const participants = update.participants.map((p) => createJid(p));
      const updateParticipants = await this.client.groupParticipantsUpdate(
        update.groupJid,
        participants,
        update.action,
      );
      return { updateParticipants: updateParticipants };
    } catch (error) {
      throw new BadRequestException('Error updating participants', error.toString());
    }
  }

  public async updateGSetting(update: GroupUpdateSettingDto) {
    try {
      const updateSetting = await this.client.groupSettingUpdate(update.groupJid, update.action);
      return { updateSetting: updateSetting };
    } catch (error) {
      throw new BadRequestException('Error updating setting', error.toString());
    }
  }

  public async toggleEphemeral(update: GroupToggleEphemeralDto) {
    try {
      await this.client.groupToggleEphemeral(update.groupJid, update.expiration);
      return { success: true };
    } catch (error) {
      throw new BadRequestException('Error updating setting', error.toString());
    }
  }

  public async leaveGroup(id: GroupJid) {
    try {
      await this.client.groupLeave(id.groupJid);
      return { groupJid: id.groupJid, leave: true };
    } catch (error) {
      throw new BadRequestException('Unable to leave the group', error.toString());
    }
  }

  public async templateMessage() {
    throw new Error('Method not available in the Baileys service');
  }

  /**
   * Normaliza o remoteJid para SEMPRE usar o número real (@s.whatsapp.net)
   * Prioriza: remoteJidAlt > senderPn > remoteJid (se não for LID) > LID (último caso)
   * Isso evita duplicação de conversas quando mensagens vêm pelo LID e são enviadas pelo número real
   */
  /**
   * Resolve LID para PN usando cache Redis + Baileys lidMapping com timeout
   * @param lid - LID sem o @lid (ex: "40424601321473")
   * @param timeoutMs - Timeout em milissegundos (padrão: 500ms)
   * @returns Phone Number ou null se não conseguir resolver
   */
  private async resolveLIDToPN(lid: string, timeoutMs: number = 500): Promise<string | null> {
    try {
      // 1. Verifica cache Redis primeiro (rápido!)
      const cachedPN = await this.lidMappingService.getPNForLID(lid);
      if (cachedPN) {
        return cachedPN;
      }

      // 2. Se não está no cache, tenta buscar do Baileys com timeout
      const pnPromise = this.getPNFromBaileys(lid);
      const timeoutPromise = new Promise<null>((resolve) => setTimeout(() => resolve(null), timeoutMs));

      const pn = await Promise.race([pnPromise, timeoutPromise]);

      // 3. Se conseguiu resolver, salva no cache para próximas vezes
      if (pn) {
        await this.lidMappingService.storeLIDPNMapping(lid, pn);
        return pn;
      }

      return null;
    } catch (error) {
      this.logger.warn(`[resolveLIDToPN] Error resolving LID ${lid}: ${error?.message || error}`);
      return null;
    }
  }

  /**
   * Busca PN do Baileys signalRepository
   */
  private async getPNFromBaileys(lid: string): Promise<string | null> {
    try {
      // @ts-ignore - lidMapping disponível em Baileys v6.7.19+
      if (this.client?.signalRepository?.lidMapping?.getPNForLID) {
        // @ts-ignore
        const pn = await this.client.signalRepository.lidMapping.getPNForLID(lid);
        return pn || null;
      }
      return null;
    } catch (error) {
      this.logger.verbose(`[getPNFromBaileys] Failed: ${error?.message || error}`);
      return null;
    }
  }

  /**
   * Resolve um JID que pode ser LID para PN
   * Retorna o JID original se não conseguir resolver
   */
  private async resolveJID(jid: string | undefined | null): Promise<string | undefined | null> {
    if (!jid) return jid;

    if (jid.includes('@lid')) {
      const lidValue = jid.replace('@lid', '');
      const pn = await this.resolveLIDToPN(lidValue);

      if (pn) {
        const resolved = `${pn}@s.whatsapp.net`;
        this.logger.verbose(`[resolveJID] ${jid} -> ${resolved}`);
        return resolved;
      }
    }

    return jid;
  }

  /**
   * Resolve LIDs em uma mensagem ANTES do processamento
   * Modifica a mensagem in-place para substituir LID por PN quando possível
   */
  private async resolveLIDsInMessage(message: proto.IWebMessageInfo): Promise<void> {
    try {
      const key = message.key;
      if (!key) return;

      // Resolve remoteJid se for LID
      if (key.remoteJid) {
        const resolved = await this.resolveJID(key.remoteJid);
        if (resolved && resolved !== key.remoteJid) {
          key.remoteJid = resolved;
        }
      }

      // Resolve participant se for LID
      if (key.participant) {
        const resolved = await this.resolveJID(key.participant);
        if (resolved && resolved !== key.participant) {
          key.participant = resolved;
        }
      }
    } catch (error) {
      this.logger.warn(`[resolveLIDsInMessage] Error: ${error?.message || error}`);
    }
  }

  /**
   * Resolve LIDs em um contato
   */
  private async resolveLIDsInContact(contact: Partial<Contact>): Promise<void> {
    try {
      if (contact.id) {
        const resolved = await this.resolveJID(contact.id);
        if (resolved && resolved !== contact.id) {
          this.logger.verbose(`[resolveLIDsInContact] ${contact.id} -> ${resolved}`);
          contact.id = resolved;
        }
      }
    } catch (error) {
      this.logger.warn(`[resolveLIDsInContact] Error: ${error?.message || error}`);
    }
  }

  /**
   * Resolve LIDs em um chat
   */
  private async resolveLIDsInChat(chat: Chat): Promise<void> {
    try {
      if (chat.id) {
        const resolved = await this.resolveJID(chat.id);
        if (resolved && resolved !== chat.id) {
          this.logger.verbose(`[resolveLIDsInChat] ${chat.id} -> ${resolved}`);
          chat.id = resolved;
        }
      }
    } catch (error) {
      this.logger.warn(`[resolveLIDsInChat] Error: ${error?.message || error}`);
    }
  }

  private normalizeRemoteJid(key: any): string | null {
    // Validação inicial: verifica se key existe
    if (!key) {
      this.logger.warn('[normalizeRemoteJid] Key is null or undefined');
      return null;
    }

    const remoteJid = key?.remoteJid;
    const remoteJidAlt = key?.remoteJidAlt;
    const senderPn = key?.senderPn; // Número real do remetente (quando disponível)

    // Se não tem nenhum, retorna null
    if (!remoteJid && !remoteJidAlt && !senderPn) {
      this.logger.warn('[normalizeRemoteJid] No remoteJid found in key');
      return null;
    }

    // Para grupos e broadcast, SEMPRE mantém o remoteJid original
    if (remoteJid && typeof remoteJid === 'string' && (isJidGroup(remoteJid) || isJidBroadcast(remoteJid))) {
      return remoteJid;
    }

    // ESTRATÉGIA: SEMPRE priorizar números reais (@s.whatsapp.net) sobre LID

    // PRIORIDADE 1: remoteJidAlt (se for número real)
    if (remoteJidAlt && typeof remoteJidAlt === 'string' && isPnUser(remoteJidAlt)) {
      this.logger.debug(`[normalizeRemoteJid] Using remoteJidAlt: ${remoteJid} -> ${remoteJidAlt}`);
      return remoteJidAlt;
    }

    // PRIORIDADE 2: senderPn (se for número real)
    if (senderPn && typeof senderPn === 'string' && isPnUser(senderPn)) {
      this.logger.debug(`[normalizeRemoteJid] Using senderPn: ${remoteJid} -> ${senderPn}`);
      return senderPn;
    }

    // PRIORIDADE 3: remoteJid (se JÁ for número real, não LID)
    if (remoteJid && typeof remoteJid === 'string' && isPnUser(remoteJid)) {
      return remoteJid;
    }

    // PRIORIDADE 4: Se remoteJid é LID, tenta usar remoteJidAlt (qualquer formato)
    if (remoteJid && typeof remoteJid === 'string' && isLidUser(remoteJid)) {
      if (remoteJidAlt && typeof remoteJidAlt === 'string') {
        this.logger.debug(`[normalizeRemoteJid] LID with alt: ${remoteJid} -> ${remoteJidAlt}`);
        return remoteJidAlt;
      }
      // Fallback: usa LID se não tiver nenhuma alternativa
      this.logger.warn(`[normalizeRemoteJid] LID without alternatives: ${remoteJid}`);
      return remoteJid;
    }

    // PRIORIDADE 5: Usa remoteJid (outros formatos válidos)
    if (remoteJid && typeof remoteJid === 'string') {
      return remoteJid;
    }

    // Fallback final: remoteJidAlt (qualquer formato) ou null
    return remoteJidAlt || null;
  }

  private prepareMessage(message: proto.IWebMessageInfo): any {
    const contentType = getContentType(message.message);
    const contentMsg = message?.message[contentType] as any;

    // Normaliza o remoteJid para evitar duplicação de conversas
    // Protege contra message.key null/undefined
    const normalizedRemoteJid = this.normalizeRemoteJid(message.key);
    const normalizedKey = {
      ...message.key,
      remoteJid: normalizedRemoteJid || message.key?.remoteJid || 'unknown',
    };

    const messageRaw = {
      key: normalizedKey,
      pushName:
        message.pushName ||
        (message.key.fromMe
          ? 'Você'
          : message?.participant || (message.key?.participant ? message.key.participant.split('@')[0] : null)),
      status: status[message.status],
      message: { ...message.message },
      contextInfo: contentMsg?.contextInfo,
      messageType: contentType || 'unknown',
      messageTimestamp: message.messageTimestamp as number,
      instanceId: this.instanceId,
      source: getDevice(message.key.id),
    };

    if (!messageRaw.status && message.key.fromMe === false) {
      messageRaw.status = status[3]; // DELIVERED MESSAGE
    }

    if (messageRaw.message.extendedTextMessage) {
      messageRaw.messageType = 'conversation';
      messageRaw.message.conversation = messageRaw.message.extendedTextMessage.text;
      delete messageRaw.message.extendedTextMessage;
    }

    if (messageRaw.message.documentWithCaptionMessage) {
      messageRaw.messageType = 'documentMessage';
      messageRaw.message.documentMessage = messageRaw.message.documentWithCaptionMessage.message.documentMessage;
      delete messageRaw.message.documentWithCaptionMessage;
    }

    const quotedMessage = messageRaw?.contextInfo?.quotedMessage;
    if (quotedMessage) {
      if (quotedMessage.extendedTextMessage) {
        quotedMessage.conversation = quotedMessage.extendedTextMessage.text;
        delete quotedMessage.extendedTextMessage;
      }

      if (quotedMessage.documentWithCaptionMessage) {
        quotedMessage.documentMessage = quotedMessage.documentWithCaptionMessage.message.documentMessage;
        delete quotedMessage.documentWithCaptionMessage;
      }
    }

    return messageRaw;
  }

  private async syncChatwootLostMessages() {
    if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
      const chatwootConfig = await this.findChatwoot();
      const prepare = (message: any) => this.prepareMessage(message);
      this.chatwootService.syncLostMessages({ instanceName: this.instance.name }, chatwootConfig, prepare);

      const task = cron.schedule('0,30 * * * *', async () => {
        this.chatwootService.syncLostMessages({ instanceName: this.instance.name }, chatwootConfig, prepare);
      });
      task.start();
    }
  }

  private async updateMessagesReadedByTimestamp(remoteJid: string, timestamp?: number): Promise<number> {
    if (timestamp === undefined || timestamp === null) return 0;

    const result = await this.prismaRepository.message.updateMany({
      where: {
        AND: [
          { key: { path: ['remoteJid'], equals: remoteJid } },
          { key: { path: ['fromMe'], equals: false } },
          { messageTimestamp: { lte: timestamp } },
          { OR: [{ status: null }, { status: status[3] }] },
        ],
      },
      data: { status: status[4] },
    });

    if (result) {
      if (result.count > 0) {
        this.updateChatUnreadMessages(remoteJid);
      }

      return result.count;
    }

    return 0;
  }

  private async updateChatUnreadMessages(remoteJid: string): Promise<number> {
    try {
      const [chat, unreadMessages] = await Promise.all([
        this.prismaRepository.chat.findFirst({
          where: {
            remoteJid,
            instanceId: this.instanceId
          }
        }),
        this.prismaRepository.message.count({
          where: {
            AND: [
              { instanceId: this.instanceId },
              { key: { path: ['remoteJid'], equals: remoteJid } },
              { key: { path: ['fromMe'], equals: false } },
              { status: { equals: status[3] } },
            ],
          },
        }),
      ]);

      if (chat && chat.unreadMessages !== unreadMessages) {
        await this.prismaRepository.chat.update({ where: { id: chat.id }, data: { unreadMessages } });
      }

      return unreadMessages;
    } catch (error) {
      this.logger.error('Error updating chat unread messages:');
      this.logger.error(error);
      return 0;
    }
  }

  private async addLabel(labelId: string, instanceId: string, chatId: string) {
    const id = cuid();

    await this.prismaRepository.$executeRawUnsafe(
      `INSERT INTO "Chat" ("id", "instanceId", "remoteJid", "labels", "createdAt", "updatedAt")
       VALUES ($4, $2, $3, to_jsonb(ARRAY[$1]::text[]), NOW(), NOW()) ON CONFLICT ("instanceId", "remoteJid")
     DO
      UPDATE
          SET "labels" = (
          SELECT to_jsonb(array_agg(DISTINCT elem))
          FROM (
          SELECT jsonb_array_elements_text("Chat"."labels") AS elem
          UNION
          SELECT $1::text AS elem
          ) sub
          ),
          "updatedAt" = NOW();`,
      labelId,
      instanceId,
      chatId,
      id,
    );
  }

  private async removeLabel(labelId: string, instanceId: string, chatId: string) {
    const id = cuid();

    await this.prismaRepository.$executeRawUnsafe(
      `INSERT INTO "Chat" ("id", "instanceId", "remoteJid", "labels", "createdAt", "updatedAt")
       VALUES ($4, $2, $3, '[]'::jsonb, NOW(), NOW()) ON CONFLICT ("instanceId", "remoteJid")
     DO
      UPDATE
          SET "labels" = COALESCE (
          (
          SELECT jsonb_agg(elem)
          FROM jsonb_array_elements_text("Chat"."labels") AS elem
          WHERE elem <> $1
          ),
          '[]'::jsonb
          ),
          "updatedAt" = NOW();`,
      labelId,
      instanceId,
      chatId,
      id,
    );
  }

  public async baileysOnWhatsapp(jid: string) {
    const response = await this.client.onWhatsApp(jid);

    return response;
  }

  public async baileysProfilePictureUrl(jid: string, type: 'image' | 'preview', timeoutMs: number) {
    const response = await this.client.profilePictureUrl(jid, type, timeoutMs);

    return response;
  }

  public async baileysAssertSessions(jids: string[], force: boolean = false) {
    const response = await this.client.assertSessions(jids, force);

    return response;
  }

  public async baileysCreateParticipantNodes(jids: string[], message: proto.IMessage, extraAttrs: any) {
    const response = await this.client.createParticipantNodes(jids, message, extraAttrs);

    const convertedResponse = {
      ...response,
      nodes: response.nodes.map((node: any) => ({
        ...node,
        content: node.content?.map((c: any) => ({
          ...c,
          content: c.content instanceof Uint8Array ? Buffer.from(c.content).toString('base64') : c.content,
        })),
      })),
    };

    return convertedResponse;
  }

  public async baileysSendNode(stanza: any) {
    console.log('stanza', JSON.stringify(stanza));
    const response = await this.client.sendNode(stanza);

    return response;
  }

  public async baileysGetUSyncDevices(jids: string[], useCache: boolean, ignoreZeroDevices: boolean) {
    const response = await this.client.getUSyncDevices(jids, useCache, ignoreZeroDevices);

    return response;
  }

  public async baileysGenerateMessageTag() {
    const response = await this.client.generateMessageTag();

    return response;
  }

  public async baileysSignalRepositoryDecryptMessage(jid: string, type: 'pkmsg' | 'msg', ciphertext: string) {
    try {
      const ciphertextBuffer = Buffer.from(ciphertext, 'base64');

      const response = await this.client.signalRepository.decryptMessage({ jid, type, ciphertext: ciphertextBuffer });

      return response instanceof Uint8Array ? Buffer.from(response).toString('base64') : response;
    } catch (error) {
      this.logger.error('Error decrypting message:');
      this.logger.error(error);
      throw error;
    }
  }

  public async baileysGetAuthState() {
    const response = { me: this.client.authState.creds.me, account: this.client.authState.creds.account };

    return response;
  }

  private async handleSessionError(jid: string, errorMessage: string): Promise<void> {
    const now = Date.now();
    const cached = this.sessionErrorCache.get(jid);

    // Reset counter if last error was more than 5 minutes ago
    if (cached && now - cached.lastError > 300000) {
      this.sessionErrorCache.delete(jid);
    }

    const errorCount = cached ? cached.count + 1 : 1;
    this.pruneMapCache(this.sessionErrorCache, this.MAX_SESSION_ERROR_CACHE);
    this.sessionErrorCache.set(jid, { count: errorCount, lastError: now });

    // Auto-clear threshold: 3 errors within 5 minutes
    const threshold = 3;

    if (errorCount >= threshold) {
      this.logger.warn(
        `Session error threshold (${threshold}) reached for ${jid}. Auto-clearing session. Error: ${errorMessage}`,
      );

      try {
        await this.baileysClearSessions([jid]);
        this.sessionErrorCache.delete(jid); // Reset counter after clearing
        this.logger.info(`Successfully auto-cleared session for ${jid}`);
      } catch (error) {
        this.logger.error(`Failed to auto-clear session for ${jid}:`);
        this.logger.error(error);
      }
    } else {
      this.logger.warn(`Session error ${errorCount}/${threshold} for ${jid}: ${errorMessage}`);
    }
  }

  public async baileysClearSessions(jids: string[]) {
    if (!jids || !Array.isArray(jids) || jids.length === 0) {
      throw new BadRequestException('JIDs array is required and must not be empty');
    }

    const clearedSessions = [];
    const errors = [];

    for (const jid of jids) {
      try {
        // Clear all session-related keys for this JID by setting them to undefined
        const keysToRemove = {
          session: { [jid]: undefined },
          'sender-key': { [jid]: undefined },
          'sender-key-memory': { [jid]: undefined },
        };

        // Use the auth state's keys.set method to remove the session data
        await this.instance.authState.state.keys.set(keysToRemove);

        this.logger.info(`Cleared session for ${jid}`);
        clearedSessions.push(jid);
      } catch (error) {
        this.logger.error(`Failed to clear session for ${jid}:`);
        this.logger.error(error);
        errors.push({ jid, error: error.message });
      }
    }

    // Force reassert sessions to establish new ones
    try {
      await this.client.assertSessions(jids, true);
      this.logger.info(`Reasserted sessions for ${jids.length} contacts`);
    } catch (error) {
      this.logger.warn('Failed to reassert sessions:');
      this.logger.warn(error);
    }

    return {
      success: clearedSessions.length > 0,
      clearedSessions,
      errors: errors.length > 0 ? errors : undefined,
      message: `Cleared ${clearedSessions.length} session(s). ${errors.length > 0 ? `Failed to clear ${errors.length} session(s).` : ''}`,
    };
  }

  //Business Controller
  public async fetchCatalog(instanceName: string, data: getCollectionsDto) {
    const jid = data.number ? createJid(data.number) : this.client?.user?.id;
    const limit = data.limit || 10;
    const cursor = null;

    const onWhatsapp = (await this.whatsappNumber({ numbers: [jid] }))?.shift();

    if (!onWhatsapp.exists) {
      throw new BadRequestException(onWhatsapp);
    }

    try {
      const info = (await this.whatsappNumber({ numbers: [jid] }))?.shift();
      const business = await this.fetchBusinessProfile(info?.jid);

      let catalog = await this.getCatalog({ jid: info?.jid, limit, cursor });
      let nextPageCursor = catalog.nextPageCursor;
      let nextPageCursorJson = nextPageCursor ? JSON.parse(atob(nextPageCursor)) : null;
      let pagination = nextPageCursorJson?.pagination_cursor
        ? JSON.parse(atob(nextPageCursorJson.pagination_cursor))
        : null;
      let fetcherHasMore = pagination?.fetcher_has_more === true ? true : false;

      let productsCatalog = catalog.products || [];
      let countLoops = 0;
      while (fetcherHasMore && countLoops < 4) {
        catalog = await this.getCatalog({ jid: info?.jid, limit, cursor: nextPageCursor });
        nextPageCursor = catalog.nextPageCursor;
        nextPageCursorJson = nextPageCursor ? JSON.parse(atob(nextPageCursor)) : null;
        pagination = nextPageCursorJson?.pagination_cursor
          ? JSON.parse(atob(nextPageCursorJson.pagination_cursor))
          : null;
        fetcherHasMore = pagination?.fetcher_has_more === true ? true : false;
        productsCatalog = [...productsCatalog, ...catalog.products];
        countLoops++;
      }

      return {
        wuid: info?.jid || jid,
        numberExists: info?.exists,
        isBusiness: business.isBusiness,
        catalogLength: productsCatalog.length,
        catalog: productsCatalog,
      };
    } catch (error) {
      console.log(error);
      return { wuid: jid, name: null, isBusiness: false };
    }
  }

  public async getCatalog({
    jid,
    limit,
    cursor,
  }: GetCatalogOptions): Promise<{ products: Product[]; nextPageCursor: string | undefined }> {
    try {
      jid = jid ? createJid(jid) : this.instance.wuid;

      const catalog = await this.client.getCatalog({ jid, limit: limit, cursor: cursor });

      if (!catalog) {
        return { products: undefined, nextPageCursor: undefined };
      }

      return catalog;
    } catch (error) {
      throw new InternalServerErrorException('Error getCatalog', error.toString());
    }
  }

  public async fetchCollections(instanceName: string, data: getCollectionsDto) {
    const jid = data.number ? createJid(data.number) : this.client?.user?.id;
    const limit = data.limit <= 20 ? data.limit : 20; //(tem esse limite, não sei porque)

    const onWhatsapp = (await this.whatsappNumber({ numbers: [jid] }))?.shift();

    if (!onWhatsapp.exists) {
      throw new BadRequestException(onWhatsapp);
    }

    try {
      const info = (await this.whatsappNumber({ numbers: [jid] }))?.shift();
      const business = await this.fetchBusinessProfile(info?.jid);
      const collections = await this.getCollections(info?.jid, limit);

      return {
        wuid: info?.jid || jid,
        name: info?.name,
        numberExists: info?.exists,
        isBusiness: business.isBusiness,
        collectionsLength: collections?.length,
        collections: collections,
      };
    } catch (error) {
      return { wuid: jid, name: null, isBusiness: false };
    }
  }

  public async getCollections(jid?: string | undefined, limit?: number): Promise<CatalogCollection[]> {
    try {
      jid = jid ? createJid(jid) : this.instance.wuid;

      const result = await this.client.getCollections(jid, limit);

      if (!result) {
        return [{ id: undefined, name: undefined, products: [], status: undefined }];
      }

      return result.collections;
    } catch (error) {
      throw new InternalServerErrorException('Error getCatalog', error.toString());
    }
  }

  public async fetchMessages(query: Query<Message>) {
    const keyFilters = query?.where?.key as {
      id?: string;
      fromMe?: boolean;
      remoteJid?: string;
      participants?: string;
    };

    const timestampFilter = {};
    if (query?.where?.messageTimestamp) {
      if (query.where.messageTimestamp['gte'] && query.where.messageTimestamp['lte']) {
        timestampFilter['messageTimestamp'] = {
          gte: Math.floor(new Date(query.where.messageTimestamp['gte']).getTime() / 1000),
          lte: Math.floor(new Date(query.where.messageTimestamp['lte']).getTime() / 1000),
        };
      }
    }

    // Constrói filtro para remoteJid que busca tanto no remoteJid quanto no remoteJidAlt
    // Valida que remoteJid é uma string não vazia antes de criar o filtro
    const remoteJidFilter =
      keyFilters?.remoteJid && typeof keyFilters.remoteJid === 'string' && keyFilters.remoteJid.trim().length > 0
        ? {
            OR: [
              { key: { path: ['remoteJid'], equals: keyFilters.remoteJid } },
              { key: { path: ['remoteJidAlt'], equals: keyFilters.remoteJid } },
            ],
          }
        : {};

    const count = await this.prismaRepository.message.count({
      where: {
        instanceId: this.instanceId,
        id: query?.where?.id,
        source: query?.where?.source,
        messageType: query?.where?.messageType,
        ...timestampFilter,
        ...remoteJidFilter,
        AND: [
          keyFilters?.id ? { key: { path: ['id'], equals: keyFilters?.id } } : {},
          keyFilters?.fromMe ? { key: { path: ['fromMe'], equals: keyFilters?.fromMe } } : {},
          keyFilters?.participants ? { key: { path: ['participants'], equals: keyFilters?.participants } } : {},
        ],
      },
    });

    if (!query?.offset) {
      query.offset = 50;
    }

    if (!query?.page) {
      query.page = 1;
    }

    const messages = await this.prismaRepository.message.findMany({
      where: {
        instanceId: this.instanceId,
        id: query?.where?.id,
        source: query?.where?.source,
        messageType: query?.where?.messageType,
        ...timestampFilter,
        ...remoteJidFilter,
        AND: [
          keyFilters?.id ? { key: { path: ['id'], equals: keyFilters?.id } } : {},
          keyFilters?.fromMe ? { key: { path: ['fromMe'], equals: keyFilters?.fromMe } } : {},
          keyFilters?.participants ? { key: { path: ['participants'], equals: keyFilters?.participants } } : {},
        ],
      },
      orderBy: { messageTimestamp: 'desc' },
      skip: query.offset * (query?.page === 1 ? 0 : (query?.page as number) - 1),
      take: query.offset,
      select: {
        id: true,
        key: true,
        pushName: true,
        messageType: true,
        message: true,
        messageTimestamp: true,
        instanceId: true,
        source: true,
        contextInfo: true,
        MessageUpdate: { select: { status: true } },
      },
    });

    const formattedMessages = messages.map((message) => {
      const messageKey = message.key as { fromMe: boolean; remoteJid: string; id: string; participant?: string };

      if (!message.pushName) {
        if (messageKey.fromMe) {
          message.pushName = 'Você';
        } else if (message.contextInfo) {
          const contextInfo = message.contextInfo as { participant?: string };
          if (contextInfo.participant) {
            message.pushName = contextInfo.participant.split('@')[0];
          } else if (messageKey.participant) {
            message.pushName = messageKey.participant.split('@')[0];
          }
        }
      }

      return message;
    });

    return {
      messages: {
        total: count,
        pages: Math.ceil(count / query.offset),
        currentPage: query.page,
        records: formattedMessages,
      },
    };
  }
}
