import { InstanceDto } from '@api/dto/instance.dto';
import {
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
  SendTemplateDto,
  SendTextDto,
} from '@api/dto/sendMessage.dto';
import { WAMonitoringService } from '@api/services/monitor.service';
import { BadRequestException } from '@exceptions';
import { isBase64, isURL } from 'class-validator';
import emojiRegex from 'emoji-regex';

const regex = emojiRegex();

function isEmoji(str: string) {
  if (str === '') return true;

  const match = str.match(regex);
  return match?.length === 1 && match[0] === str;
}

export class SendMessageController {
  constructor(private readonly waMonitor: WAMonitoringService) {}

  /**
   * Verifica se a instância está pronta para enviar mídias
   * Usa verificação robusta que vai além do simples state === 'open'
   */
  private checkConnectionStatus(instanceName: string): void {
    const instance = this.waMonitor.waInstances[instanceName];
    if (!instance) {
      throw new BadRequestException(`Instance "${instanceName}" not found`);
    }

    // Usar o método robusto isConnectionReady() ao invés de apenas verificar o state
    if (typeof instance.isConnectionReady === 'function') {
      if (!instance.isConnectionReady()) {
        const connectionState = instance.connectionStatus?.state || 'unknown';
        throw new BadRequestException(
          `Instance is not ready to send media. Current state: ${connectionState}. The connection may be establishing or unstable. Please wait a moment and try again.`,
        );
      }
    } else {
      // Fallback para verificação antiga se o método não existir (compatibilidade)
      const connectionState = instance.connectionStatus?.state;
      if (connectionState !== 'open') {
        throw new BadRequestException(
          `Instance is not connected to WhatsApp. Current state: ${connectionState || 'unknown'}. Please wait for connection to be established before sending media.`,
        );
      }
    }
  }

  public async sendTemplate({ instanceName }: InstanceDto, data: SendTemplateDto) {
    return await this.waMonitor.waInstances[instanceName].templateMessage(data);
  }

  public async sendText({ instanceName }: InstanceDto, data: SendTextDto) {
    return await this.waMonitor.waInstances[instanceName].textMessage(data);
  }

  public async sendMedia({ instanceName }: InstanceDto, data: SendMediaDto, file?: any) {
    // Verificar conexão antes de processar mídia
    this.checkConnectionStatus(instanceName);

    if (isBase64(data?.media) && !data?.fileName && data?.mediatype === 'document') {
      throw new BadRequestException('For base64 the file name must be informed.');
    }

    if (file || isURL(data?.media) || isBase64(data?.media)) {
      return await this.waMonitor.waInstances[instanceName].mediaMessage(data, file);
    }
    throw new BadRequestException('Owned media must be a url or base64');
  }

  public async sendPtv({ instanceName }: InstanceDto, data: SendPtvDto, file?: any) {
    // Verificar conexão antes de processar mídia
    this.checkConnectionStatus(instanceName);

    if (file || isURL(data?.video) || isBase64(data?.video)) {
      return await this.waMonitor.waInstances[instanceName].ptvMessage(data, file);
    }
    throw new BadRequestException('Owned media must be a url or base64');
  }

  public async sendSticker({ instanceName }: InstanceDto, data: SendStickerDto, file?: any) {
    // Verificar conexão antes de processar mídia
    this.checkConnectionStatus(instanceName);

    if (file || isURL(data.sticker) || isBase64(data.sticker)) {
      return await this.waMonitor.waInstances[instanceName].mediaSticker(data, file);
    }
    throw new BadRequestException('Owned media must be a url or base64');
  }

  public async sendWhatsAppAudio({ instanceName }: InstanceDto, data: SendAudioDto, file?: any) {
    // Verificar conexão antes de processar mídia
    this.checkConnectionStatus(instanceName);

    if (file?.buffer || isURL(data.audio) || isBase64(data.audio)) {
      // Si file existe y tiene buffer, o si es una URL o Base64, continúa
      return await this.waMonitor.waInstances[instanceName].audioWhatsapp(data, file);
    } else {
      console.error('El archivo no tiene buffer o el audio no es una URL o Base64 válida');
      throw new BadRequestException('Owned media must be a url, base64, or valid file with buffer');
    }
  }

  public async sendButtons({ instanceName }: InstanceDto, data: SendButtonsDto) {
    return await this.waMonitor.waInstances[instanceName].buttonMessage(data);
  }

  public async sendLocation({ instanceName }: InstanceDto, data: SendLocationDto) {
    return await this.waMonitor.waInstances[instanceName].locationMessage(data);
  }

  public async sendList({ instanceName }: InstanceDto, data: SendListDto) {
    return await this.waMonitor.waInstances[instanceName].listMessage(data);
  }

  public async sendContact({ instanceName }: InstanceDto, data: SendContactDto) {
    return await this.waMonitor.waInstances[instanceName].contactMessage(data);
  }

  public async sendReaction({ instanceName }: InstanceDto, data: SendReactionDto) {
    if (!isEmoji(data.reaction)) {
      throw new BadRequestException('Reaction must be a single emoji or empty string');
    }
    return await this.waMonitor.waInstances[instanceName].reactionMessage(data);
  }

  public async sendPoll({ instanceName }: InstanceDto, data: SendPollDto) {
    return await this.waMonitor.waInstances[instanceName].pollMessage(data);
  }

  public async sendStatus({ instanceName }: InstanceDto, data: SendStatusDto, file?: any) {
    // Verificar conexão antes de processar mídia
    this.checkConnectionStatus(instanceName);

    return await this.waMonitor.waInstances[instanceName].statusMessage(data, file);
  }
}
