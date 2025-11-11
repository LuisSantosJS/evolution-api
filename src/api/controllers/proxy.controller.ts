import { InstanceDto } from '@api/dto/instance.dto';
import { ProxyDto } from '@api/dto/proxy.dto';
import { WAMonitoringService } from '@api/services/monitor.service';
import { ProxyService } from '@api/services/proxy.service';
import { Logger } from '@config/logger.config';
import { BadRequestException, NotFoundException } from '@exceptions';
import { makeProxyAgent } from '@utils/makeProxyAgent';
import axios from 'axios';

const logger = new Logger('ProxyController');

export class ProxyController {
  constructor(
    private readonly proxyService: ProxyService,
    private readonly waMonitor: WAMonitoringService,
  ) {}

  public async createProxy(instance: InstanceDto, data: ProxyDto) {
    if (!this.waMonitor.waInstances[instance.instanceName]) {
      throw new NotFoundException(`The "${instance.instanceName}" instance does not exist`);
    }

    if (!data?.enabled) {
      data.host = '';
      data.port = '';
      data.protocol = '';
      data.username = '';
      data.password = '';
    }

    if (data.host) {
      const testProxy = await this.testProxy(data);
      if (!testProxy) {
        throw new BadRequestException('Invalid proxy');
      }
    }

    const result = await this.proxyService.create(instance, data);

    // Se a instância não está conectada, reinicia para aplicar o proxy e gerar novo QR
    const waInstance = this.waMonitor.waInstances[instance.instanceName];
    const connectionState = waInstance?.connectionStatus?.state;

    if (connectionState === 'close' && data?.enabled) {
      logger.info(`Instance ${instance.instanceName} is disconnected. Reconnecting with new proxy...`);
      try {
        await waInstance.connectToWhatsapp();

        // Aguarda até 15 segundos para QR code ser gerado
        const maxAttempts = 30; // 15 segundos (500ms * 30)
        let attempts = 0;
        let qrCode;

        while (attempts < maxAttempts) {
          qrCode = waInstance.qrCode;
          if (qrCode?.base64) {
            break;
          }
          await new Promise(resolve => setTimeout(resolve, 500));
          attempts++;
        }

        return {
          ...result,
          qrcode: qrCode,
          message: 'Proxy configured and instance reconnected successfully',
        };
      } catch (error) {
        logger.error(`Failed to reconnect instance with new proxy: ${error}`);
        return {
          ...result,
          message: 'Proxy configured but failed to reconnect. Please restart instance manually.',
        };
      }
    }

    return result;
  }

  public async findProxy(instance: InstanceDto) {
    if (!this.waMonitor.waInstances[instance.instanceName]) {
      throw new NotFoundException(`The "${instance.instanceName}" instance does not exist`);
    }

    return this.proxyService.find(instance);
  }

  public async testProxy(proxy: ProxyDto) {
    try {
      const serverIp = await axios.get('https://icanhazip.com/');
      const response = await axios.get('https://icanhazip.com/', {
        httpsAgent: makeProxyAgent(proxy),
      });

      const result = response?.data !== serverIp?.data;
      if (result) {
        logger.info('testProxy: proxy connection successful');
      } else {
        logger.warn("testProxy: proxy connection doesn't change the origin IP");
      }

      return result;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        logger.error('testProxy error: axios error: ' + error.message);
      } else {
        logger.error('testProxy error: unexpected error: ' + error);
      }

      return false;
    }
  }
}
