import { InstanceDto } from '@api/dto/instance.dto';
import { DeleteTemplateDto, TemplateDto, UpdateTemplateDto } from '@api/dto/template.dto';
import { PrismaRepository } from '@api/repository/repository.service';
import { ConfigService, WaBusiness } from '@config/env.config';
import { Logger } from '@config/logger.config';
import axios from 'axios';

import { WAMonitoringService } from './monitor.service';

export class TemplateService {
  constructor(
    private readonly waMonitor: WAMonitoringService,
    public readonly prismaRepository: PrismaRepository,
    private readonly configService: ConfigService,
  ) {}

  private readonly logger = new Logger('TemplateService');

  private businessId: string;
  private token: string;

  public async find(instance: InstanceDto) {
    const getInstance = await this.waMonitor.waInstances[instance.instanceName].instance;

    if (!getInstance) {
      throw new Error('Instance not found');
    }

    this.businessId = getInstance.businessId;
    this.token = getInstance.token;

    const response = await this.requestTemplate({}, 'GET');

    if (!response) {
      throw new Error('Error to find templates');
    }

    return response.data;
  }

  public async create(instance: InstanceDto, data: TemplateDto) {
    try {
      const getInstance = await this.waMonitor.waInstances[instance.instanceName].instance;

      if (!getInstance) {
        throw new Error('Instance not found');
      }

      this.businessId = getInstance.businessId;
      this.token = getInstance.token;

      const postData = {
        name: data.name,
        category: data.category,
        allow_category_change: data.allowCategoryChange,
        language: data.language,
        components: data.components,
      };

      const response = await this.requestTemplate(postData, 'POST');

      if (!response || response.error) {
        throw new Error('Error to create template');
      }

      const template = await this.prismaRepository.template.create({
        data: {
          templateId: response.id,
          name: data.name,
          template: response,
          webhookUrl: data.webhookUrl,
          instanceId: getInstance.id,
        },
      });

      return template;
    } catch (error) {
      this.logger.error(error);
      throw new Error('Error to create template');
    }
  }

  public async update(instance: InstanceDto, data: UpdateTemplateDto) {
    try {
      const getInstance = await this.waMonitor.waInstances[instance.instanceName].instance;

      if (!getInstance) {
        throw new Error('Instance not found');
      }

      this.businessId = getInstance.businessId;
      this.token = getInstance.token;

      const postData: any = {};
      if (data.category) {
        postData.category = data.category;
      }
      if (data.components) {
        postData.components = data.components;
      }

      const response = await this.requestTemplateById(data.templateId, postData, 'POST');

      if (!response || response.error) {
        throw new Error('Error to update template');
      }

      // Update local database
      const template = await this.prismaRepository.template.updateMany({
        where: {
          templateId: data.templateId,
          instanceId: getInstance.id,
        },
        data: {
          template: response,
        },
      });

      return { success: true, templateId: data.templateId, response };
    } catch (error) {
      this.logger.error(error);
      throw new Error('Error to update template');
    }
  }

  public async delete(instance: InstanceDto, data: DeleteTemplateDto) {
    try {
      const getInstance = await this.waMonitor.waInstances[instance.instanceName].instance;

      if (!getInstance) {
        throw new Error('Instance not found');
      }

      this.businessId = getInstance.businessId;
      this.token = getInstance.token;

      const response = await this.requestTemplateDelete(data.templateId, data.name);

      if (!response || response.error) {
        throw new Error('Error to delete template');
      }

      // Delete from local database
      await this.prismaRepository.template.deleteMany({
        where: {
          templateId: data.templateId,
          instanceId: getInstance.id,
        },
      });

      return { success: true, templateId: data.templateId, name: data.name };
    } catch (error) {
      this.logger.error(error);
      throw new Error('Error to delete template');
    }
  }

  private async requestTemplate(data: any, method: string) {
    try {
      let urlServer = this.configService.get<WaBusiness>('WA_BUSINESS').URL;
      const version = this.configService.get<WaBusiness>('WA_BUSINESS').VERSION;
      urlServer = `${urlServer}/${version}/${this.businessId}/message_templates`;
      const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${this.token}` };
      if (method === 'GET') {
        const result = await axios.get(urlServer, { headers });
        return result.data;
      } else if (method === 'POST') {
        const result = await axios.post(urlServer, data, { headers });
        return result.data;
      }
    } catch (e) {
      this.logger.error(e.response?.data);
      return e.response?.data?.error;
    }
  }

  private async requestTemplateById(templateId: string, data: any, method: string) {
    try {
      let urlServer = this.configService.get<WaBusiness>('WA_BUSINESS').URL;
      const version = this.configService.get<WaBusiness>('WA_BUSINESS').VERSION;
      urlServer = `${urlServer}/${version}/${templateId}`;
      const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${this.token}` };
      if (method === 'POST') {
        const result = await axios.post(urlServer, data, { headers });
        return result.data;
      }
    } catch (e) {
      this.logger.error(e.response?.data);
      return e.response?.data?.error;
    }
  }

  private async requestTemplateDelete(templateId: string, name: string) {
    try {
      let urlServer = this.configService.get<WaBusiness>('WA_BUSINESS').URL;
      const version = this.configService.get<WaBusiness>('WA_BUSINESS').VERSION;
      urlServer = `${urlServer}/${version}/${this.businessId}/message_templates?hsm_id=${templateId}&name=${name}`;
      const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${this.token}` };
      const result = await axios.delete(urlServer, { headers });
      return result.data;
    } catch (e) {
      this.logger.error(e.response?.data);
      return e.response?.data?.error;
    }
  }
}
