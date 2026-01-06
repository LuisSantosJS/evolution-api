import { InstanceDto } from '@api/dto/instance.dto';
import { DeleteTemplateDto, TemplateDto, UpdateTemplateDto } from '@api/dto/template.dto';
import { TemplateService } from '@api/services/template.service';

export class TemplateController {
  constructor(private readonly templateService: TemplateService) {}

  public async createTemplate(instance: InstanceDto, data: TemplateDto) {
    return this.templateService.create(instance, data);
  }

  public async findTemplate(instance: InstanceDto) {
    return this.templateService.find(instance);
  }

  public async updateTemplate(instance: InstanceDto, data: UpdateTemplateDto) {
    return this.templateService.update(instance, data);
  }

  public async deleteTemplate(instance: InstanceDto, data: DeleteTemplateDto) {
    return this.templateService.delete(instance, data);
  }
}
