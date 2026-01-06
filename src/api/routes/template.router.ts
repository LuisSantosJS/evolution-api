import { RouterBroker } from '@api/abstract/abstract.router';
import { InstanceDto } from '@api/dto/instance.dto';
import { DeleteTemplateDto, TemplateDto, UpdateTemplateDto } from '@api/dto/template.dto';
import { templateController } from '@api/server.module';
import { ConfigService } from '@config/env.config';
import { deleteTemplateSchema, instanceSchema, templateSchema, updateTemplateSchema } from '@validate/validate.schema';
import { RequestHandler, Router } from 'express';

import { HttpStatus } from './index.router';

export class TemplateRouter extends RouterBroker {
  constructor(
    readonly configService: ConfigService,
    ...guards: RequestHandler[]
  ) {
    super();
    this.router
      .post(this.routerPath('create'), ...guards, async (req, res) => {
        const response = await this.dataValidate<TemplateDto>({
          request: req,
          schema: templateSchema,
          ClassRef: TemplateDto,
          execute: (instance, data) => templateController.createTemplate(instance, data),
        });

        res.status(HttpStatus.CREATED).json(response);
      })
      .get(this.routerPath('find'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: instanceSchema,
          ClassRef: InstanceDto,
          execute: (instance) => templateController.findTemplate(instance),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .put(this.routerPath('update'), ...guards, async (req, res) => {
        const response = await this.dataValidate<UpdateTemplateDto>({
          request: req,
          schema: updateTemplateSchema,
          ClassRef: UpdateTemplateDto,
          execute: (instance, data) => templateController.updateTemplate(instance, data),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .delete(this.routerPath('delete'), ...guards, async (req, res) => {
        const response = await this.dataValidate<DeleteTemplateDto>({
          request: req,
          schema: deleteTemplateSchema,
          ClassRef: DeleteTemplateDto,
          execute: (instance, data) => templateController.deleteTemplate(instance, data),
        });

        res.status(HttpStatus.OK).json(response);
      });
  }

  public readonly router: Router = Router();
}
