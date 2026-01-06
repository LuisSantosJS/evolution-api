export class TemplateDto {
  name: string;
  category: string;
  allowCategoryChange: boolean;
  language: string;
  components: any;
  webhookUrl?: string;
}

export class UpdateTemplateDto {
  templateId: string;
  category?: string;
  components?: any;
}

export class DeleteTemplateDto {
  templateId: string;
  name: string;
}
