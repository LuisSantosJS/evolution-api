import { ConfigService, Language } from '@config/env.config';
import fs from 'fs';
import i18next from 'i18next';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

// ESM equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const languages = ['en', 'pt-BR', 'es'];
const translationsPath = path.join(__dirname, 'translations');
const configService: ConfigService = new ConfigService();

const resources: any = {};

languages.forEach((language) => {
  const languagePath = path.join(translationsPath, `${language}.json`);
  if (fs.existsSync(languagePath)) {
    resources[language] = {
      translation: JSON.parse(fs.readFileSync(languagePath, 'utf8')),
    };
  }
});

i18next.init({
  resources,
  fallbackLng: 'en',
  lng: configService.get<Language>('LANGUAGE'),
  debug: false,

  interpolation: {
    escapeValue: false,
  },
});
export default i18next;
