import { Logger } from './logger.config';

export function onUnexpectedError() {
  process.on('uncaughtException', (error: any, origin) => {
    // Suprimir erros conhecidos do Baileys relacionados a arquivos temporários
    // Esses erros são esperados e já tratados pelo sistema de retry
    if (error?.code === 'ENOENT' && error?.path?.includes('-enc')) {
      // Logar apenas em modo verbose para debug, mas não como erro crítico
      const logger = new Logger('uncaughtException');
      logger.verbose(`Baileys temporary file error (suppressed): ${error.message} - ${error.path}`);
      return; // Não deixar o erro crashar a aplicação
    }

    const logger = new Logger('uncaughtException');
    logger.error({
      origin,
      stderr: process.stderr.fd,
      error,
    });
  });

  process.on('unhandledRejection', (error, origin) => {
    const logger = new Logger('unhandledRejection');
    logger.error({
      origin,
      stderr: process.stderr.fd,
    });
    logger.error(error);
  });
}
