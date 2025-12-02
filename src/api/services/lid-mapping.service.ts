import { Logger } from '@config/logger.config';
import { CacheService } from './cache.service';

/**
 * Serviço para gerenciar o mapeamento de LID (Limited ID) para PN (Phone Number)
 * usando cache Redis para melhorar performance
 */
export class LidMappingService {
  private readonly logger = new Logger('LidMappingService');
  private readonly CACHE_PREFIX = 'lid:mapping:';
  private readonly CACHE_TTL = 7 * 24 * 60 * 60; // 7 dias em segundos

  constructor(private readonly cache: CacheService) {}

  /**
   * Obtém o Phone Number a partir de um LID, usando cache
   */
  async getPNForLID(lid: string): Promise<string | null> {
    try {
      const cacheKey = `${this.CACHE_PREFIX}${lid}`;

      // Tenta buscar no cache primeiro
      const cached = await this.cache.get(cacheKey);
      if (cached) {
        this.logger.verbose(`LID mapping found in cache: ${lid} -> ${cached}`);
        return cached;
      }

      return null;
    } catch (error) {
      this.logger.warn(`Error getting LID mapping from cache: ${error?.message || error}`);
      return null;
    }
  }

  /**
   * Armazena o mapeamento LID -> PN no cache
   */
  async storeLIDPNMapping(lid: string, pn: string): Promise<void> {
    try {
      const cacheKey = `${this.CACHE_PREFIX}${lid}`;
      await this.cache.set(cacheKey, pn, this.CACHE_TTL);
      this.logger.verbose(`LID mapping stored in cache: ${lid} -> ${pn}`);
    } catch (error) {
      this.logger.warn(`Error storing LID mapping in cache: ${error?.message || error}`);
    }
  }

  /**
   * Remove o mapeamento do cache
   */
  async removeLIDPNMapping(lid: string): Promise<void> {
    try {
      const cacheKey = `${this.CACHE_PREFIX}${lid}`;
      await this.cache.delete(cacheKey);
      this.logger.verbose(`LID mapping removed from cache: ${lid}`);
    } catch (error) {
      this.logger.warn(`Error removing LID mapping from cache: ${error?.message || error}`);
    }
  }

  /**
   * Limpa todos os mapeamentos do cache
   */
  async clearAllMappings(): Promise<void> {
    try {
      // Nota: Isso requer implementação específica no CacheService
      // Para Redis, seria algo como SCAN + DEL pattern
      this.logger.warn('clearAllMappings not fully implemented - requires pattern deletion');
    } catch (error) {
      this.logger.error(`Error clearing LID mappings: ${error?.message || error}`);
    }
  }
}
