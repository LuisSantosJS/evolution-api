import { CacheConf, CacheConfRedis, configService } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { createClient, RedisClientType } from 'redis';

class Redis {
  private logger = new Logger('Redis');
  private client: RedisClientType = null;
  private conf: CacheConfRedis;
  private connected = false;

  // Store event listener callbacks for cleanup
  private connectListener: (() => void) | null = null;
  private readyListener: (() => void) | null = null;
  private errorListener: (() => void) | null = null;
  private endListener: (() => void) | null = null;

  constructor() {
    this.conf = configService.get<CacheConf>('CACHE')?.REDIS;
  }

  getConnection(): RedisClientType {
    if (this.connected) {
      return this.client;
    } else {
      this.client = createClient({
        url: this.conf.URI,
      });

      // Store callback references for cleanup
      this.connectListener = () => {
        this.logger.verbose('redis connecting');
      };
      this.client.on('connect', this.connectListener);

      this.readyListener = () => {
        this.logger.verbose('redis ready');
        this.connected = true;
      };
      this.client.on('ready', this.readyListener);

      this.errorListener = () => {
        this.logger.error('redis disconnected');
        this.connected = false;
      };
      this.client.on('error', this.errorListener);

      this.endListener = () => {
        this.logger.verbose('redis connection ended');
        this.connected = false;
      };
      this.client.on('end', this.endListener);

      try {
        this.client.connect();
        this.connected = true;
      } catch (e) {
        this.connected = false;
        this.logger.error('redis connect exception caught: ' + e);
        return null;
      }

      return this.client;
    }
  }

  async disconnect(): Promise<void> {
    try {
      if (this.client) {
        // Remove event listeners before disconnecting
        if (this.connectListener) {
          this.client.removeListener('connect', this.connectListener);
          this.connectListener = null;
        }
        if (this.readyListener) {
          this.client.removeListener('ready', this.readyListener);
          this.readyListener = null;
        }
        if (this.errorListener) {
          this.client.removeListener('error', this.errorListener);
          this.errorListener = null;
        }
        if (this.endListener) {
          this.client.removeListener('end', this.endListener);
          this.endListener = null;
        }

        await this.client.quit();
        this.client = null;
        this.connected = false;
        this.logger.verbose('Redis client disconnected and cleaned up');
      }
    } catch (error) {
      this.logger.error('Error during Redis cleanup: ' + error);
      // Force cleanup even if error occurs
      this.client = null;
      this.connected = false;
      this.connectListener = null;
      this.readyListener = null;
      this.errorListener = null;
      this.endListener = null;
    }
  }
}

export const redisClient = new Redis();
