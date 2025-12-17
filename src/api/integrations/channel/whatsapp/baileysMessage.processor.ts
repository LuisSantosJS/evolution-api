import { Logger } from '@config/logger.config';
import { BaileysEventMap, MessageUpsertType, WAMessage } from '@whiskeysockets/baileys';
import { catchError, concatMap, delay, EMPTY, from, retryWhen, Subject, Subscription, take, tap } from 'rxjs';

type MessageUpsertPayload = BaileysEventMap['messages.upsert'];
type MountProps = {
  onMessageReceive: (payload: MessageUpsertPayload, settings: any) => Promise<void>;
};

export class BaileysMessageProcessor {
  private processorLogs = new Logger('BaileysMessageProcessor');
  private subscription?: Subscription;
  private messageCount = 0;
  private errorCount = 0;
  private lastMessageTime = Date.now();

  protected messageSubject = new Subject<{
    messages: WAMessage[];
    type: MessageUpsertType;
    requestId?: string;
    settings: any;
  }>();

  mount({ onMessageReceive }: MountProps) {
    this.subscription = this.messageSubject
      .pipe(
        tap(({ messages }) => {
          this.messageCount++;
          this.lastMessageTime = Date.now();
          this.processorLogs.log(`Processing batch of ${messages.length} messages (total processed: ${this.messageCount})`);

          // CRITICAL: Monitor stream health - detect if observers are missing
          if (this.messageSubject.observers.length === 0) {
            this.processorLogs.error('⚠️ CRITICAL: NO OBSERVERS on messageSubject! Stream may be dead!');
          }

          // CRITICAL: Detect backpressure - warn if processing is too slow
          if (this.messageSubject.observed && (this.messageSubject as any)._events?.length > 100) {
            this.processorLogs.warn(`⚠️ HIGH BACKPRESSURE: ${(this.messageSubject as any)._events?.length} queued events`);
          }
        }),
        concatMap(({ messages, type, requestId, settings }) =>
          from(onMessageReceive({ messages, type, requestId }, settings)).pipe(
            retryWhen((errors) =>
              errors.pipe(
                tap((error) => this.processorLogs.warn(`Retrying message batch due to error: ${error.message}`)),
                delay(1000), // 1 segundo de delay
                take(3), // Máximo 3 tentativas
              ),
            ),
            // Captura erros de mensagens individuais sem matar o stream principal
            catchError((error) => {
              this.errorCount++;
              this.processorLogs.error(`Failed to process message batch after retries (error #${this.errorCount}): ${error.message}`);
              this.processorLogs.error(error);
              // Retorna EMPTY para esta mensagem específica, mas o stream principal continua
              return EMPTY;
            }),
          ),
        ),
        // Removido catchError do pipe principal para evitar que o stream complete
        // Se houver erro não capturado, será tratado no subscribe error handler
      )
      .subscribe({
        error: (error) => {
          // Este handler só será chamado se houver erro não capturado
          // Loga o erro mas NÃO completa o stream
          this.processorLogs.error(`CRITICAL: Message stream error (this should not happen): ${error}`);
          this.processorLogs.error(`Stream stats: processed=${this.messageCount}, errors=${this.errorCount}`);
          // O stream será recriado automaticamente pelo health check
        },
        complete: () => {
          // Detecta quando o stream completa (não deveria acontecer em operação normal)
          this.processorLogs.warn(`Message stream completed unexpectedly (processed=${this.messageCount}, errors=${this.errorCount})`);
        },
      });
  }

  processMessage(payload: MessageUpsertPayload, settings: any) {
    const { messages, type, requestId } = payload;
    this.messageSubject.next({ messages, type, requestId, settings });
  }

  /**
   * Verifica se o stream está ativo e saudável
   */
  isStreamHealthy(): boolean {
    // Verifica se a subscription existe e não está closed
    if (!this.subscription || this.subscription.closed) {
      this.processorLogs.warn('Stream health check FAILED: subscription is closed');
      return false;
    }

    // Verifica se o Subject não foi completado
    if (this.messageSubject.closed) {
      this.processorLogs.warn('Stream health check FAILED: messageSubject is closed');
      return false;
    }

    return true;
  }

  /**
   * Retorna estatísticas do stream para monitoramento
   */
  getStreamStats() {
    return {
      messageCount: this.messageCount,
      errorCount: this.errorCount,
      lastMessageTime: this.lastMessageTime,
      isHealthy: this.isStreamHealthy(),
      subscriptionClosed: this.subscription?.closed ?? true,
      subjectClosed: this.messageSubject.closed,
    };
  }

  /**
   * Reseta os contadores (útil após reconexão)
   */
  resetStats() {
    this.messageCount = 0;
    this.errorCount = 0;
    this.lastMessageTime = Date.now();
    this.processorLogs.verbose('Stream statistics reset');
  }

  onDestroy() {
    this.processorLogs.verbose(`Destroying message processor (processed=${this.messageCount}, errors=${this.errorCount})`);
    this.subscription?.unsubscribe();
    this.messageSubject.complete();
  }
}
