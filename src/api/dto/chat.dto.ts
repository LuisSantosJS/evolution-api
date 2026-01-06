import {
  proto,
  WAPresence,
  WAPrivacyGroupAddValue,
  WAPrivacyOnlineValue,
  WAPrivacyValue,
  WAReadReceiptsValue,
} from '@whiskeysockets/baileys';

export class OnWhatsAppDto {
  constructor(
    public readonly jid: string,
    public readonly exists: boolean,
    public readonly number: string,
    public readonly name?: string,
    public readonly lid?: string,
  ) { }
}

export class getBase64FromMediaMessageDto {
  message: proto.WebMessageInfo;
  convertToMp4?: boolean;
}

export class WhatsAppNumberDto {
  numbers: string[];
}

export class NumberDto {
  number: string;
}

export class NumberBusiness {
  wid?: string;
  jid?: string;
  exists?: boolean;
  isBusiness: boolean;
  name?: string;
  message?: string;
  description?: string;
  email?: string;
  websites?: string[];
  website?: string[];
  address?: string;
  about?: string;
  vertical?: string;
  profilehandle?: string;
}

export class ProfileNameDto {
  name: string;
}

export class ProfileStatusDto {
  status: string;
}

export class ProfilePictureDto {
  number?: string;
  // url or base64
  picture?: string;
}

class Key {
  id: string;
  fromMe: boolean;
  remoteJid: string;
}
export class ReadMessageDto {
  readMessages: Key[];
}

export class LastMessage {
  key: Key;
  messageTimestamp?: number;
}

export class ArchiveChatDto {
  lastMessage?: LastMessage;
  chat?: string;
  archive: boolean;
}

export class MarkChatUnreadDto {
  lastMessage?: LastMessage;
  chat?: string;
}

export class PrivacySettingDto {
  readreceipts: WAReadReceiptsValue;
  profile: WAPrivacyValue;
  status: WAPrivacyValue;
  online: WAPrivacyOnlineValue;
  last: WAPrivacyValue;
  groupadd: WAPrivacyGroupAddValue;
}

export class DeleteMessage {
  id: string;
  fromMe: boolean;
  remoteJid: string;
  participant?: string;
}
export class Options {
  delay?: number;
  presence?: WAPresence;
}
class OptionsMessage {
  options: Options;
}
export class Metadata extends OptionsMessage {
  number: string;
}

export class SendPresenceDto extends Metadata {
  presence: WAPresence;
  delay: number;
}

export class UpdateMessageDto extends Metadata {
  number: string;
  key: proto.IMessageKey;
  text: string;
}

export class BlockUserDto {
  number: string;
  status: 'block' | 'unblock';
}

export class SyncMessagesDto {
  /**
   * Optional: Specific chat to sync (phone number or JID)
   * If not provided, syncs all recent chats based on 'source' option
   * @example "5511999999999" or "5511999999999@s.whatsapp.net"
   */
  remoteJid?: string;

  /**
   * Number of messages to sync per chat (default: 100, max: 500)
   * @example 100
   */
  limit?: number;

  /**
   * Optional: Array of multiple chats to sync
   * Alternative to remoteJid for batch sync
   * @example ["5511999999999", "5511888888888"]
   */
  remoteJids?: string[];

  /**
   * Force full sync even if no previous messages exist
   * Uses current timestamp as reference (default: false)
   * @example false
   */
  forceSync?: boolean;

  /**
   * Source to get chat list from:
   * - 'database': Syncs only chats that exist in the database (default)
   * - 'whatsapp': Syncs chats from WhatsApp cache (recovers lost chats)
   * - 'all': Syncs from both sources (most complete)
   * @example "whatsapp"
   */
  source?: 'database' | 'whatsapp' | 'all';

  /**
   * Include group chats in sync (default: false)
   * Only applies when syncing all chats (no remoteJid specified)
   * @example false
   */
  includeGroups?: boolean;

  /**
   * Maximum number of chats to sync when syncing all (default: 100, max: 500)
   * Only applies when no remoteJid is specified
   * @example 100
   */
  maxChats?: number;

  /**
   * Force requesting full history from WhatsApp server
   * This will trigger a new history sync request (may take time)
   * @example false
   */
  requestFullSync?: boolean;
}
