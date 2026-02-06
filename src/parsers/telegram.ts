/**
 * Zod schemas for Telegram Desktop "Export Chat History" JSON.
 *
 * Telegram's result.json has this rough shape:
 * {
 *   "name": "Group Name",
 *   "type": "public_supergroup" | "private_supergroup" | ...,
 *   "id": 1234567890,
 *   "messages": [ ... ]
 * }
 *
 * Each message's `text` field is EITHER a plain string OR an array
 * of mixed strings and rich-text objects. We normalize both.
 */

import { z } from 'zod';

// ── Text segment (rich-text element) ────────────────────

/**
 * When text is an array, each element is either a raw string
 * or a typed object like { type: "mention", text: "@alice" }.
 */
const TelegramTextEntity = z.object({
  type: z.string(),
  text: z.string().default(''),
});

/**
 * A text segment is either a plain string or a rich-text entity.
 */
const TelegramTextSegment = z.union([
  z.string(),
  TelegramTextEntity,
]);

/**
 * The `text` field: string | array of segments.
 * We also allow it to be missing/empty.
 */
const TelegramTextField = z.union([
  z.string(),
  z.array(TelegramTextSegment),
]).optional().default('');

// ── Message schema ──────────────────────────────────────

export const TelegramMessageSchema = z.object({
  id: z.number(),
  type: z.string().default('message'),
  date: z.string(),                        // ISO-ish: "2024-01-15T10:30:00"
  date_unixtime: z.string().optional(),     // sometimes present
  from: z.string().nullable().optional(),   // display name
  from_id: z.string().nullable().optional(),// "user1234567" or "channel1234567"
  text: TelegramTextField,
  text_entities: z.array(TelegramTextEntity).optional().default([]),
  reply_to_message_id: z.number().nullable().optional(),
  forwarded_from: z.string().nullable().optional(),
  photo: z.string().nullable().optional(),
  file: z.string().nullable().optional(),
  media_type: z.string().nullable().optional(),
});

export type TelegramMessage = z.infer<typeof TelegramMessageSchema>;

// ── Export root schema ──────────────────────────────────

export const TelegramExportSchema = z.object({
  name: z.string().default('Unknown Group'),
  type: z.string().default('unknown'),
  id: z.number(),
  messages: z.array(
    // Be lenient: skip messages that don't parse (service messages, etc.)
    z.unknown()
  ),
});

export type TelegramExport = z.infer<typeof TelegramExportSchema>;

// ── Normalization helpers ───────────────────────────────

/**
 * Flatten Telegram's text field into a plain string.
 * Handles both "string" and "[string | {type, text}]" forms.
 */
export function normalizeText(text: TelegramMessage['text']): string {
  if (text === undefined || text === null) return '';
  if (typeof text === 'string') return text;
  if (Array.isArray(text)) {
    return text
      .map((seg) => (typeof seg === 'string' ? seg : seg.text ?? ''))
      .join('');
  }
  return String(text);
}

/** Regex for @handle mentions */
const MENTION_RE = /@([a-zA-Z0-9_]{3,32})/g;

/** Regex for http(s) links */
const LINK_RE = /https?:\/\/[^\s)>\]]+/gi;

/**
 * Extract @handles from plain text.
 * Returns deduplicated, lowercased handles (without @).
 */
export function extractMentions(plainText: string): string[] {
  const matches = plainText.matchAll(MENTION_RE);
  const handles = new Set<string>();
  for (const m of matches) {
    handles.add(m[1].toLowerCase());
  }
  return [...handles];
}

/**
 * Check whether the text contains at least one http(s) link.
 */
export function hasLinks(plainText: string): boolean {
  return LINK_RE.test(plainText);
}

/**
 * Extract the user's numeric ID from Telegram's from_id field.
 * from_id looks like "user1234567" or "channel1234567".
 * Returns the full string (e.g. "user1234567") as the external_id.
 */
export function parseFromId(fromId: string | null | undefined): string | null {
  if (!fromId) return null;
  return fromId;
}

/**
 * Parse a single raw message from the export.
 * Returns null if it's a service message or unparseable.
 */
export function parseMessage(raw: unknown): TelegramMessage | null {
  const result = TelegramMessageSchema.safeParse(raw);
  if (!result.success) return null;
  const msg = result.data;
  // Skip service messages (type !== 'message')
  if (msg.type !== 'message') return null;
  return msg;
}
