#!/usr/bin/env node
import crypto from 'node:crypto';
import fs from 'fs';
import readline from 'node:readline';
import path from 'path';
import { db } from '../db/index.js';
import { parseArgs } from '../utils.js';

interface DmEvent {
  message_id: number;
  chat_id: number | string;
  direction: 'inbound' | 'outbound';
  sender_id: string | null;
  sender_name: string | null;
  sender_username: string | null;
  peer_id: string | null;
  peer_name: string | null;
  peer_username: string | null;
  text: string | null;
  text_len: number;
  date: string;
  reply_to_message_id: number | null;
  views: number;
  forwards: number;
  has_links: boolean;
  has_mentions: boolean;
}

interface ProfileFact {
  field: 'primary_company' | 'primary_role';
  old_value: string | null;
  new_value: string | null;
  confidence: number;
}

interface ProfileEvent {
  event_type: string;
  event_payload: Record<string, unknown>;
  extracted_facts: ProfileFact[];
  confidence: number;
}

const HANDLE_RE = /@([A-Za-z0-9_]+)/g;

function parseLine(raw: string): DmEvent | null {
  try {
    return JSON.parse(raw) as DmEvent;
  } catch {
    return null;
  }
}

function validDmDirection(v: unknown): v is 'inbound' | 'outbound' {
  return v === 'inbound' || v === 'outbound';
}

function toLowerHandle(input: string | null | undefined): string | null {
  if (!input) return null;
  return input.trim().toLowerCase();
}

function extractHandles(text: string | null): string[] {
  if (!text) return [];
  const out = new Set<string>();
  for (const m of text.matchAll(HANDLE_RE)) {
    const handle = m[1].toLowerCase();
    if (handle) out.add(handle);
  }
  return [...out];
}

function textHash(text: string | null): string {
  return crypto.createHash('sha256').update(text || '').digest('hex');
}

function sanitizeEntity(value: string | null | undefined): string | null {
  if (!value) return null;
  return value
    .replace(/[\t\n\r]+/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .replace(/["'`]+/g, '')
    .trim();
}

function cleanupCompanyName(raw: string): string {
  return raw
    .replace(/\b(?:LLC|Ltd\.?|Inc\.?|Co\.?|Corp\.?)/gi, '')
    .replace(/\s{2,}/g, ' ')
    .trim()
    .replace(/[.\s]+$/g, '')
    .trim();
}

function extractProfileEventsFromText(text: string | null): ProfileEvent[] {
  if (!text) return [];
  const lower = text.toLowerCase();
  const events: ProfileEvent[] = [];

  // Pattern: "I no longer work at X and now I'm at Y"
  const entityToken = "[A-Za-z0-9 .,'-]{1,120}";
  const twoStep = new RegExp(
    `(?:i(?:'m| am)|i|just)?\s*(?:no longer|not|never|no|didn't)?\s*(?:work|worked|work(ed)?|work for|work at|work with|work there)?\s*(?:at|with|for)?\s*(${entityToken})(?:\.|,)?\s*(?:and|then|and now|now)\s+(?:i(?:'m| am))\s+(?:just\s+)?(?:left|moved|moving|joined|working|work|start(ed)?|switch(?:ed)?|shifted)\s+(?:at|with)?\s*(${entityToken})`,
    'giu',
  );
  const mTwoStep = lower.matchAll(twoStep);
  for (const m of mTwoStep) {
    const oldCompany = sanitizeEntity(m[1]);
    const newCompany = sanitizeEntity(m[2]);
    if (!newCompany) continue;
    if (oldCompany === newCompany) continue;
    const cleaned = cleanupCompanyName(newCompany);
    events.push({
      event_type: 'profile.company_update',
      confidence: 0.82,
      event_payload: {
        raw_text: text,
        old_company: oldCompany,
        new_company: cleaned,
        trigger: 'left_and_now_working_at',
      },
      extracted_facts: [
        {
          field: 'primary_company',
          old_value: oldCompany,
          new_value: cleaned,
          confidence: 0.82,
        },
      ],
    });
  }

  // Pattern: "now I'm working at X"
  const nowAt = /(?:^|[\s\n])(?:currently|now)\s+(?:i(?:'m| am)\s+)?working\s+at\s+([A-Za-z0-9 .,'-]{1,100})/giu;
  for (const m of lower.matchAll(nowAt)) {
    const target = sanitizeEntity(m[1]);
    if (!target) continue;
    const cleaned = cleanupCompanyName(target);
    events.push({
      event_type: 'profile.company_update',
      confidence: 0.78,
      event_payload: {
        raw_text: text,
        new_company: cleaned,
        trigger: 'currently_working_at',
      },
      extracted_facts: [
        {
          field: 'primary_company',
          old_value: null,
          new_value: cleaned,
          confidence: 0.78,
        },
      ],
    });
  }

  // Pattern: "I joined X"
  const joined = /(?:^|[\s\n])(?:i\s+just\s+|i\s+have\s+just\s+|i\s+recently\s+)?joined\s+([A-Za-z0-9 .,'-]{1,100})/giu;
  for (const m of lower.matchAll(joined)) {
    const target = sanitizeEntity(m[1]);
    if (!target) continue;
    const cleaned = cleanupCompanyName(target);
    events.push({
      event_type: 'profile.company_update',
      confidence: 0.76,
      event_payload: {
        raw_text: text,
        new_company: cleaned,
        trigger: 'joined',
      },
      extracted_facts: [
        {
          field: 'primary_company',
          old_value: null,
          new_value: cleaned,
          confidence: 0.76,
        },
      ],
    });
  }

  // dedupe by event type + new_company + old_company
  const keySet = new Set<string>();
  const deduped: ProfileEvent[] = [];
  for (const evt of events) {
    const fact = evt.extracted_facts[0];
    const k = `${evt.event_type}|${fact?.new_value || ''}|${fact?.old_value || ''}`;
    if (keySet.has(k)) continue;
    keySet.add(k);
    deduped.push(evt);
  }

  return deduped;
}

async function upsertUser(
  client: any,
  externalId: string,
  handle: string | null,
  displayName: string | null,
): Promise<number> {
  const handleNormalized = toLowerHandle(handle);
  const display = displayName?.trim() || handleNormalized;

  const res = await client.query(
    `INSERT INTO users (platform, external_id, handle, display_name)
     VALUES ('telegram', $1, $2::text, $3::text)
     ON CONFLICT (platform, external_id) DO UPDATE SET
       handle = COALESCE(NULLIF(EXCLUDED.handle, ''), users.handle),
       display_name = COALESCE(NULLIF(EXCLUDED.display_name, ''), users.display_name)
     RETURNING id`,
    [externalId, handleNormalized, display],
  );

  return res.rows[0].id as number;
}

async function addProfileEvents(
  client: any,
  userId: number,
  conversationId: number,
  messageDbId: number,
  messageId: string,
  events: ProfileEvent[],
): Promise<void> {
  for (const evt of events) {
    await client.query(
      `INSERT INTO dm_profile_update_events (
         user_id,
         conversation_id,
         source_message_id,
         source_external_message_id,
         event_type,
         event_source,
         actor_role,
         event_payload,
         extracted_facts,
         confidence
       ) VALUES ($1, $2, $3, $4, $5, 'dm_listener', 'user', $6::jsonb, $7::jsonb, $8)
       ON CONFLICT (source_message_id, event_type) DO UPDATE SET
         actor_role = EXCLUDED.actor_role,
         event_payload = EXCLUDED.event_payload,
         extracted_facts = EXCLUDED.extracted_facts,
         confidence = EXCLUDED.confidence,
         created_at = now()
      `,
      [
        userId,
        conversationId,
        messageDbId,
        messageId,
        evt.event_type,
        JSON.stringify(evt.event_payload),
        JSON.stringify(evt.extracted_facts),
        evt.confidence,
      ],
    );
  }
}

async function ingestBatch(client: any, events: string[]): Promise<{
  inserted: number;
  skipped: number;
  malformed: number;
  users: number;
  psychEvents: number;
}> {
  let inserted = 0;
  let skipped = 0;
  let malformed = 0;
  let psychEvents = 0;
  const userCache = new Map<string, number>();

  for (const line of events) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const row = parseLine(trimmed);
    if (!row || !validDmDirection(row.direction) || !row.sender_id || !row.peer_id) {
      malformed += 1;
      continue;
    }

    const senderExt = row.sender_id;
    const peerExt = row.peer_id;

    const getUserId = async (extId: string, handle: string | null, name: string | null): Promise<number> => {
      const key = extId.toLowerCase();
      const hit = userCache.get(key);
      if (hit) return hit;
      const id = await upsertUser(client, extId, handle, name);
      userCache.set(key, id);
      return id;
    };

    const senderUserId = await getUserId(senderExt, row.sender_username, row.sender_name);
    const peerUserId = await getUserId(peerExt, row.peer_username, row.peer_name);

    const sentAt = new Date(row.date || Date.now()).toISOString();
    const chatId = String(row.chat_id);

    const accountUserId = row.direction === 'outbound' ? senderUserId : peerUserId;
    const subjectUserId = row.direction === 'outbound' ? peerUserId : senderUserId;

    const convRes = await client.query(
      `INSERT INTO dm_conversations (
         platform, external_chat_id, user_a_id, user_b_id, last_message_at
       )
       VALUES ('telegram', $1, $2, $3, $4::timestamptz)
       ON CONFLICT (platform, external_chat_id)
       DO UPDATE SET
         user_a_id = EXCLUDED.user_a_id,
         user_b_id = EXCLUDED.user_b_id,
         last_message_at = GREATEST(dm_conversations.last_message_at, EXCLUDED.last_message_at),
         updated_at = now()
       RETURNING id`,
      [
        chatId,
        accountUserId,
        subjectUserId,
        sentAt,
      ],
    );

    if (!convRes.rows.length) {
      skipped += 1;
      continue;
    }

    const convId = convRes.rows[0].id as number;
    const msgText = row.text ?? null;

    const msgRes = await client.query(
      `INSERT INTO dm_messages (
         conversation_id, external_message_id, sender_id, direction, text,
         text_len, sent_at, reply_to_external_message_id,
         views, forwards, has_links, has_mentions, raw_payload
       ) VALUES ($1, $2, $3, $4::text, $5, $6, $7::timestamptz, $8, $9, $10, $11, $12, $13::jsonb)
       ON CONFLICT (conversation_id, external_message_id) DO NOTHING`,
      [
        convId,
        String(row.message_id),
        senderUserId,
        row.direction,
        msgText,
        row.text_len ?? 0,
        sentAt,
        row.reply_to_message_id != null ? String(row.reply_to_message_id) : null,
        Number(row.views || 0),
        Number(row.forwards || 0),
        Boolean(row.has_links),
        Boolean(row.has_mentions),
        JSON.stringify(row),
      ],
    );

    if (msgRes.rowCount === 1) {
      const msgDbId = await client.query(
        `SELECT id FROM dm_messages WHERE conversation_id=$1 AND external_message_id=$2`,
        [convId, String(row.message_id)],
      );
      const messageDbId = msgDbId.rows[0]?.id as number | undefined;

      // Extract profile-correction signals only for inbound user messages.
      const profileEvents =
        row.direction === 'inbound' ? extractProfileEventsFromText(msgText) : [];
      if (profileEvents.length > 0 && messageDbId) {
        // subjectUserId is the inbound sender in private DM context
        const targetUserId = row.direction === 'inbound' ? senderUserId : subjectUserId;
        await addProfileEvents(
          client,
          targetUserId,
          convId,
          messageDbId,
          String(row.message_id),
          profileEvents,
        );
        psychEvents += profileEvents.length;
      }

      inserted += 1;
    } else {
      skipped += 1;
    }
  }

  return { inserted, skipped, malformed, users: userCache.size, psychEvents };
}

async function main() {
  const args = parseArgs();
  const filePath = args['file'];

  if (!filePath) {
    console.error('Usage: npm run ingest-dm-jsonl -- --file <data/exports/telethon_dms_live.jsonl>');
    process.exit(1);
  }

  const abs = path.resolve(process.cwd(), filePath);
  const readStream = fs.createReadStream(abs, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: readStream });

  console.log(`📥 Ingesting DM JSONL from: ${abs}`);

  const start = Date.now();
  const rows: string[] = [];
  for await (const line of rl) rows.push(line);

  const { inserted, skipped, malformed, users, psychEvents } = await db.transaction(async (client) => {
    return ingestBatch(client, rows);
  });

  const elapsedSec = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\n✅ DM JSONL ingest finished`);
  console.log(`   Inserted messages: ${inserted}`);
  console.log(`   Skipped/Duplicates: ${skipped}`);
  console.log(`   Malformed lines: ${malformed}`);
  console.log(`   Profile signal events: ${psychEvents}`);
  console.log(`   Upserted users: ${users}`);
  console.log(`   Time: ${elapsedSec}s`);

  await db.close();
}

main().catch(async (err) => {
  console.error('❌ DM JSONL ingest failed:', err);
  await db.close();
  process.exit(1);
});
