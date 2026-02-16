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

async function ingestBatch(client: any, events: string[]): Promise<{
  inserted: number;
  skipped: number;
  malformed: number;
  users: number;
}> {
  let inserted = 0;
  let skipped = 0;
  let malformed = 0;
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
         platform, account_user_id, subject_user_id, external_chat_id,
         status, source, priority, metadata, last_activity_at, title
       )
       VALUES ('telegram', $1, $2, $3, 'active', 'listener', 0, '{}'::jsonb, $4::timestamptz, $5)
       ON CONFLICT (platform, account_user_id, external_chat_id)
       DO UPDATE SET
         subject_user_id = EXCLUDED.subject_user_id,
         last_activity_at = GREATEST(dm_conversations.last_activity_at, EXCLUDED.last_activity_at),
         updated_at = now()
       RETURNING id`,
      [
        accountUserId,
        subjectUserId,
        chatId,
        sentAt,
        `${row.peer_name || row.peer_username || row.peer_id || 'DM Thread'}`,
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
         conversation_id, external_message_id, direction, message_text,
         text_hash, sent_at, raw_json, response_to_external_message_id,
         has_links, has_mentions, extracted_handles
       ) VALUES ($1, $2, $3::text, $4, $5, $6::timestamptz, $7::jsonb, $8, $9, $10, $11)
       ON CONFLICT (conversation_id, external_message_id) DO NOTHING`,
      [
        convId,
        String(row.message_id),
        row.direction,
        msgText,
        textHash(msgText),
        sentAt,
        JSON.stringify(row),
        row.reply_to_message_id != null ? String(row.reply_to_message_id) : null,
        Boolean(row.has_links),
        Boolean(row.has_mentions),
        extractHandles(msgText),
      ],
    );

    if (msgRes.rowCount === 1) {
      inserted += 1;
    } else {
      skipped += 1;
    }
  }

  return { inserted, skipped, malformed, users: userCache.size };
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

  const { inserted, skipped, malformed, users } = await db.transaction(async (client) => {
    return ingestBatch(client, rows);
  });

  const elapsedSec = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\n✅ DM JSONL ingest finished`);
  console.log(`   Inserted messages: ${inserted}`);
  console.log(`   Skipped/Duplicates: ${skipped}`);
  console.log(`   Malformed lines: ${malformed}`);
  console.log(`   Upserted users: ${users}`);
  console.log(`   Time: ${elapsedSec}s`);

  await db.close();
}

main().catch(async (err) => {
  console.error('❌ DM JSONL ingest failed:', err);
  await db.close();
  process.exit(1);
});
