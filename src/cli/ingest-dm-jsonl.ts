#!/usr/bin/env node
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

function toLowerHandle(input: string | null | undefined): string | null {
  if (!input) return null;
  return input.trim().toLowerCase();
}

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

async function upsertUser(client: any, platform: string, externalId: string, handle: string | null, displayName: string | null): Promise<number> {
  const handleNormalized = toLowerHandle(handle);
  const display = displayName?.trim() || handleNormalized;

  const res = await client.query(
    `INSERT INTO users (platform, external_id, handle, display_name)
     VALUES ($1, $2, $3::text, $4::text)
     ON CONFLICT (platform, external_id) DO UPDATE SET
       handle = COALESCE(NULLIF(EXCLUDED.handle, ''), users.handle),
       display_name = COALESCE(NULLIF(EXCLUDED.display_name, ''), users.display_name)
     RETURNING id`,
    [platform, externalId, handleNormalized, display],
  );

  return res.rows[0].id as number;
}

async function ingestBatch(client: any, events: string[]): Promise<{ inserted: number; skipped: number; malformed: number; userCount: number }> {
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
      const key = `${extId.toLowerCase()}`;
      const hit = userCache.get(key);
      if (hit) return hit;
      const id = await upsertUser(
        client,
        'telegram',
        extId,
        handle,
        name,
      );
      userCache.set(key, id);
      return id;
    };

    const senderUserId = await getUserId(senderExt, row.sender_username, row.sender_name);
    const peerUserId = await getUserId(peerExt, row.peer_username, row.peer_name);

    const chatId = String(row.chat_id);

    const convRes = await client.query(
      `INSERT INTO dm_conversations (platform, external_chat_id, user_a_id, user_b_id, last_message_at)
       VALUES ('telegram', $1, LEAST($2::bigint, $3::bigint), GREATEST($2::bigint, $3::bigint), $4::timestamptz)
       ON CONFLICT (platform, external_chat_id)
       DO UPDATE SET
         last_message_at = GREATEST(dm_conversations.last_message_at, EXCLUDED.last_message_at),
         updated_at = now()
       RETURNING id`,
      [chatId, senderUserId, peerUserId, new Date(row.date || Date.now()).toISOString()],
    );

    if (!convRes.rows.length) {
      skipped += 1;
      continue;
    }

    const convId = convRes.rows[0].id as number;

    const msgRes = await client.query(
      `INSERT INTO dm_messages (
         conversation_id, external_message_id, sender_id,
         direction, text, text_len, sent_at, reply_to_external_message_id,
         views, forwards, has_links, has_mentions
       ) VALUES ($1, $2, $3, $4::text, $5, $6, $7::timestamptz, $8, $9, $10, $11)
       ON CONFLICT (conversation_id, external_message_id) DO NOTHING`,
      [
        convId,
        String(row.message_id),
        senderUserId,
        row.direction,
        row.text ?? null,
        Number(row.text_len || 0),
        new Date(row.date || Date.now()).toISOString(),
        row.reply_to_message_id != null ? String(row.reply_to_message_id) : null,
        Number(row.views || 0),
        Number(row.forwards || 0),
        Boolean(row.has_links),
        Boolean(row.has_mentions),
      ],
    );

    if (msgRes.rowCount === 1) {
      inserted += 1;
      await client.query(
        `UPDATE dm_conversations
         SET message_count = COALESCE(message_count, 0) + 1,
             last_message_at = GREATEST(last_message_at, $2::timestamptz)
         WHERE id = $1`,
        [convId, new Date(row.date || Date.now()).toISOString()],
      );
    } else {
      skipped += 1;
    }
  }

  return {
    inserted,
    skipped,
    malformed,
    userCount: userCache.size,
  };
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
  for await (const line of rl) {
    rows.push(line);
  }

  const { inserted, skipped, malformed, userCount } = await db.transaction(async (client) => {
    return ingestBatch(client, rows);
  });

  const elapsedSec = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\n✅ DM JSONL ingest finished`);
  console.log(`   Inserted messages: ${inserted}`);
  console.log(`   Skipped/Duplicates: ${skipped}`);
  console.log(`   Malformed lines: ${malformed}`);
  console.log(`   Upserted users: ${userCount}`);
  console.log(`   Time: ${elapsedSec}s`);

  await db.close();
}

main().catch(async (err) => {
  console.error('❌ DM JSONL ingest failed:', err);
  await db.close();
  process.exit(1);
});
