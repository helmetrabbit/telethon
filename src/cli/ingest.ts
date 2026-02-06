/**
 * CLI: Ingest Telegram export JSON into Postgres.
 *
 * Usage:
 *   npm run ingest -- --file data/exports/sample_bd_chat.json --group-kind bd
 *
 * What it does:
 *   1. Reads the JSON file and computes SHA-256 (for traceability).
 *   2. Checks for duplicate import (same SHA = already imported).
 *   3. Inserts raw traceability rows (raw_imports + raw_import_rows).
 *   4. Normalizes into ontology tables: users, groups, memberships,
 *      messages, message_mentions.
 *   5. Everything in one transaction — all-or-nothing.
 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import pg from 'pg';
import { db } from '../db/index.js';
import {
  TelegramExportSchema,
  parseMessage,
  normalizeText,
  extractMentions,
  hasLinks,
  parseFromId,
} from '../parsers/telegram.js';
import { fileSha256, parseArgs } from '../utils.js';

// ── Types ───────────────────────────────────────────────

interface IngestStats {
  rawRows: number;
  skippedMessages: number;
  usersUpserted: number;
  messagesInserted: number;
  mentionsInserted: number;
}

// ── Main ────────────────────────────────────────────────

async function main(): Promise<void> {
  const args = parseArgs();
  const filePath = args['file'];
  const groupKind = args['group-kind'] || 'unknown';

  if (!filePath) {
    console.error('Usage: npm run ingest -- --file <path> [--group-kind bd|work|general_chat|unknown]');
    process.exit(1);
  }

  const absPath = resolve(filePath);
  console.log(`\n📂 Ingesting: ${absPath}`);
  console.log(`   Group kind: ${groupKind}`);

  // ── 1. Read + hash file ─────────────────────────────
  const sha = fileSha256(absPath);
  console.log(`   SHA-256: ${sha.slice(0, 16)}…`);

  // ── 2. Check for duplicate ──────────────────────────
  const existing = await db.query<{ id: number }>(
    'SELECT id FROM raw_imports WHERE sha256 = $1',
    [sha],
  );
  if (existing.rows.length > 0) {
    console.log(`\n⚠️  This file was already imported (raw_imports.id = ${existing.rows[0].id}). Skipping.`);
    await db.close();
    return;
  }

  // ── 3. Parse JSON ───────────────────────────────────
  const rawJson = JSON.parse(readFileSync(absPath, 'utf-8'));
  const exportData = TelegramExportSchema.parse(rawJson);
  console.log(`   Group name: "${exportData.name}"`);
  console.log(`   Raw messages in file: ${exportData.messages.length}`);

  // ── 4. Run everything in a transaction ──────────────
  const stats = await db.transaction(async (client) => {
    return ingestInTransaction(client, absPath, sha, groupKind, exportData, rawJson);
  });

  // ── 5. Print summary ───────────────────────────────
  console.log('\n✅ Ingest complete:');
  console.log(`   Raw import rows:    ${stats.rawRows}`);
  console.log(`   Skipped (non-msg):  ${stats.skippedMessages}`);
  console.log(`   Users upserted:     ${stats.usersUpserted}`);
  console.log(`   Messages inserted:  ${stats.messagesInserted}`);
  console.log(`   Mentions inserted:  ${stats.mentionsInserted}`);

  await db.close();
}

// ── Transaction body ────────────────────────────────────

async function ingestInTransaction(
  client: pg.PoolClient,
  absPath: string,
  sha: string,
  groupKind: string,
  exportData: ReturnType<typeof TelegramExportSchema.parse>,
  _rawJson: unknown,
): Promise<IngestStats> {
  const stats: IngestStats = {
    rawRows: 0,
    skippedMessages: 0,
    usersUpserted: 0,
    messagesInserted: 0,
    mentionsInserted: 0,
  };

  // ── raw_imports ───────────────────────────────────
  const importRes = await client.query(
    `INSERT INTO raw_imports (source_path, sha256)
     VALUES ($1, $2)
     RETURNING id`,
    [absPath, sha],
  );
  const rawImportId: number = importRes.rows[0].id;

  // ── Group upsert ──────────────────────────────────
  const groupExternalId = String(exportData.id);
  const groupRes = await client.query(
    `INSERT INTO groups (platform, external_id, title, kind)
     VALUES ('telegram', $1, $2, $3::group_kind)
     ON CONFLICT (platform, external_id)
     DO UPDATE SET title = EXCLUDED.title, kind = EXCLUDED.kind, updated_at = now()
     RETURNING id`,
    [groupExternalId, exportData.name, groupKind],
  );
  const groupId: number = groupRes.rows[0].id;

  // ── Track users we've seen (external_id → db id) ─
  const userCache = new Map<string, number>();

  async function ensureUser(
    externalId: string,
    displayName: string | null,
  ): Promise<number> {
    const cached = userCache.get(externalId);
    if (cached !== undefined) return cached;

    // Derive a handle from display_name if possible (lowercase, no spaces)
    const handle = displayName
      ? '@' + displayName.toLowerCase().replace(/\s+/g, '')
      : null;

    const res = await client.query(
      `INSERT INTO users (platform, external_id, handle, display_name)
       VALUES ('telegram', $1, $2, $3)
       ON CONFLICT (platform, external_id)
       DO UPDATE SET
         display_name = COALESCE(EXCLUDED.display_name, users.display_name),
         handle = COALESCE(EXCLUDED.handle, users.handle),
         updated_at = now()
       RETURNING id`,
      [externalId, handle, displayName],
    );
    const userId: number = res.rows[0].id;
    userCache.set(externalId, userId);
    stats.usersUpserted++;
    return userId;
  }

  // ── Membership tracking (userId → {first, last, count}) ─
  const membershipMap = new Map<
    number,
    { firstSeen: Date; lastSeen: Date; msgCount: number }
  >();

  function trackMembership(userId: number, sentAt: Date): void {
    const existing = membershipMap.get(userId);
    if (!existing) {
      membershipMap.set(userId, {
        firstSeen: sentAt,
        lastSeen: sentAt,
        msgCount: 1,
      });
    } else {
      if (sentAt < existing.firstSeen) existing.firstSeen = sentAt;
      if (sentAt > existing.lastSeen) existing.lastSeen = sentAt;
      existing.msgCount++;
    }
  }

  // ── Process messages ──────────────────────────────
  for (const rawMsg of exportData.messages) {
    // Insert raw row for traceability (all messages, including service)
    await client.query(
      `INSERT INTO raw_import_rows (raw_import_id, row_type, external_id, raw_json)
       VALUES ($1, $2, $3, $4)`,
      [
        rawImportId,
        typeof rawMsg === 'object' && rawMsg !== null && 'type' in rawMsg
          ? (rawMsg as Record<string, unknown>).type
          : 'unknown',
        typeof rawMsg === 'object' && rawMsg !== null && 'id' in rawMsg
          ? String((rawMsg as Record<string, unknown>).id)
          : null,
        JSON.stringify(rawMsg),
      ],
    );
    stats.rawRows++;

    // Parse into typed message (skips service messages)
    const msg = parseMessage(rawMsg);
    if (!msg) {
      stats.skippedMessages++;
      continue;
    }

    // Resolve user
    const fromExtId = parseFromId(msg.from_id);
    if (!fromExtId) {
      stats.skippedMessages++;
      continue;
    }

    const userId = await ensureUser(fromExtId, msg.from ?? null);
    const sentAt = new Date(msg.date);

    // Normalize text
    const plainText = normalizeText(msg.text);
    const textLen = plainText.length;
    const msgHasLinks = hasLinks(plainText);
    const mentions = extractMentions(plainText);
    const msgHasMentions = mentions.length > 0;

    // Get the raw_import_rows id for this message (last inserted)
    const rawRefRes = await client.query(
      `SELECT id FROM raw_import_rows
       WHERE raw_import_id = $1 AND external_id = $2
       ORDER BY id DESC LIMIT 1`,
      [rawImportId, String(msg.id)],
    );
    const rawRefRowId: number | null = rawRefRes.rows[0]?.id ?? null;

    // Insert message
    const msgRes = await client.query(
      `INSERT INTO messages (
         group_id, user_id, external_message_id, sent_at,
         text, text_len, reply_to_external_message_id,
         has_links, has_mentions, raw_ref_row_id
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       RETURNING id`,
      [
        groupId,
        userId,
        String(msg.id),
        sentAt,
        plainText,
        textLen,
        msg.reply_to_message_id ? String(msg.reply_to_message_id) : null,
        msgHasLinks,
        msgHasMentions,
        rawRefRowId,
      ],
    );
    const messageId: number = msgRes.rows[0].id;
    stats.messagesInserted++;

    // Track membership
    trackMembership(userId, sentAt);

    // Insert mentions
    for (const handle of mentions) {
      await client.query(
        `INSERT INTO message_mentions (message_id, mentioned_handle)
         VALUES ($1, $2)
         ON CONFLICT DO NOTHING`,
        [messageId, handle],
      );
      stats.mentionsInserted++;
    }
  }

  // ── Flush memberships ─────────────────────────────
  for (const [userId, m] of membershipMap) {
    await client.query(
      `INSERT INTO memberships (group_id, user_id, first_seen_at, last_seen_at, msg_count)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (group_id, user_id)
       DO UPDATE SET
         first_seen_at = LEAST(memberships.first_seen_at, EXCLUDED.first_seen_at),
         last_seen_at  = GREATEST(memberships.last_seen_at, EXCLUDED.last_seen_at),
         msg_count     = memberships.msg_count + EXCLUDED.msg_count`,
      [groupId, userId, m.firstSeen, m.lastSeen, m.msgCount],
    );
  }

  // ── Backfill mentioned_user_id where possible ─────
  await client.query(
    `UPDATE message_mentions mm
        SET mentioned_user_id = u.id
       FROM users u
      WHERE mm.mentioned_user_id IS NULL
        AND u.handle = '@' || mm.mentioned_handle`,
  );

  return stats;
}

// ── Entry point ─────────────────────────────────────────

main().catch((err) => {
  console.error('❌ Ingest failed:', err);
  process.exit(1);
});
