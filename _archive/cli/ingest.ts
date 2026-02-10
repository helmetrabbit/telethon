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
  parseParticipant,
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
  participantsIngested: number;
  membershipsFromParticipants: number;
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
  const isTelethon = Array.isArray(exportData.participants) && exportData.participants.length > 0;
  console.log(`   Group name: "${exportData.name}"`);
  console.log(`   Raw messages in file: ${exportData.messages.length}`);
  if (isTelethon) {
    console.log(`   Telethon export detected`);
    console.log(`   Participants in file: ${exportData.participants.length}`);
    console.log(`   Participants status:  ${exportData.participants_status ?? 'n/a'}`);
  }

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
  if (stats.participantsIngested > 0) {
    console.log(`   Participants:       ${stats.participantsIngested}`);
    console.log(`   Memberships (part): ${stats.membershipsFromParticipants}`);
  }

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
    participantsIngested: 0,
    membershipsFromParticipants: 0,
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
    explicitHandle?: string | null,
    bio?: string | null,
    extra: {
      is_scam?: boolean | null;
      is_fake?: boolean | null;
      is_verified?: boolean | null;
      is_premium?: boolean | null;
      lang_code?: string | null;
    } = {}
  ): Promise<number> {
    const cached = userCache.get(externalId);
    // If cached, we might still want to update if we have new 'extra' info that was missing?
    // For now, let's assume cache hit is fine, but strictly speaking "Sync" implies updating.
    // However, repeated updates in a loop is expensive.
    // Optimization: If we are processing participants (rich data), always upsert.
    // If processing messages (sparse), trust cache.
    // But 'cached' is just the ID.
    if (cached !== undefined && Object.keys(extra).length === 0) return cached;

    // Prefer explicit handle (from Telethon participant.username),
    // otherwise derive from display_name.
    const handle = explicitHandle
      ? (explicitHandle.startsWith('@') ? explicitHandle : '@' + explicitHandle)
      : displayName
        ? '@' + displayName.toLowerCase().replace(/\s+/g, '')
        : null;

    const res = await client.query(
      `INSERT INTO users (
         platform, external_id, handle, display_name, bio,
         is_scam, is_fake, is_verified, is_premium, lang_code
       )
       VALUES ('telegram', $1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (platform, external_id)
       DO UPDATE SET
         display_name = COALESCE(EXCLUDED.display_name, users.display_name),
         handle = COALESCE(EXCLUDED.handle, users.handle),
         bio = COALESCE(EXCLUDED.bio, users.bio),
         is_scam = COALESCE(EXCLUDED.is_scam, users.is_scam),
         is_fake = COALESCE(EXCLUDED.is_fake, users.is_fake),
         is_verified = COALESCE(EXCLUDED.is_verified, users.is_verified),
         is_premium = COALESCE(EXCLUDED.is_premium, users.is_premium),
         lang_code = COALESCE(EXCLUDED.lang_code, users.lang_code),
         updated_at = now()
       RETURNING id`,
      [
        externalId, 
        handle, 
        displayName, 
        bio || null,
        extra.is_scam ?? null,
        extra.is_fake ?? null,
        extra.is_verified ?? null,
        extra.is_premium ?? null,
        extra.lang_code ?? null
      ],
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
  const totalMessages = exportData.messages.length;
  const t0 = Date.now();
  let processed = 0;

  for (const rawMsg of exportData.messages) {
    processed++;

    // Progress every 1000 messages
    if (processed % 1000 === 0 || processed === totalMessages) {
      const elapsed = (Date.now() - t0) / 1000;
      const rate = elapsed > 0 ? Math.round(processed / elapsed) : 0;
      const remaining = totalMessages - processed;
      const eta = rate > 0 ? Math.round(remaining / rate) : 0;
      const pct = Math.round((processed / totalMessages) * 100);
      console.log(
        `   ... ${processed.toLocaleString()}/${totalMessages.toLocaleString()} messages (${pct}%, ${rate} msg/s, ETA ${eta}s)`
      );
    }

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

    // Insert message (ON CONFLICT for idempotent re-import)
    const msgRes = await client.query(
      `INSERT INTO messages (
         group_id, user_id, external_message_id, sent_at,
         text, text_len, reply_to_external_message_id,
         has_links, has_mentions, raw_ref_row_id,
         views, forwards, reply_count, reaction_count, reactions, media_type
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
       ON CONFLICT (group_id, external_message_id) DO UPDATE SET
         views = EXCLUDED.views,
         forwards = EXCLUDED.forwards,
         reply_count = EXCLUDED.reply_count,
         reaction_count = EXCLUDED.reaction_count,
         reactions = EXCLUDED.reactions,
         updated_at = now()
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
        msg.views ?? 0,
        msg.forwards ?? 0,
        msg.reply_count ?? 0,
        msg.reaction_count_total ?? 0,
        msg.reactions ? JSON.stringify(msg.reactions) : '[]',
        msg.media_type ?? null
      ],
    );
    if (msgRes.rows.length === 0) {
      // Message already existed — skip mentions but still track membership
      trackMembership(userId, sentAt);
      continue;
    }
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
  console.log(`\n   Flushing ${membershipMap.size.toLocaleString()} memberships...`);
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

  // ── Process participants (Telethon exports only) ──
  const hasParticipantData =
    exportData.participants &&
    exportData.participants.length > 0;

  if (hasParticipantData) {
    // Track participant user IDs so we can mark non-participants as departed
    const participantUserIds = new Set<number>();
    const totalParticipants = exportData.participants!.length;
    let partProcessed = 0;

    console.log(`\n   Processing ${totalParticipants.toLocaleString()} participants...`);

    for (const rawPart of exportData.participants!) {
      partProcessed++;
      if (partProcessed % 1000 === 0) {
        console.log(`   ... ${partProcessed.toLocaleString()}/${totalParticipants.toLocaleString()} participants`);
      }

      const part = parseParticipant(rawPart);
      if (!part) continue;

      // Skip bots and deleted accounts
      if (part.bot || part.deleted) continue;

      // Normalize external_id to match message from_id format
      const extId = `user${part.user_id}`;
      const displayName = part.display_name
        ?? ([part.first_name, part.last_name].filter(Boolean).join(' ') || null);
      const handle = part.username ?? null;
      // @ts-ignore - parseParticipant returns any, explicit cast or extension needed if 'about' is added to schema
      const bio = part.about || part.bio || null;

      if (bio) console.log(`   Detailed logging: Found bio for ${extId}: "${bio}"`);
      const userId = await ensureUser(extId, displayName, handle, bio, {
        is_scam: part.scam,
        is_fake: part.fake,
        is_verified: part.verified,
        is_premium: part.premium,
        lang_code: part.lang_code
      });
      stats.participantsIngested++;
      participantUserIds.add(userId);

      // Create membership if not already tracked from messages
      if (!membershipMap.has(userId)) {
        await client.query(
          `INSERT INTO memberships (group_id, user_id, first_seen_at, last_seen_at, msg_count, is_current_member)
           VALUES ($1, $2, NULL, NULL, 0, TRUE)
           ON CONFLICT (group_id, user_id)
           DO UPDATE SET is_current_member = TRUE`,
          [groupId, userId],
        );
        stats.membershipsFromParticipants++;
      } else {
        // Already have a membership from messages — mark as current
        await client.query(
          `UPDATE memberships SET is_current_member = TRUE
           WHERE group_id = $1 AND user_id = $2`,
          [groupId, userId],
        );
      }
    }

    // Mark any membership for this group that is NOT in the participant list
    // as is_current_member = FALSE (departed users known only from messages)
    const participantIdArray = Array.from(participantUserIds);
    if (participantIdArray.length > 0) {
      const departed = await client.query(
        `UPDATE memberships
         SET is_current_member = FALSE
         WHERE group_id = $1
           AND is_current_member IS DISTINCT FROM FALSE
           AND user_id != ALL($2::bigint[])
         RETURNING user_id`,
        [groupId, participantIdArray],
      );
      console.log(`   ✅ ${participantUserIds.size.toLocaleString()} current members, ${departed.rowCount} departed`);
    }
  }

  return stats;
}

// ── Entry point ─────────────────────────────────────────

main().catch((err) => {
  console.error('❌ Ingest failed:', err);
  process.exit(1);
});
