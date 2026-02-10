#!/usr/bin/env node
/**
 * Ultra-fast bulk ingestion using COPY
 * Bypasses all ORM overhead and uses Postgres COPY FROM STDIN
 */

import { parseArgs } from '../utils.js';
import { db } from '../db/index.js';
import fs from 'fs';
import path from 'path';
import { TelegramExportSchema, parseMessage, parseParticipant, normalizeText, parseFromId, extractMentions, hasLinks } from '../parsers/telegram.js';
import crypto from 'crypto';
import { pipeline } from 'stream/promises';
import { from as copyFrom } from 'pg-copy-streams';

async function main() {
  const args = parseArgs();
  const filePath = args['file'];
  
  if (!filePath) {
    console.error('Usage: npm run bulk-ingest -- --file <path>');
    process.exit(1);
  }

  const absPath = path.resolve(process.cwd(), filePath);
  console.log(`\n📂 BULK Ingesting: ${absPath}`);
  
  // Read and parse
  const rawJson = fs.readFileSync(absPath, 'utf-8');
  const sha = crypto.createHash('sha256').update(rawJson).digest('hex').slice(0, 16);
  const exportData = TelegramExportSchema.parse(JSON.parse(rawJson));
  
  console.log(`   SHA-256: ${sha}…`);
  console.log(`   Group: "${exportData.name}"`);
  console.log(`   Messages: ${exportData.messages.length.toLocaleString()}`);
  console.log(`   Participants: ${exportData.participants?.length ?? 0}`);

  const t0 = Date.now();
  
  await db.transaction(async (client) => {
    // 1. Upsert group
    const groupCheck = await client.query(
      `SELECT id FROM groups WHERE external_id = $1`,
      [String(exportData.id)]
    );
    
    let groupId: number;
    if (groupCheck.rows.length > 0) {
      groupId = groupCheck.rows[0].id;
      await client.query(
        `UPDATE groups SET title = $1 WHERE id = $2`,
        [exportData.name, groupId]
      );
    } else {
      const groupRes = await client.query(
        `INSERT INTO groups (external_id, title, kind)
         VALUES ($1, $2, $3::group_kind) RETURNING id`,
        [String(exportData.id), exportData.name, 'unknown']
      );
      groupId = groupRes.rows[0].id;
    }

    // 2. Build user map
    console.log('   Building user map...');
    const userMap = new Map<string, number>();
    
    // Process participants first
    for (const rawP of exportData.participants || []) {
      const p = parseParticipant(rawP);
      if (!p) continue;
      
      const extId = `user${p.user_id}`;
      const handle = p.username || null;
      const displayName = p.display_name || p.first_name || p.username || null;
      const bio = p.about || p.bio || null;
      
      const res = await client.query(
        `INSERT INTO users (platform, external_id, handle, display_name, bio, is_scam, is_fake, is_verified, is_premium, lang_code)
         VALUES ('telegram', $1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (platform, external_id) DO UPDATE SET
           handle = COALESCE(EXCLUDED.handle, users.handle),
           display_name = COALESCE(EXCLUDED.display_name, users.display_name),
           bio = COALESCE(EXCLUDED.bio, users.bio),
           is_scam = COALESCE(EXCLUDED.is_scam, users.is_scam),
           is_fake = COALESCE(EXCLUDED.is_fake, users.is_fake),
           is_verified = COALESCE(EXCLUDED.is_verified, users.is_verified),
           is_premium = COALESCE(EXCLUDED.is_premium, users.is_premium),
           lang_code = COALESCE(EXCLUDED.lang_code, users.lang_code)
         RETURNING id`,
        [extId, handle, displayName, bio, p.scam, p.fake, p.verified, p.premium, p.lang_code]
      );
      userMap.set(extId, res.rows[0].id);
    }
    
    console.log(`   ✅ ${userMap.size} users ready`);

    // 4. Prepare message data
    console.log('   Preparing message data...');
    const messages: string[] = [];
    let skipped = 0;
    
    for (const rawMsg of exportData.messages) {
      const msg = parseMessage(rawMsg);
      if (!msg) {
        skipped++;
        continue;
      }
      
      const fromExtId = parseFromId(msg.from_id);
      if (!fromExtId) {
        skipped++;
        continue;
      }
      
      // Ensure user exists
      if (!userMap.has(fromExtId)) {
        const res = await client.query(
          `INSERT INTO users (platform, external_id, display_name)
           VALUES ('telegram', $1, $2)
           ON CONFLICT (platform, external_id) DO UPDATE SET display_name = COALESCE(EXCLUDED.display_name, users.display_name)
           RETURNING id`,
          [fromExtId, msg.from]
        );
        userMap.set(fromExtId, res.rows[0].id);
      }
      
      const userId = userMap.get(fromExtId)!;
      const plainText = normalizeText(msg.text);
      const textLen = plainText.length;
      const msgHasLinks = hasLinks(plainText);
      const mentions = extractMentions(plainText);
      const msgHasMentions = mentions.length > 0;
      
      // Escape for COPY
      const escape = (val: any) => {
        if (val === null || val === undefined) return '\\N';
        if (typeof val === 'string') return val.replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/\t/g, '\\t');
        return String(val);
      };
      
      const reactionCount = msg.reactions?.results?.reduce((sum, r) => sum + r.count, 0) ?? 0;
      const reactionsJson = msg.reactions ? JSON.stringify(msg.reactions).replace(/\\/g, '\\\\').replace(/\t/g, '\\t') : '\\N';
      
      const row = [
        groupId,
        userId,
        msg.id,
        msg.date,
        escape(plainText),
        textLen,
        msg.reply_to_message_id ?? '\\N',
        msgHasLinks ? 't' : 'f',
        msgHasMentions ? 't' : 'f',
        '\\N', // raw_ref_row_id
        msg.views ?? 0,
        msg.forwards ?? 0,
        msg.reply_count ?? 0,
        reactionCount,
        reactionsJson,
        msg.media_type ? escape(msg.media_type) : '\\N'
      ].join('\t');
      
      messages.push(row);
    }
    
    console.log(`   ✅ ${messages.length.toLocaleString()} messages prepared (${skipped} skipped)`);

    // 5. COPY into database
    console.log('   🚀 Bulk loading via COPY...');
    const copyStream = client.query(copyFrom(`
      COPY messages (
        group_id, user_id, external_message_id, sent_at,
        text, text_len, reply_to_external_message_id,
        has_links, has_mentions, raw_ref_row_id,
        views, forwards, reply_count, reaction_count, reactions, media_type
      ) FROM STDIN
    `));
    
    const dataStream = require('stream').Readable.from(messages.map(m => m + '\n'));
    await pipeline(dataStream, copyStream);
    
    const elapsed = (Date.now() - t0) / 1000;
    const rate = Math.round(messages.length / elapsed);
    
    console.log(`\n✅ Bulk ingest complete:`);
    console.log(`   Messages:  ${messages.length.toLocaleString()}`);
    console.log(`   Users:     ${userMap.size.toLocaleString()}`);
    console.log(`   Time:      ${elapsed.toFixed(1)}s`);
    console.log(`   Rate:      ${rate} msg/s`);
  });

  // ── Backfill memberships from messages ──────────────
  console.log('\n📋 Backfilling memberships from messages...');
  const { rowCount: membershipCount } = await db.query(`
    INSERT INTO memberships (group_id, user_id, first_seen_at, last_seen_at, msg_count, is_current_member)
    SELECT
      m.group_id,
      m.user_id,
      MIN(m.sent_at) AS first_seen_at,
      MAX(m.sent_at) AS last_seen_at,
      COUNT(*)::int AS msg_count,
      true AS is_current_member
    FROM messages m
    WHERE m.user_id IS NOT NULL AND m.group_id IS NOT NULL
    GROUP BY m.group_id, m.user_id
    ON CONFLICT (group_id, user_id) DO UPDATE
    SET last_seen_at = EXCLUDED.last_seen_at,
        msg_count = EXCLUDED.msg_count,
        is_current_member = true
  `);
  console.log(`   ✅ ${membershipCount} memberships upserted.`);

  await db.close();
}

main().catch((err) => {
  console.error('❌ Bulk ingest failed:', err);
  process.exit(1);
});
