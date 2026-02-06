/**
 * CLI: Export a curated sample of user profiles with full messages
 * for AI-assisted taxonomy review.
 *
 * Picks 3 users per classified role + top uncategorized heavy posters.
 * Includes: TG metadata, our claims/evidence, and ALL messages.
 *
 * Usage:
 *   npm run export-sample
 *   npm run export-sample -- --per-role 5 --max-msgs 100
 */

import { db } from '../db/index.js';
import { writeFileSync, mkdirSync } from 'fs';
import { resolve } from 'path';

// ── Config ──────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name: string, fallback: string): string {
  const idx = args.indexOf(`--${name}`);
  return idx >= 0 && args[idx + 1] ? args[idx + 1] : fallback;
}

const PER_ROLE = parseInt(getArg('per-role', '3'), 10);
const MAX_MSGS = parseInt(getArg('max-msgs', '200'), 10);
const UNCATEGORIZED_COUNT = parseInt(getArg('uncategorized', '5'), 10);
const OUT_DIR = getArg('out-dir', 'data/output');
const MODEL_VERSION = getArg('model-version', 'v0.4.0');

// ── Main ────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log('\n📋 Exporting taxonomy review sample...\n');

  // 1. Find sample user IDs: top N per role + uncategorized heavy posters
  const roleUsers = await db.query<{ user_id: number; role: string; confidence: number }>(`
    WITH ranked AS (
      SELECT subject_user_id AS user_id, object_value AS role, confidence,
        ROW_NUMBER() OVER (PARTITION BY object_value ORDER BY confidence DESC) AS rn
      FROM claims
      WHERE model_version = $1 AND predicate = 'has_role'
    )
    SELECT user_id, role, confidence FROM ranked WHERE rn <= $2
    ORDER BY role, confidence DESC
  `, [MODEL_VERSION, PER_ROLE]);

  // Users with org_type claims — include a sample so the AI reviewer can evaluate them
  const orgTypeUsers = await db.query<{ user_id: number; org_type: string }>(`
    WITH ranked AS (
      SELECT subject_user_id AS user_id, object_value AS org_type,
        ROW_NUMBER() OVER (PARTITION BY object_value ORDER BY confidence DESC) AS rn
      FROM claims
      WHERE model_version = $1 AND predicate = 'has_org_type'
    )
    SELECT user_id, org_type FROM ranked WHERE rn <= 2
    ORDER BY org_type
  `, [MODEL_VERSION]);

  // Users with messages but no role claim at all — OR if none exist,
  // grab the heaviest posters as additional review samples
  const uncategorized = await db.query<{ user_id: number; msg_count: number }>(`
    (
      SELECT m.user_id, COUNT(*) AS msg_count
      FROM messages m
      WHERE m.user_id IS NOT NULL
        AND m.user_id NOT IN (
          SELECT subject_user_id FROM claims
          WHERE model_version = $1 AND predicate = 'has_role'
        )
      GROUP BY m.user_id
      HAVING COUNT(*) >= 3
      ORDER BY COUNT(*) DESC
      LIMIT $2
    )
    UNION ALL
    (
      SELECT m.user_id, COUNT(*) AS msg_count
      FROM messages m
      WHERE m.user_id IS NOT NULL
        AND m.user_id NOT IN (
          SELECT user_id FROM (
            WITH ranked AS (
              SELECT subject_user_id AS user_id,
                ROW_NUMBER() OVER (PARTITION BY object_value ORDER BY confidence DESC) AS rn
              FROM claims
              WHERE model_version = $1 AND predicate = 'has_role'
            )
            SELECT user_id FROM ranked WHERE rn <= $2
          ) already_selected
        )
      GROUP BY m.user_id
      ORDER BY COUNT(*) DESC
      LIMIT $2
    )
    LIMIT $2
  `, [MODEL_VERSION, UNCATEGORIZED_COUNT]);

  const userIds = [
    ...roleUsers.rows.map(r => r.user_id),
    ...orgTypeUsers.rows.map(r => r.user_id),
    ...uncategorized.rows.map(r => r.user_id),
  ];
  const uniqueIds = [...new Set(userIds)];

  console.log(`   Selected ${roleUsers.rows.length} classified users (${PER_ROLE}/role)`);
  console.log(`   Selected ${orgTypeUsers.rows.length} org-type users (2/type)`);
  console.log(`   Selected ${uncategorized.rows.length} uncategorized heavy posters`);
  console.log(`   Total sample: ${uniqueIds.length} users\n`);

  // 2. Build profiles
  const profiles: Record<string, unknown>[] = [];

  for (const userId of uniqueIds) {
    // User metadata
    const userRes = await db.query<{
      id: number; display_name: string | null; bio: string | null;
      external_id: string | null; handle: string | null;
    }>('SELECT id, display_name, bio, external_id, handle FROM users WHERE id = $1', [userId]);
    const user = userRes.rows[0];
    if (!user) continue;

    // Memberships
    const memRes = await db.query<{
      group_title: string; group_kind: string; msg_count: number;
      first_seen: string; last_seen: string; is_current_member: boolean | null;
    }>(`
      SELECT g.title AS group_title, g.kind AS group_kind,
        ms.msg_count, ms.first_seen_at, ms.last_seen_at, ms.is_current_member
      FROM memberships ms
      JOIN groups g ON g.id = ms.group_id
      WHERE ms.user_id = $1
    `, [userId]);

    // All claims for this user (this model version)
    const claimsRes = await db.query<{
      predicate: string; object_value: string; confidence: number;
      status: string; evidence_type: string; evidence_ref: string; weight: number;
    }>(`
      SELECT c.predicate, c.object_value, c.confidence, c.status,
        ce.evidence_type, ce.evidence_ref, ce.weight
      FROM claims c
      LEFT JOIN claim_evidence ce ON ce.claim_id = c.id
      WHERE c.subject_user_id = $1 AND c.model_version = $2
      ORDER BY c.predicate, c.confidence DESC
    `, [userId, MODEL_VERSION]);

    // Group claims by predicate+value
    const claimsMap = new Map<string, {
      predicate: string; object_value: string; confidence: number;
      status: string; evidence: { type: string; ref: string; weight: number }[];
    }>();
    for (const row of claimsRes.rows) {
      const key = `${row.predicate}::${row.object_value}`;
      if (!claimsMap.has(key)) {
        claimsMap.set(key, {
          predicate: row.predicate,
          object_value: row.object_value,
          confidence: row.confidence,
          status: row.status,
          evidence: [],
        });
      }
      if (row.evidence_type) {
        claimsMap.get(key)!.evidence.push({
          type: row.evidence_type,
          ref: row.evidence_ref,
          weight: row.weight,
        });
      }
    }

    // Messages (newest first, capped)
    const msgsRes = await db.query<{
      sent_at: string; text: string | null; has_links: boolean;
      has_mentions: boolean; reply_to: string | null;
    }>(`
      SELECT sent_at, text, has_links, has_mentions,
        reply_to_external_message_id AS reply_to
      FROM messages
      WHERE user_id = $1 AND text IS NOT NULL
      ORDER BY sent_at DESC
      LIMIT $2
    `, [userId, MAX_MSGS]);

    // Feature aggregates
    const featRes = await db.query<{
      total_msg_count: string; total_reply_count: string;
      total_mention_count: string; avg_msg_len: string;
    }>(`
      SELECT SUM(msg_count)::text AS total_msg_count,
        SUM(reply_count)::text AS total_reply_count,
        SUM(mention_count)::text AS total_mention_count,
        AVG(avg_msg_len)::text AS avg_msg_len
      FROM user_features_daily WHERE user_id = $1
    `, [userId]);
    const feat = featRes.rows[0];

    // Selection reason
    const roleMatch = roleUsers.rows.find(r => r.user_id === userId);
    const uncatMatch = uncategorized.rows.find(r => r.user_id === userId);
    const selectionReason = roleMatch
      ? `classified as ${roleMatch.role} (confidence=${roleMatch.confidence})`
      : `uncategorized heavy poster (${uncatMatch?.msg_count} messages)`;

    profiles.push({
      _selection_reason: selectionReason,
      telegram_metadata: {
        user_id: user.id,
        external_id: user.external_id,
        handle: user.handle,
        display_name: user.display_name,
        bio: user.bio,
        memberships: memRes.rows,
      },
      our_claims: [...claimsMap.values()],
      features: feat ? {
        total_messages: parseInt(feat.total_msg_count ?? '0', 10),
        total_replies: parseInt(feat.total_reply_count ?? '0', 10),
        total_mentions: parseInt(feat.total_mention_count ?? '0', 10),
        avg_msg_length: parseFloat(parseFloat(feat.avg_msg_len ?? '0').toFixed(1)),
      } : null,
      messages: msgsRes.rows.map(m => ({
        sent_at: m.sent_at,
        text: m.text,
        is_reply: m.reply_to !== null,
        has_links: m.has_links,
        has_mentions: m.has_mentions,
      })),
      message_count_in_sample: msgsRes.rows.length,
    });

    console.log(`   ✅ ${user.display_name ?? user.id} — ${selectionReason} (${msgsRes.rows.length} msgs)`);
  }

  // 3. Write output
  mkdirSync(resolve(OUT_DIR), { recursive: true });
  const outPath = resolve(OUT_DIR, 'taxonomy_review_sample.json');

  const output = {
    _meta: {
      description: 'Curated sample for AI-assisted taxonomy review. Contains user metadata, our inferred claims, and their messages.',
      exported_at: new Date().toISOString(),
      model_version: MODEL_VERSION,
      sample_strategy: `${PER_ROLE} highest-confidence users per role + ${UNCATEGORIZED_COUNT} uncategorized heavy posters`,
      max_messages_per_user: MAX_MSGS,
      total_users: profiles.length,
      prompt_hint: 'Review each user\'s display_name, messages, and our claims. Are the role/intent classifications correct? What signals did we miss? What new roles, intents, or keyword patterns would improve coverage?',
    },
    taxonomy: {
      roles: ['bd', 'builder', 'founder_exec', 'investor_analyst', 'recruiter', 'vendor_agency', 'community', 'media_kol', 'market_maker'],
      intents: ['networking', 'evaluating', 'selling', 'hiring', 'support_seeking', 'support_giving', 'broadcasting'],
      evidence_sources: ['display_name', 'bio', 'message', 'feature'],
    },
    users: profiles,
  };

  writeFileSync(outPath, JSON.stringify(output, null, 2));

  const sizeMB = (Buffer.byteLength(JSON.stringify(output)) / (1024 * 1024)).toFixed(1);
  console.log(`\n   📁 ${outPath} (${sizeMB} MB, ${profiles.length} users)`);
  console.log('\n✅ Sample export complete.\n');

  await db.close();
}

main().catch((err) => {
  console.error('❌ export-sample failed:', err);
  process.exit(1);
});
