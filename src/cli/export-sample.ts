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
import {
  scoreUser,
  type UserInferenceInput,
  type MessageMeta,
  type TraceResult,
  type MessageHit,
} from '../inference/engine.js';
import { loadInferenceConfig } from '../config/inference-config.js';
import type { GroupKind } from '../config/taxonomies.js';

// ── Config ──────────────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name: string, fallback: string): string {
  const idx = args.indexOf(`--${name}`);
  return idx >= 0 && args[idx + 1] ? args[idx + 1] : fallback;
}

const PER_ROLE = parseInt(getArg('per-role', '3'), 10);
const PAGE = parseInt(getArg('page', '1'), 10);
const MAX_MSGS = parseInt(getArg('max-msgs', '200'), 10);
const UNCATEGORIZED_COUNT = parseInt(getArg('uncategorized', '5'), 10);
const OUT_DIR = getArg('out-dir', 'data/output');
const MODEL_VERSION = getArg('model-version', 'v0.5.8');

// ── Main ────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log(`\n📋 Exporting taxonomy review sample (page ${PAGE})...\n`);

  const offset = (PAGE - 1) * PER_ROLE;

  // 1. Find sample user IDs: N per role (paged) + uncategorized heavy posters
  const roleUsers = await db.query<{ user_id: number; role: string; confidence: number }>(`
    WITH ranked AS (
      SELECT subject_user_id AS user_id, object_value AS role, confidence,
        ROW_NUMBER() OVER (PARTITION BY object_value ORDER BY confidence DESC) AS rn
      FROM claims
      WHERE model_version = $1 AND predicate = 'has_role'
    )
    SELECT user_id, role, confidence FROM ranked WHERE rn > $2 AND rn <= $3
    ORDER BY role, confidence DESC
  `, [MODEL_VERSION, offset, offset + PER_ROLE]);

  // Users with org_type claims — include a sample so the AI reviewer can evaluate them
  const orgOffset = (PAGE - 1) * 2;
  const orgTypeUsers = await db.query<{ user_id: number; org_type: string }>(`
    WITH ranked AS (
      SELECT subject_user_id AS user_id, object_value AS org_type,
        ROW_NUMBER() OVER (PARTITION BY object_value ORDER BY confidence DESC) AS rn
      FROM claims
      WHERE model_version = $1 AND predicate = 'has_org_type'
    )
    SELECT user_id, org_type FROM ranked WHERE rn > $2 AND rn <= $3
    ORDER BY org_type
  `, [MODEL_VERSION, orgOffset, orgOffset + 2]);

  // Users with messages but no role claim at all — OR if none exist,
  // grab the heaviest posters as additional review samples
  const uncatOffset = (PAGE - 1) * UNCATEGORIZED_COUNT;
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
      LIMIT $2 OFFSET $3
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
      LIMIT $2 OFFSET $3
    )
    LIMIT $2
  `, [MODEL_VERSION, UNCATEGORIZED_COUNT, uncatOffset]);

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

  // 2. Build profiles with trace data
  const config = loadInferenceConfig();
  const profiles: Record<string, unknown>[] = [];
  const traceProfiles: Record<string, unknown>[] = []; // condensed trace-only output

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

    // Messages (newest first, capped) — with IDs for trace
    const msgsRes = await db.query<{
      id: number; sent_at: string; text: string | null; has_links: boolean;
      has_mentions: boolean; reply_to: string | null;
    }>(`
      SELECT id, sent_at, text, has_links, has_mentions,
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
      bd_group_msg_share: string; groups_active_count: string;
    }>(`
      SELECT SUM(msg_count)::text AS total_msg_count,
        SUM(reply_count)::text AS total_reply_count,
        SUM(mention_count)::text AS total_mention_count,
        AVG(avg_msg_len)::text AS avg_msg_len,
        AVG(bd_group_msg_share)::text AS bd_group_msg_share,
        MAX(groups_active_count)::text AS groups_active_count
      FROM user_features_daily WHERE user_id = $1
    `, [userId]);
    const feat = featRes.rows[0];

    // Memberships for group_kinds
    const groupKinds: GroupKind[] = memRes.rows.map(r => r.group_kind as GroupKind);

    // Build message texts + metas for trace mode
    const messageTexts = msgsRes.rows
      .filter(m => m.text != null)
      .map(m => m.text!);
    const messageMetas: MessageMeta[] = msgsRes.rows
      .filter(m => m.text != null)
      .map(m => ({
        id: m.id,
        text: m.text!,
        sent_at: m.sent_at,
      }));

    // Re-run scoreUser with trace=true
    const traceInput: UserInferenceInput = {
      userId,
      displayName: user.display_name,
      bio: user.bio,
      memberGroupKinds: groupKinds,
      messageTexts,
      messageMetas,
      trace: true,
      totalMsgCount: parseInt(feat?.total_msg_count ?? '0', 10),
      totalReplyCount: parseInt(feat?.total_reply_count ?? '0', 10),
      totalMentionCount: parseInt(feat?.total_mention_count ?? '0', 10),
      avgMsgLen: parseFloat(feat?.avg_msg_len ?? '0'),
      bdGroupMsgShare: parseFloat(feat?.bd_group_msg_share ?? '0'),
      groupsActiveCount: parseInt(feat?.groups_active_count ?? '0', 10),
    };
    const traceResult = scoreUser(traceInput, config);

    // Build debug_trace for each emitted claim
    const debugTraces: Record<string, unknown>[] = [];
    const allClaims = [...claimsMap.values()];
    for (const claim of allClaims) {
      // Match to scored label from traceResult
      let finalScore: number | null = null;
      let probability: number | null = null;
      let evidenceItems: { evidence_ref: string; source_type: string; pattern_id: string; weight: number }[] = [];

      if (claim.predicate === 'has_role' && traceResult.roleClaim?.label === claim.object_value) {
        finalScore = traceResult.roleClaim.score;
        probability = traceResult.roleClaim.probability;
        evidenceItems = traceResult.roleClaim.evidence.map(e => ({
          evidence_ref: e.evidence_ref,
          source_type: e.evidence_type,
          pattern_id: e.evidence_ref.split(':')[1] ?? e.evidence_ref,
          weight: e.weight,
        }));
      } else if (claim.predicate === 'has_intent' && traceResult.intentClaim?.label === claim.object_value) {
        finalScore = traceResult.intentClaim.score;
        probability = traceResult.intentClaim.probability;
        evidenceItems = traceResult.intentClaim.evidence.map(e => ({
          evidence_ref: e.evidence_ref,
          source_type: e.evidence_type,
          pattern_id: e.evidence_ref.split(':')[1] ?? e.evidence_ref,
          weight: e.weight,
        }));
      }

      // Find message-level hits for this claim
      const msgHitsForClaim = (traceResult.trace?.message_hits ?? []).filter(
        h => h.label === claim.object_value,
      ).map(h => ({
        message_id: h.message_id,
        sent_at: h.sent_at,
        matched_span: h.matched_span,
        pattern_id: h.pattern_id,
        text_snippet: h.text_snippet,
        weight: h.weight,
      }));

      debugTraces.push({
        predicate: claim.predicate,
        object_value: claim.object_value,
        final_score: finalScore != null ? parseFloat(finalScore.toFixed(4)) : null,
        probability: probability != null ? parseFloat(probability.toFixed(6)) : null,
        evidence: evidenceItems,
        message_hits: msgHitsForClaim,
        raw_role_scores: traceResult.trace?.role_scores ?? [],
        raw_intent_scores: traceResult.trace?.intent_scores ?? [],
      });
    }

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
      our_claims: allClaims,
      debug_trace: debugTraces,
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

    // Build condensed trace profile for taxonomy_review_trace.json
    // Top 5 messages by total evidence weight across all hits
    const allMsgHits = traceResult.trace?.message_hits ?? [];
    const msgWeightMap = new Map<number, { totalWeight: number; hit: MessageHit }>();
    for (const hit of allMsgHits) {
      const existing = msgWeightMap.get(hit.message_id);
      if (!existing || hit.weight > existing.totalWeight) {
        msgWeightMap.set(hit.message_id, {
          totalWeight: (existing?.totalWeight ?? 0) + hit.weight,
          hit,
        });
      }
    }
    // Accumulate total weight properly
    for (const hit of allMsgHits) {
      const entry = msgWeightMap.get(hit.message_id)!;
      if (entry.hit !== hit) {
        entry.totalWeight += hit.weight;
      }
    }
    const topMsgHits = [...msgWeightMap.values()]
      .sort((a, b) => b.totalWeight - a.totalWeight)
      .slice(0, 5)
      .map(e => ({
        message_id: e.hit.message_id,
        sent_at: e.hit.sent_at,
        text_snippet: e.hit.text_snippet,
        total_evidence_weight: parseFloat(e.totalWeight.toFixed(3)),
        hits: allMsgHits.filter(h => h.message_id === e.hit.message_id).map(h => ({
          pattern_id: h.pattern_id,
          label: h.label,
          label_type: h.label_type,
          matched_span: h.matched_span,
          weight: h.weight,
        })),
      }));

    // Role claim trace
    const roleClaimTrace = traceResult.roleClaim ? {
      label: traceResult.roleClaim.label,
      raw_score: parseFloat(traceResult.roleClaim.score.toFixed(4)),
      probability: parseFloat(traceResult.roleClaim.probability.toFixed(6)),
      evidence: traceResult.roleClaim.evidence.map(e => ({
        source_type: e.evidence_type,
        evidence_ref: e.evidence_ref,
        pattern_id: e.evidence_ref.split(':')[1] ?? e.evidence_ref,
        weight: e.weight,
      })),
    } : null;

    // Intent claim trace
    const intentClaimTrace = traceResult.intentClaim ? {
      label: traceResult.intentClaim.label,
      raw_score: parseFloat(traceResult.intentClaim.score.toFixed(4)),
      probability: parseFloat(traceResult.intentClaim.probability.toFixed(6)),
      evidence: traceResult.intentClaim.evidence.map(e => ({
        source_type: e.evidence_type,
        evidence_ref: e.evidence_ref,
        pattern_id: e.evidence_ref.split(':')[1] ?? e.evidence_ref,
        weight: e.weight,
      })),
    } : null;

    traceProfiles.push({
      user_id: user.id,
      display_name: user.display_name,
      role_claim_trace: roleClaimTrace,
      intent_claim_trace: intentClaimTrace,
      raw_role_scores: traceResult.trace?.role_scores ?? [],
      raw_intent_scores: traceResult.trace?.intent_scores ?? [],
      top_evidence_messages: topMsgHits,
      gating_notes: traceResult.gatingNotes,
    });
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
      roles: ['bd', 'builder', 'founder_exec', 'investor_analyst', 'recruiter', 'vendor_agency', 'community', 'media_kol'],
      intents: ['networking', 'evaluating', 'selling', 'hiring', 'support_seeking', 'support_giving', 'broadcasting'],
      evidence_sources: ['display_name', 'bio', 'message', 'feature'],
    },
    users: profiles,
  };

  writeFileSync(outPath, JSON.stringify(output, null, 2));

  const sizeMB = (Buffer.byteLength(JSON.stringify(output)) / (1024 * 1024)).toFixed(1);
  console.log(`\n   📁 ${outPath} (${sizeMB} MB, ${profiles.length} users)`);

  // 4. Write trace-only output
  const tracePath = resolve(OUT_DIR, 'taxonomy_review_trace.json');
  const traceOutput = {
    _meta: {
      description: 'Condensed inference trace. For each user: role/intent claim traces with all evidence, raw scores for ALL labels, and top 5 messages that triggered evidence.',
      exported_at: new Date().toISOString(),
      model_version: MODEL_VERSION,
      total_users: traceProfiles.length,
    },
    users: traceProfiles,
  };
  writeFileSync(tracePath, JSON.stringify(traceOutput, null, 2));
  const traceSizeMB = (Buffer.byteLength(JSON.stringify(traceOutput)) / (1024 * 1024)).toFixed(1);
  console.log(`   📁 ${tracePath} (${traceSizeMB} MB, ${traceProfiles.length} users)`);

  console.log('\n✅ Sample export complete.\n');

  await db.close();
}

main().catch((err) => {
  console.error('❌ export-sample failed:', err);
  process.exit(1);
});
