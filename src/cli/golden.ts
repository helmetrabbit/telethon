/**
 * Golden cohort test — runs inference for a set of real users
 * pulled from the database, then checks expected role + intent.
 *
 * Unlike regress.ts (synthetic in-memory), this exercises the
 * FULL pipeline against actual DB data.
 *
 * Usage:
 *   npm run golden
 *
 * Reads tests/fixtures/taxonomy_golden_cohort_2026-02-07.json,
 * fetches each user's real data from the DB, runs scoreUser(),
 * and checks expected_role / expected_intent.
 *
 * Exit code 0 = all pass, 1 = failures.
 */

import { readFileSync } from 'fs';
import { resolve } from 'path';
import { db } from '../db/index.js';
import { scoreUser, type UserInferenceInput } from '../inference/engine.js';
import { loadInferenceConfig } from '../config/inference-config.js';
import type { GroupKind, Role, Intent } from '../config/taxonomies.js';

// ── Types ───────────────────────────────────────────────

interface GoldenCase {
  id: string;
  user_id: number;
  description: string;
  expected_role: Role | null;
  expected_intent: Intent | null;
  notes: string;
}

interface GoldenSuite {
  _comment: string;
  cases: GoldenCase[];
}

// ── Status computation (mirrors infer-claims.ts Gate 3) ──

function computeStatus(
  probability: number,
  totalMsgCount: number,
  evidence: { evidence_type: string }[],
): 'supported' | 'tentative' {
  const hasSubstantive = evidence.some(
    (e) => e.evidence_type === 'bio' || e.evidence_type === 'message' || e.evidence_type === 'display_name',
  );
  return (probability >= 0.55 && totalMsgCount >= 5 && hasSubstantive)
    ? 'supported'
    : 'tentative';
}

// ── Main ────────────────────────────────────────────────

async function main(): Promise<void> {
  const config = loadInferenceConfig();
  const suitePath = resolve('tests/fixtures/taxonomy_golden_cohort_2026-02-07.json');
  const raw = readFileSync(suitePath, 'utf-8');
  const suite: GoldenSuite = JSON.parse(raw);

  console.log(`\n🏆 Golden cohort test — ${suite.cases.length} users (${config.version})\n`);

  let passed = 0;
  let failed = 0;
  let skipped = 0;
  const failures: string[] = [];

  for (const tc of suite.cases) {
    // ── Fetch real user data from DB ──
    const userRow = await db.query<{
      display_name: string | null;
      bio: string | null;
    }>(`SELECT display_name, bio FROM users WHERE id = $1`, [tc.user_id]);

    if (userRow.rows.length === 0) {
      console.log(`  ⏭  ${tc.id} — user_id ${tc.user_id} not found in DB, skipping`);
      skipped++;
      continue;
    }

    const user = userRow.rows[0];

    // Fetch messages
    const msgRows = await db.query<{ text: string }>(`
      SELECT text FROM messages
      WHERE user_id = $1 AND text IS NOT NULL
      ORDER BY sent_at
      LIMIT 200
    `, [tc.user_id]);

    const messageTexts = msgRows.rows.map(r => r.text);

    // Fetch group memberships
    const groupRows = await db.query<{ group_kind: string }>(`
      SELECT DISTINCT g.kind AS group_kind
      FROM memberships ms
      JOIN groups g ON g.id = ms.group_id
      WHERE ms.user_id = $1
    `, [tc.user_id]);

    const memberGroupKinds = groupRows.rows.map(r => r.group_kind as GroupKind);

    // Fetch aggregated features
    const featRow = await db.query<{
      total_msg_count: string;
      total_reply_count: string;
      total_mention_count: string;
      avg_msg_len: string;
      bd_group_msg_share: string;
      groups_active_count: string;
    }>(`SELECT SUM(msg_count)::text AS total_msg_count,
              SUM(reply_count)::text AS total_reply_count,
              SUM(mention_count)::text AS total_mention_count,
              AVG(avg_msg_len)::text AS avg_msg_len,
              AVG(bd_group_msg_share)::text AS bd_group_msg_share,
              MAX(groups_active_count)::text AS groups_active_count
       FROM user_features_daily WHERE user_id = $1`, [tc.user_id]);

    const feat = featRow.rows[0] ?? {
      total_msg_count: String(messageTexts.length),
      total_reply_count: '0',
      total_mention_count: '0',
      avg_msg_len: String(messageTexts.reduce((s, m) => s + m.length, 0) / (messageTexts.length || 1)),
      bd_group_msg_share: '0',
      groups_active_count: String(memberGroupKinds.length),
    };

    const input: UserInferenceInput = {
      userId: tc.user_id,
      displayName: user.display_name,
      bio: user.bio,
      memberGroupKinds,
      messageTexts,
      totalMsgCount: Number(feat.total_msg_count) || 0,
      totalReplyCount: Number(feat.total_reply_count) || 0,
      totalMentionCount: Number(feat.total_mention_count) || 0,
      avgMsgLen: Number(feat.avg_msg_len) || 0,
      bdGroupMsgShare: Number(feat.bd_group_msg_share) || 0,
      groupsActiveCount: Number(feat.groups_active_count) || 0,
    };

    const result = scoreUser(input, config);
    const errors: string[] = [];

    // Check role
    const actualRole = result.roleClaim?.label ?? null;
    if (tc.expected_role !== actualRole) {
      errors.push(`role: expected=${tc.expected_role ?? 'NONE'}, got=${actualRole ?? 'NONE'} (p=${result.roleClaim?.probability.toFixed(3) ?? '—'})`);
    }

    // Check intent
    const actualIntent = result.intentClaim?.label ?? null;
    if (tc.expected_intent !== null && tc.expected_intent !== actualIntent) {
      errors.push(`intent: expected=${tc.expected_intent}, got=${actualIntent ?? 'NONE'} (p=${result.intentClaim?.probability.toFixed(3) ?? '—'})`);
    }
    // If expected_intent is null, we don't check (it means "don't care")

    if (errors.length === 0) {
      passed++;
      const roleStr = actualRole ? `${actualRole}(${result.roleClaim!.probability.toFixed(3)})` : 'NONE';
      console.log(`  ✅ ${tc.id} → ${roleStr}`);
    } else {
      failed++;
      const summary = `  ❌ ${tc.id}\n${errors.map(e => `     → ${e}`).join('\n')}`;
      console.log(summary);
      failures.push(summary);
    }
  }

  // ── Summary ──────────────────────────────────────────
  console.log('\n' + '━'.repeat(50));
  console.log(`🏆 Golden: ${passed} passed, ${failed} failed, ${skipped} skipped out of ${suite.cases.length}`);

  if (failures.length > 0) {
    console.log('\n📋 Failures:\n');
    for (const f of failures) {
      console.log(f);
    }
    await db.close();
    process.exit(1);
  } else {
    console.log('🎉 All golden cohort tests passed!\n');
    await db.close();
    process.exit(0);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(2);
});
