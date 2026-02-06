/**
 * CLI: Run deterministic inference engine, emit evidence-gated claims.
 *
 * Usage:
 *   npm run infer-claims
 *
 * What it does:
 *   1. Loads all users with their memberships, bios, messages, and features.
 *   2. Runs the deterministic scoring engine per user.
 *   3. Applies evidence gating (refuses to emit under-evidenced claims).
 *   4. Writes claim + claim_evidence rows in one transaction per user.
 *   5. Reports what was emitted and what was gated.
 *
 * Prerequisite: run ingest + compute-features first.
 */

import { db } from '../db/index.js';
import {
  scoreUser,
  writeClaimWithEvidence,
  writeAbstention,
  type UserInferenceInput,
} from '../inference/engine.js';
import { loadInferenceConfig } from '../config/inference-config.js';
import type { GroupKind } from '../config/taxonomies.js';

// ── Types for DB queries ────────────────────────────────

interface UserRow {
  id: number;
  display_name: string | null;
  bio: string | null;
}

interface MembershipRow {
  user_id: number;
  group_kind: GroupKind;
}

interface MessageTextRow {
  user_id: number;
  text: string;
}

interface FeatureAggRow {
  user_id: number;
  total_msg_count: string;
  total_reply_count: string;
  total_mention_count: string;
  avg_msg_len: string;
  bd_group_msg_share: string;
  groups_active_count: string;
}

// ── Main ────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log('\n🔍 Running inference engine...\n');

  // ── 0. Load versioned inference config ──────────────
  const config = loadInferenceConfig();
  const ver = config.version;

  // ── 1. Clear previous claims for THIS version (idempotent re-run) ──
  // Delete claims for this model_version only. Claims from other versions are preserved.
  // ON DELETE CASCADE removes claim_evidence automatically.
  await db.query('DELETE FROM claims WHERE model_version = $1', [ver]);
  await db.query('DELETE FROM abstention_log WHERE model_version = $1', [ver]);
  console.log(`   Cleared previous claims/abstentions for ${ver}.`);

  // ── 2. Load all users ───────────────────────────────
  const users = await db.query<UserRow>(
    'SELECT id, display_name, bio FROM users ORDER BY id',
  );
  console.log(`   Loaded ${users.rows.length} users.\n`);

  // ── 3. Load memberships (user → group_kinds) ───────
  const memberships = await db.query<MembershipRow>(`
    SELECT m.user_id, g.kind AS group_kind
    FROM memberships m
    JOIN groups g ON g.id = m.group_id
  `);
  const memberMap = new Map<number, GroupKind[]>();
  for (const row of memberships.rows) {
    const arr = memberMap.get(row.user_id) ?? [];
    arr.push(row.group_kind);
    memberMap.set(row.user_id, arr);
  }

  // ── 4. Load message texts per user ─────────────────
  const messages = await db.query<MessageTextRow>(`
    SELECT user_id, text FROM messages
    WHERE user_id IS NOT NULL AND text IS NOT NULL
    ORDER BY user_id, sent_at
  `);
  const msgMap = new Map<number, string[]>();
  for (const row of messages.rows) {
    const arr = msgMap.get(row.user_id) ?? [];
    arr.push(row.text);
    msgMap.set(row.user_id, arr);
  }

  // ── 5. Load aggregated features per user ───────────
  const features = await db.query<FeatureAggRow>(`
    SELECT
      user_id,
      SUM(msg_count)::text           AS total_msg_count,
      SUM(reply_count)::text         AS total_reply_count,
      SUM(mention_count)::text       AS total_mention_count,
      AVG(avg_msg_len)::text         AS avg_msg_len,
      AVG(bd_group_msg_share)::text  AS bd_group_msg_share,
      MAX(groups_active_count)::text AS groups_active_count
    FROM user_features_daily
    GROUP BY user_id
  `);
  const featMap = new Map<number, FeatureAggRow>();
  for (const row of features.rows) {
    featMap.set(row.user_id, row);
  }

  // ── 6. Score each user and write claims ─────────────

  let totalClaims = 0;
  let totalGated = 0;

  for (const user of users.rows) {
    const feat = featMap.get(user.id);
    const input: UserInferenceInput = {
      userId: user.id,
      displayName: user.display_name,
      bio: user.bio,
      memberGroupKinds: memberMap.get(user.id) ?? [],
      messageTexts: msgMap.get(user.id) ?? [],
      totalMsgCount: parseInt(feat?.total_msg_count ?? '0', 10),
      totalReplyCount: parseInt(feat?.total_reply_count ?? '0', 10),
      totalMentionCount: parseInt(feat?.total_mention_count ?? '0', 10),
      avgMsgLen: parseFloat(feat?.avg_msg_len ?? '0'),
      bdGroupMsgShare: parseFloat(feat?.bd_group_msg_share ?? '0'),
      groupsActiveCount: parseInt(feat?.groups_active_count ?? '0', 10),
    };

    const result = scoreUser(input, config);

    console.log(`── ${user.display_name ?? user.id} ──`);

    // Write role claim if gating passed
    if (result.roleClaim) {
      const rc = result.roleClaim;
      const status = rc.probability >= 0.3 ? 'supported' : 'tentative';
      await db.transaction(async (client) => {
        await writeClaimWithEvidence(
          client,
          user.id,
          'has_role',
          rc.label,
          parseFloat(rc.probability.toFixed(4)),
          status,
          rc.evidence,
          ver,
        );
      });
      totalClaims++;
      console.log(
        `   ✅ role: ${rc.label} (p=${rc.probability.toFixed(3)}, status=${status}, evidence=${rc.evidence.length})`,
      );
    }

    // Write intent claim if gating passed
    if (result.intentClaim) {
      const ic = result.intentClaim;
      const status = ic.probability >= 0.3 ? 'supported' : 'tentative';
      await db.transaction(async (client) => {
        await writeClaimWithEvidence(
          client,
          user.id,
          'has_intent',
          ic.label,
          parseFloat(ic.probability.toFixed(4)),
          status,
          ic.evidence,
          ver,
        );
      });
      totalClaims++;
      console.log(
        `   ✅ intent: ${ic.label} (p=${ic.probability.toFixed(3)}, status=${status}, evidence=${ic.evidence.length})`,
      );
    }

    // Write affiliation claims (self-declared only)
    for (const aff of result.affiliations) {
      await db.transaction(async (client) => {
        await writeClaimWithEvidence(
          client,
          user.id,
          'affiliated_with',
          aff,
          0.9, // high confidence — self-declared
          'supported',
          [{ evidence_type: 'bio', evidence_ref: `bio:affiliation:${aff}`, weight: 3.0 }],
          ver,
        );
      });
      totalClaims++;
      console.log(`   ✅ affiliation: ${aff}`);
    }

    // Report and persist gating decisions
    for (const note of result.gatingNotes) {
      totalGated++;
      console.log(`   🚫 ${note}`);

      // Parse gating note to extract predicate and reason
      const predicate = note.startsWith('role:') ? 'has_role' as const
        : note.startsWith('intent:') ? 'has_intent' as const
        : 'has_role' as const;
      const reasonCode = note.includes('GATED — only') ? 'insufficient_evidence'
        : note.includes('GATED — confidence') ? 'low_confidence'
        : 'insufficient_evidence';

      await db.transaction(async (client) => {
        await writeAbstention(client, user.id, predicate, reasonCode, note, ver);
      });
    }

    console.log('');
  }

  // ── 7. Summary ──────────────────────────────────────
  console.log('━'.repeat(50));
  console.log(`✅ Inference complete (${ver}):`);
  console.log(`   Claims emitted:    ${totalClaims}`);
  console.log(`   Claims gated:      ${totalGated} (logged to abstention_log)`);
  console.log(`   Users processed:   ${users.rows.length}`);
  console.log(`   Model version:     ${ver}`);

  await db.close();
}

main().catch((err) => {
  console.error('❌ infer-claims failed:', err);
  process.exit(1);
});
