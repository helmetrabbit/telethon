/**
 * CLI: Export share-safe diagnostics JSON.
 *
 * Produces a privacy-safe summary of the database for taxonomy review
 * and weakness analysis. Contains NO message text, handles, usernames,
 * bios, external_ids, or group titles.
 *
 * Usage:
 *   DIAGNOSTICS_SALT=mysecret npm run export-diagnostics
 *   DIAGNOSTICS_SALT=mysecret npm run export-diagnostics -- --out-file share/diagnostics.json
 *   DIAGNOSTICS_SALT=mysecret npm run export-diagnostics -- --group-external-id 1234567890
 *   DIAGNOSTICS_SALT=mysecret npm run export-diagnostics -- --model-version v0.2.0
 *
 * Requires: DIAGNOSTICS_SALT env var (used for pseudonymization).
 */

import { createHash } from 'node:crypto';
import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { db } from '../db/index.js';
import { parseArgs } from '../utils.js';
import { loadInferenceConfig } from '../config/inference-config.js';

// ── Pseudonymization ────────────────────────────────────

function pseudonymize(salt: string, id: string): string {
  return createHash('sha256')
    .update(salt + id)
    .digest('hex')
    .slice(0, 12);
}

// ── Main ────────────────────────────────────────────────

async function main(): Promise<void> {
  const args = parseArgs();
  const outFile = args['out-file'] || 'share/diagnostics.json';
  const groupFilter = args['group-external-id'] || null;

  // Default model version from inference config
  let modelVersion: string;
  if (args['model-version']) {
    modelVersion = args['model-version'];
  } else {
    const config = loadInferenceConfig();
    modelVersion = config.version;
  }

  // ── Require DIAGNOSTICS_SALT ────────────────────────
  const salt = process.env.DIAGNOSTICS_SALT;
  if (!salt) {
    console.error('❌  DIAGNOSTICS_SALT env var is required.');
    console.error('   Set it to any random string for pseudonymization.');
    console.error('   Example: DIAGNOSTICS_SALT=my_random_salt npm run export-diagnostics');
    process.exit(1);
  }

  console.log('\n🔒 Export diagnostics (share-safe, no PII)');
  console.log(`   Model version: ${modelVersion}`);
  console.log(`   Output:        ${outFile}`);
  if (groupFilter) console.log(`   Group filter:  ${groupFilter}`);

  // ── Build WHERE clause for group filter ─────────────
  const groupWhere = groupFilter
    ? `JOIN groups g_filter ON g_filter.id = sub.group_id WHERE g_filter.external_id = $GROUP`
    : '';
  // We'll use parameterized queries below.

  // ── 1. Counts ─────────────────────────────────────
  const groupCountRes = await db.query<{ count: string }>(
    groupFilter
      ? `SELECT COUNT(*) AS count FROM groups WHERE external_id = $1`
      : `SELECT COUNT(*) AS count FROM groups`,
    groupFilter ? [groupFilter] : [],
  );
  const groupCount = Number(groupCountRes.rows[0].count);

  const userCountRes = await db.query<{ count: string }>(
    groupFilter
      ? `SELECT COUNT(DISTINCT m.user_id) AS count
         FROM memberships m JOIN groups g ON g.id = m.group_id
         WHERE g.external_id = $1`
      : `SELECT COUNT(*) AS count FROM users`,
    groupFilter ? [groupFilter] : [],
  );
  const userCount = Number(userCountRes.rows[0].count);

  const msgCountRes = await db.query<{ count: string; first_date: string | null; last_date: string | null }>(
    groupFilter
      ? `SELECT COUNT(*) AS count,
                MIN(msg.sent_at)::date::text AS first_date,
                MAX(msg.sent_at)::date::text AS last_date
         FROM messages msg JOIN groups g ON g.id = msg.group_id
         WHERE g.external_id = $1`
      : `SELECT COUNT(*) AS count,
                MIN(sent_at)::date::text AS first_date,
                MAX(sent_at)::date::text AS last_date
         FROM messages`,
    groupFilter ? [groupFilter] : [],
  );
  const messageCount = Number(msgCountRes.rows[0].count);
  const dateRange = {
    first: msgCountRes.rows[0].first_date,
    last: msgCountRes.rows[0].last_date,
  };

  // ── 2. Membership distribution by group_kind ──────
  const membershipDistRes = await db.query<{ kind: string; member_count: string }>(
    groupFilter
      ? `SELECT g.kind, COUNT(DISTINCT m.user_id)::text AS member_count
         FROM memberships m JOIN groups g ON g.id = m.group_id
         WHERE g.external_id = $1
         GROUP BY g.kind ORDER BY member_count DESC`
      : `SELECT g.kind, COUNT(DISTINCT m.user_id)::text AS member_count
         FROM memberships m JOIN groups g ON g.id = m.group_id
         GROUP BY g.kind ORDER BY member_count DESC`,
    groupFilter ? [groupFilter] : [],
  );
  const membershipByKind: Record<string, number> = {};
  for (const row of membershipDistRes.rows) {
    membershipByKind[row.kind] = Number(row.member_count);
  }

  // ── 2b. Membership churn (is_current_member) ──────
  const churnRes = await db.query<{ status: string; count: string }>(
    groupFilter
      ? `SELECT
           CASE WHEN m.is_current_member = TRUE THEN 'current'
                WHEN m.is_current_member = FALSE THEN 'departed'
                ELSE 'unknown'
           END AS status,
           COUNT(*)::text AS count
         FROM memberships m JOIN groups g ON g.id = m.group_id
         WHERE g.external_id = $1
         GROUP BY status ORDER BY count DESC`
      : `SELECT
           CASE WHEN m.is_current_member = TRUE THEN 'current'
                WHEN m.is_current_member = FALSE THEN 'departed'
                ELSE 'unknown'
           END AS status,
           COUNT(*)::text AS count
         FROM memberships m
         GROUP BY status ORDER BY count DESC`,
    groupFilter ? [groupFilter] : [],
  );
  const membershipChurn: Record<string, number> = {};
  for (const row of churnRes.rows) {
    membershipChurn[row.status] = Number(row.count);
  }

  // ── 3. Top 20 users by msg_count (pseudonymized) ──
  const topUsersRes = await db.query<{ user_id: string; total_msgs: string }>(
    groupFilter
      ? `SELECT m.user_id::text AS user_id, SUM(m.msg_count)::text AS total_msgs
         FROM memberships m JOIN groups g ON g.id = m.group_id
         WHERE g.external_id = $1
         GROUP BY m.user_id ORDER BY total_msgs DESC LIMIT 20`
      : `SELECT m.user_id::text AS user_id, SUM(m.msg_count)::text AS total_msgs
         FROM memberships m
         GROUP BY m.user_id ORDER BY total_msgs DESC LIMIT 20`,
    groupFilter ? [groupFilter] : [],
  );
  const topUsers = topUsersRes.rows.map((row) => ({
    pseudonym: pseudonymize(salt, row.user_id),
    msg_count: Number(row.total_msgs),
  }));

  // ── 4. Inference summary (for model_version) ─────
  const claimsTotalRes = await db.query<{ count: string }>(
    `SELECT COUNT(*) AS count FROM claims WHERE model_version = $1`,
    [modelVersion],
  );
  const claimsTotal = Number(claimsTotalRes.rows[0].count);

  const claimsByPredicateRes = await db.query<{ predicate: string; count: string }>(
    `SELECT predicate, COUNT(*)::text AS count
     FROM claims WHERE model_version = $1
     GROUP BY predicate ORDER BY count DESC`,
    [modelVersion],
  );
  const claimsByPredicate: Record<string, number> = {};
  for (const row of claimsByPredicateRes.rows) {
    claimsByPredicate[row.predicate] = Number(row.count);
  }

  const claimsByPredObjRes = await db.query<{ predicate: string; object_value: string; count: string }>(
    `SELECT predicate, object_value, COUNT(*)::text AS count
     FROM claims WHERE model_version = $1
     GROUP BY predicate, object_value
     ORDER BY count DESC LIMIT 50`,
    [modelVersion],
  );
  const claimsByPredicateAndValue = claimsByPredObjRes.rows.map((row) => ({
    predicate: row.predicate,
    object_value: row.object_value,
    count: Number(row.count),
  }));

  // ── 5. Evidence type distribution by predicate ────
  const evidenceDistRes = await db.query<{ predicate: string; evidence_type: string; count: string }>(
    `SELECT c.predicate, ce.evidence_type, COUNT(*)::text AS count
     FROM claim_evidence ce
     JOIN claims c ON c.id = ce.claim_id
     WHERE c.model_version = $1
     GROUP BY c.predicate, ce.evidence_type
     ORDER BY c.predicate, count DESC`,
    [modelVersion],
  );
  const evidenceByPredicate: Record<string, Record<string, number>> = {};
  for (const row of evidenceDistRes.rows) {
    if (!evidenceByPredicate[row.predicate]) {
      evidenceByPredicate[row.predicate] = {};
    }
    evidenceByPredicate[row.predicate][row.evidence_type] = Number(row.count);
  }

  // ── 6. Abstentions by predicate + reason ──────────
  const abstentionRes = await db.query<{ predicate: string; reason_code: string; count: string }>(
    `SELECT predicate, reason_code, COUNT(*)::text AS count
     FROM abstention_log WHERE model_version = $1
     GROUP BY predicate, reason_code
     ORDER BY count DESC`,
    [modelVersion],
  );
  const abstentionsByPredicateAndReason = abstentionRes.rows.map((row) => ({
    predicate: row.predicate,
    reason_code: row.reason_code,
    count: Number(row.count),
  }));

  // ── 6b. Detailed Near-Miss Analysis (from details text) ──
  // Extracts "role:bd" or "intent:selling" from the details string to see what is being gated.
  const nearMissRes = await db.query<{ candidate_label: string; reason_code: string; count: string }>(
    `SELECT
       substring(details from '^(role:[a-z_]+|intent:[a-z_]+)') as candidate_label,
       reason_code,
       COUNT(*)::text as count
     FROM abstention_log
     WHERE model_version = $1
     GROUP BY candidate_label, reason_code
     ORDER BY count DESC
     LIMIT 50`,
    [modelVersion],
  );
  const nearMisses = nearMissRes.rows.map((row) => ({
    candidate_label: row.candidate_label || 'unknown',
    reason_code: row.reason_code,
    count: Number(row.count),
  }));

  // ── 7. Coverage metrics ───────────────────────────
  const totalUsersForCoverage = await db.query<{ count: string }>(
    `SELECT COUNT(*) AS count FROM users`,
  );
  const totalUsers = Number(totalUsersForCoverage.rows[0].count);

  const usersWithAnyClaim = await db.query<{ count: string }>(
    `SELECT COUNT(DISTINCT subject_user_id) AS count
     FROM claims WHERE model_version = $1`,
    [modelVersion],
  );
  const usersWithRole = await db.query<{ count: string }>(
    `SELECT COUNT(DISTINCT subject_user_id) AS count
     FROM claims WHERE model_version = $1 AND predicate = 'has_role'`,
    [modelVersion],
  );
  const usersWithIntent = await db.query<{ count: string }>(
    `SELECT COUNT(DISTINCT subject_user_id) AS count
     FROM claims WHERE model_version = $1 AND predicate = 'has_intent'`,
    [modelVersion],
  );
  const usersWithTopic = await db.query<{ count: string }>(
    `SELECT COUNT(DISTINCT subject_user_id) AS count
     FROM claims WHERE model_version = $1 AND predicate = 'has_topic_affinity'`,
    [modelVersion],
  );
  const usersWithZero = totalUsers - Number(usersWithAnyClaim.rows[0].count);

  const pct = (n: number) => totalUsers > 0 ? Math.round((n / totalUsers) * 10000) / 100 : 0;

  const coverage = {
    total_users: totalUsers,
    users_with_any_claim: Number(usersWithAnyClaim.rows[0].count),
    users_with_role: Number(usersWithRole.rows[0].count),
    users_with_intent: Number(usersWithIntent.rows[0].count),
    users_with_topic: Number(usersWithTopic.rows[0].count),
    users_with_zero_claims: usersWithZero,
    pct_any: pct(Number(usersWithAnyClaim.rows[0].count)),
    pct_role: pct(Number(usersWithRole.rows[0].count)),
    pct_intent: pct(Number(usersWithIntent.rows[0].count)),
    pct_topic: pct(Number(usersWithTopic.rows[0].count)),
    pct_zero: pct(usersWithZero),
  };

  // ── Assemble output ───────────────────────────────
  const diagnostics = {
    meta: {
      exported_at: new Date().toISOString(),
      group_filter: groupFilter,
      model_version: modelVersion,
    },
    counts: {
      groups: groupCount,
      users: userCount,
      messages: messageCount,
      date_range: dateRange,
    },
    membership: {
      by_group_kind: membershipByKind,
      churn: membershipChurn,
      top_20_by_msg_count: topUsers,
    },
    inference_summary: {
      claims_count_total: claimsTotal,
      claims_by_predicate: claimsByPredicate,
      claims_by_predicate_and_object_value: claimsByPredicateAndValue,
      evidence_type_distribution_by_predicate: evidenceByPredicate,
      abstentions_by_predicate_and_reason_code: abstentionsByPredicateAndReason,
      near_miss_analysis: nearMisses,
      coverage,
    },
  };

  // ── Write output ──────────────────────────────────
  const absOut = resolve(outFile);
  mkdirSync(dirname(absOut), { recursive: true });
  writeFileSync(absOut, JSON.stringify(diagnostics, null, 2), 'utf-8');

  console.log(`\n✅ Diagnostics exported: ${absOut}`);
  console.log(`   Groups:     ${groupCount}`);
  console.log(`   Users:      ${userCount}`);
  console.log(`   Messages:   ${messageCount}`);
  console.log(`   Claims:     ${claimsTotal} (${modelVersion})`);
  console.log(`   Abstentions:${abstentionsByPredicateAndReason.reduce((s, r) => s + r.count, 0)}`);
  console.log(`   Coverage:   ${coverage.pct_any}% users with ≥1 claim`);

  await db.close();
}

// ── Entry point ─────────────────────────────────────────

main().catch((err) => {
  console.error('❌ Export diagnostics failed:', err);
  process.exit(1);
});
