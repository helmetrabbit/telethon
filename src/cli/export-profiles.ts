/**
 * CLI: Export per-user profile JSON (observed + derived + claims).
 *
 * Usage:
 *   npm run export-profiles
 *   npm run export-profiles -- --user-id 2
 *   npm run export-profiles -- --out-dir data/output
 *
 * Outputs one JSON file per user to data/output/<user_id>_<handle>.json
 * and a combined profiles.json with all users.
 */

import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { db } from '../db/index.js';
import { parseArgs } from '../utils.js';
import { loadInferenceConfig } from '../config/inference-config.js';

// ── Output shape ────────────────────────────────────────

interface ProfileObserved {
  user_id: number;
  platform: string;
  external_id: string;
  handle: string | null;
  display_name: string | null;
  bio: string | null;
  memberships: {
    group_title: string;
    group_kind: string;
    msg_count: number;
    first_seen: string | null;
    last_seen: string | null;
    is_current_member: boolean | null;
  }[];
  message_stats: {
    total_messages: number;
    total_replies: number;
    total_with_links: number;
    total_with_mentions: number;
    date_range: { first: string | null; last: string | null };
  };
}

interface ProfileDerived {
  daily: {
    day: string;
    msg_count: number;
    reply_count: number;
    mention_count: number;
    avg_msg_len: number;
    groups_active_count: number;
    bd_group_msg_share: number;
  }[];
  aggregate: {
    total_days_active: number;
    total_msg_count: number;
    total_reply_count: number;
    total_mention_count: number;
    overall_avg_msg_len: number;
    avg_bd_group_msg_share: number;
    max_groups_active_in_day: number;
  };
}

interface ProfileClaim {
  predicate: string;
  object_value: string;
  confidence: number;
  status: string;
  model_version: string;
  generated_at: string;
  notes: string | null;
  evidence: {
    evidence_type: string;
    evidence_ref: string;
    weight: number;
  }[];
}

interface UserProfile {
  _meta: {
    exported_at: string;
    engine_version: string;
  };
  observed: ProfileObserved;
  derived: ProfileDerived;
  claims: ProfileClaim[];
}

// ── DB query types ──────────────────────────────────────

interface UserRow {
  id: number;
  platform: string;
  external_id: string;
  handle: string | null;
  display_name: string | null;
  bio: string | null;
}

interface MembershipRow {
  group_title: string;
  group_kind: string;
  msg_count: number;
  first_seen_at: string | null;
  last_seen_at: string | null;
  is_current_member: boolean | null;
}

interface MsgStatsRow {
  total_messages: string;
  total_replies: string;
  total_with_links: string;
  total_with_mentions: string;
  first_msg: string | null;
  last_msg: string | null;
}

interface DailyRow {
  day: string;
  msg_count: number;
  reply_count: number;
  mention_count: number;
  avg_msg_len: number;
  groups_active_count: number;
  bd_group_msg_share: number;
}

interface ClaimRow {
  predicate: string;
  object_value: string;
  confidence: number;
  status: string;
  model_version: string;
  generated_at: string;
  notes: string | null;
  claim_id: number;
}

interface EvidenceRow {
  claim_id: number;
  evidence_type: string;
  evidence_ref: string;
  weight: number;
}

// ── Main ────────────────────────────────────────────────

async function main(): Promise<void> {
  const args = parseArgs();
  const outDir = resolve(args['out-dir'] ?? 'data/output');
  const filterUserId = args['user-id'] ? parseInt(args['user-id'], 10) : null;

  mkdirSync(outDir, { recursive: true });

  const inferConfig = loadInferenceConfig();
  const engineVersion = inferConfig.version;

  console.log('\n📤 Exporting user profiles...');
  console.log(`   Output dir: ${outDir}`);
  console.log(`   Engine version: ${engineVersion}`);
  if (filterUserId) console.log(`   Filtering to user_id: ${filterUserId}`);

  // ── Load users ──────────────────────────────────────
  const userQuery = filterUserId
    ? { text: 'SELECT id, platform, external_id, handle, display_name, bio FROM users WHERE id = $1', params: [filterUserId] }
    : { text: 'SELECT id, platform, external_id, handle, display_name, bio FROM users ORDER BY id', params: [] as unknown[] };

  const users = await db.query<UserRow>(userQuery.text, userQuery.params);

  if (users.rows.length === 0) {
    console.log('   No users found.');
    await db.close();
    return;
  }

  const profiles: UserProfile[] = [];
  const totalUsers = users.rows.length;
  let exported = 0;
  const t0 = Date.now();

  for (const user of users.rows) {
    const profile = await buildProfile(user, engineVersion);
    profiles.push(profile);

    // Write individual file
    const safeName = (user.handle ?? user.external_id).replace(/[^a-zA-Z0-9_-]/g, '');
    const fileName = `${user.id}_${safeName}.json`;
    const filePath = resolve(outDir, fileName);
    writeFileSync(filePath, JSON.stringify(profile, null, 2));

    exported++;
    if (exported % 500 === 0 || exported === totalUsers) {
      const elapsed = (Date.now() - t0) / 1000;
      const rate = elapsed > 0 ? Math.round(exported / elapsed) : 0;
      const eta = rate > 0 ? Math.round((totalUsers - exported) / rate) : 0;
      const pct = Math.round((exported / totalUsers) * 100);
      console.log(`   ... ${exported.toLocaleString()}/${totalUsers.toLocaleString()} profiles (${pct}%, ${rate}/s, ETA ${eta}s)`);
    }
  }

  // Write combined file
  const combinedPath = resolve(outDir, 'profiles.json');
  writeFileSync(combinedPath, JSON.stringify(profiles, null, 2));
  console.log(`\n   📁 Combined: profiles.json (${profiles.length} users)`);

  console.log(`\n✅ Export complete: ${profiles.length} profiles written to ${outDir}`);
  await db.close();
}

// ── Build a single user profile ─────────────────────────

async function buildProfile(user: UserRow, engineVersion: string): Promise<UserProfile> {
  // ── Observed: Memberships ─────────────────────────
  const memberships = await db.query<MembershipRow>(
    `SELECT g.title AS group_title, g.kind AS group_kind,
            m.msg_count, m.first_seen_at::text, m.last_seen_at::text,
            m.is_current_member
     FROM memberships m
     JOIN groups g ON g.id = m.group_id
     WHERE m.user_id = $1
     ORDER BY m.msg_count DESC`,
    [user.id],
  );

  // ── Observed: Message stats ───────────────────────
  const msgStats = await db.query<MsgStatsRow>(
    `SELECT
       COUNT(*)::text                                          AS total_messages,
       COUNT(reply_to_external_message_id)::text               AS total_replies,
       COUNT(*) FILTER (WHERE has_links)::text                 AS total_with_links,
       COUNT(*) FILTER (WHERE has_mentions)::text              AS total_with_mentions,
       MIN(sent_at)::text                                      AS first_msg,
       MAX(sent_at)::text                                      AS last_msg
     FROM messages
     WHERE user_id = $1`,
    [user.id],
  );
  const ms = msgStats.rows[0];

  // ── Derived: Daily features ───────────────────────
  const daily = await db.query<DailyRow>(
    `SELECT day::text, msg_count, reply_count, mention_count,
            avg_msg_len, groups_active_count, bd_group_msg_share
     FROM user_features_daily
     WHERE user_id = $1
     ORDER BY day`,
    [user.id],
  );

  // Compute aggregate from daily
  const agg = daily.rows.reduce(
    (acc, d) => ({
      total_days_active: acc.total_days_active + 1,
      total_msg_count: acc.total_msg_count + d.msg_count,
      total_reply_count: acc.total_reply_count + d.reply_count,
      total_mention_count: acc.total_mention_count + d.mention_count,
      sum_avg_msg_len: acc.sum_avg_msg_len + d.avg_msg_len,
      sum_bd_share: acc.sum_bd_share + d.bd_group_msg_share,
      max_groups: Math.max(acc.max_groups, d.groups_active_count),
    }),
    {
      total_days_active: 0,
      total_msg_count: 0,
      total_reply_count: 0,
      total_mention_count: 0,
      sum_avg_msg_len: 0,
      sum_bd_share: 0,
      max_groups: 0,
    },
  );

  // ── Claims + Evidence ─────────────────────────────
  const claimsRes = await db.query<ClaimRow>(
    `SELECT id AS claim_id, predicate, object_value,
            confidence, status, model_version,
            generated_at::text, notes
     FROM claims
     WHERE subject_user_id = $1
     ORDER BY predicate, confidence DESC`,
    [user.id],
  );

  const claimIds = claimsRes.rows.map((c) => c.claim_id);
  let evidenceRows: EvidenceRow[] = [];
  if (claimIds.length > 0) {
    const evidenceRes = await db.query<EvidenceRow>(
      `SELECT claim_id, evidence_type, evidence_ref, weight
       FROM claim_evidence
       WHERE claim_id = ANY($1)
       ORDER BY claim_id, evidence_type`,
      [claimIds],
    );
    evidenceRows = evidenceRes.rows;
  }

  // Group evidence by claim_id
  const evidenceMap = new Map<number, EvidenceRow[]>();
  for (const e of evidenceRows) {
    const arr = evidenceMap.get(e.claim_id) ?? [];
    arr.push(e);
    evidenceMap.set(e.claim_id, arr);
  }

  const claims: ProfileClaim[] = claimsRes.rows.map((c) => ({
    predicate: c.predicate,
    object_value: c.object_value,
    confidence: c.confidence,
    status: c.status,
    model_version: c.model_version,
    generated_at: c.generated_at,
    notes: c.notes,
    evidence: (evidenceMap.get(c.claim_id) ?? []).map((e) => ({
      evidence_type: e.evidence_type,
      evidence_ref: e.evidence_ref,
      weight: e.weight,
    })),
  }));

  // ── Assemble profile ──────────────────────────────
  const daysActive = agg.total_days_active || 1; // avoid div/0

  return {
    _meta: {
      exported_at: new Date().toISOString(),
      engine_version: engineVersion,
    },
    observed: {
      user_id: user.id,
      platform: user.platform,
      external_id: user.external_id,
      handle: user.handle,
      display_name: user.display_name,
      bio: user.bio,
      memberships: memberships.rows.map((m) => ({
        group_title: m.group_title,
        group_kind: m.group_kind,
        msg_count: m.msg_count,
        first_seen: m.first_seen_at,
        last_seen: m.last_seen_at,
        is_current_member: m.is_current_member,
      })),
      message_stats: {
        total_messages: parseInt(ms?.total_messages ?? '0', 10),
        total_replies: parseInt(ms?.total_replies ?? '0', 10),
        total_with_links: parseInt(ms?.total_with_links ?? '0', 10),
        total_with_mentions: parseInt(ms?.total_with_mentions ?? '0', 10),
        date_range: {
          first: ms?.first_msg ?? null,
          last: ms?.last_msg ?? null,
        },
      },
    },
    derived: {
      daily: daily.rows.map((d) => ({
        day: d.day,
        msg_count: d.msg_count,
        reply_count: d.reply_count,
        mention_count: d.mention_count,
        avg_msg_len: d.avg_msg_len,
        groups_active_count: d.groups_active_count,
        bd_group_msg_share: d.bd_group_msg_share,
      })),
      aggregate: {
        total_days_active: agg.total_days_active,
        total_msg_count: agg.total_msg_count,
        total_reply_count: agg.total_reply_count,
        total_mention_count: agg.total_mention_count,
        overall_avg_msg_len: parseFloat((agg.sum_avg_msg_len / daysActive).toFixed(1)),
        avg_bd_group_msg_share: parseFloat((agg.sum_bd_share / daysActive).toFixed(3)),
        max_groups_active_in_day: agg.max_groups,
      },
    },
    claims,
  };
}

// ── Entry point ─────────────────────────────────────────

main().catch((err) => {
  console.error('❌ export-profiles failed:', err);
  process.exit(1);
});
