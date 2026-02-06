/**
 * CLI: Compute per-user daily + aggregate features from messages.
 *
 * Usage:
 *   npm run compute-features
 *
 * What it does:
 *   1. For every (user, day) pair in the messages table, computes:
 *      - msg_count:           total messages sent that day
 *      - reply_count:         messages that are replies
 *      - mention_count:       times the user was mentioned by others that day
 *      - avg_msg_len:         average text length
 *      - groups_active_count: distinct groups posted in
 *      - bd_group_msg_share:  fraction of messages in bd-kind groups
 *   2. Upserts into user_features_daily (idempotent).
 *
 * All logic is a single SQL statement — fully auditable.
 */

import { db } from '../db/index.js';

async function main(): Promise<void> {
  console.log('\n📊 Computing user features (daily)...');

  // ── CTE-based upsert ───────────────────────────────
  //
  // We break this into CTEs for clarity:
  //   base_stats:    msg_count, reply_count, avg_msg_len, groups_active per (user, day)
  //   bd_stats:      messages in bd-kind groups per (user, day)
  //   mention_stats: times mentioned by others per (user, day)
  //   combined:      joins all three, calculates bd_group_msg_share

  const sql = `
    WITH base_stats AS (
      SELECT
        m.user_id,
        m.sent_at::date                          AS day,
        COUNT(*)::int                            AS msg_count,
        COUNT(m.reply_to_external_message_id)::int AS reply_count,
        COALESCE(AVG(m.text_len), 0)::real       AS avg_msg_len,
        COUNT(DISTINCT m.group_id)::int          AS groups_active_count
      FROM messages m
      WHERE m.user_id IS NOT NULL
      GROUP BY m.user_id, m.sent_at::date
    ),

    bd_stats AS (
      SELECT
        m.user_id,
        m.sent_at::date AS day,
        COUNT(*)::int   AS bd_msg_count
      FROM messages m
      JOIN groups g ON g.id = m.group_id
      WHERE m.user_id IS NOT NULL
        AND g.kind = 'bd'
      GROUP BY m.user_id, m.sent_at::date
    ),

    mention_stats AS (
      -- Count how many times each user was mentioned (by others) per day.
      -- We join message_mentions → messages to get the day,
      -- and resolve the mentioned user via mentioned_user_id.
      SELECT
        mm.mentioned_user_id AS user_id,
        m.sent_at::date      AS day,
        COUNT(*)::int        AS mention_count
      FROM message_mentions mm
      JOIN messages m ON m.id = mm.message_id
      WHERE mm.mentioned_user_id IS NOT NULL
      GROUP BY mm.mentioned_user_id, m.sent_at::date
    ),

    combined AS (
      SELECT
        b.user_id,
        b.day,
        b.msg_count,
        b.reply_count,
        COALESCE(mn.mention_count, 0)::int       AS mention_count,
        b.avg_msg_len,
        b.groups_active_count,
        CASE
          WHEN b.msg_count > 0
          THEN COALESCE(bd.bd_msg_count, 0)::real / b.msg_count::real
          ELSE 0
        END                                       AS bd_group_msg_share
      FROM base_stats b
      LEFT JOIN bd_stats     bd ON bd.user_id = b.user_id AND bd.day = b.day
      LEFT JOIN mention_stats mn ON mn.user_id = b.user_id AND mn.day = b.day
    )

    INSERT INTO user_features_daily
      (user_id, day, msg_count, reply_count, mention_count,
       avg_msg_len, groups_active_count, bd_group_msg_share)
    SELECT
      user_id, day, msg_count, reply_count, mention_count,
      avg_msg_len, groups_active_count, bd_group_msg_share
    FROM combined

    ON CONFLICT (user_id, day) DO UPDATE SET
      msg_count           = EXCLUDED.msg_count,
      reply_count         = EXCLUDED.reply_count,
      mention_count       = EXCLUDED.mention_count,
      avg_msg_len         = EXCLUDED.avg_msg_len,
      groups_active_count = EXCLUDED.groups_active_count,
      bd_group_msg_share  = EXCLUDED.bd_group_msg_share
  `;

  const result = await db.query(sql);
  const count = result.rowCount ?? 0;

  console.log(`   Upserted ${count} user×day feature rows.`);

  // ── Summary stats ──────────────────────────────────
  const summary = await db.query<{
    total_users: string;
    total_days: string;
    total_rows: string;
    date_range: string;
  }>(`
    SELECT
      COUNT(DISTINCT user_id)::text AS total_users,
      COUNT(DISTINCT day)::text     AS total_days,
      COUNT(*)::text                AS total_rows,
      MIN(day) || ' → ' || MAX(day) AS date_range
    FROM user_features_daily
  `);

  if (summary.rows.length > 0) {
    const s = summary.rows[0];
    console.log(`\n✅ Feature summary:`);
    console.log(`   Users:      ${s.total_users}`);
    console.log(`   Days:       ${s.total_days}`);
    console.log(`   Total rows: ${s.total_rows}`);
    console.log(`   Date range: ${s.date_range}`);
  }

  await db.close();
}

main().catch((err) => {
  console.error('❌ compute-features failed:', err);
  process.exit(1);
});
