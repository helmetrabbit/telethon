
import { db } from '../db/index.js';
import fs from 'fs';
import path from 'path';

async function main() {
  const version = 'v0.6.0';
  console.log(`Exporting data for version ${version}...`);

  // 1. Get Claims
  console.log('Fetching claims...');
  const claimsRes = await db.query(`
    SELECT 
      c.id, c.subject_user_id, u.display_name, u.bio, 
      c.predicate, c.object_value, c.status, c.confidence, c.notes
    FROM claims c
    JOIN users u ON u.id = c.subject_user_id
    WHERE c.model_version = $1
    ORDER BY c.confidence DESC
  `, [version]);

  // 2. Get Abstentions (Deprecated table, returning empty)
  console.log('Fetching abstentions (skipped)...');
  const abstentionRes = { rows: [] }; 
/*
  const abstentionRes = await db.query(`
    SELECT 
        a.subject_user_id, u.display_name, u.bio,
        a.predicate, a.reason_code, a.details
    FROM abstention_log a
    JOIN users u ON u.id = a.subject_user_id
    WHERE a.model_version = $1
    ORDER BY a.subject_user_id
  `, [version]);
*/

  // 3. Get LLM Enrichments (Deprecated, returning empty)
  console.log('Fetching AI enrichments (skipped)...');
  const enrichmentsRes = { rows: [] };
/*
  const enrichmentsRes = await db.query(`
    SELECT DISTINCT ON (user_id) user_id, parsed_json, created_at
    FROM llm_enrichments
    ORDER BY user_id, id DESC
  `);
*/

  // 4. Get Psychographics
  console.log('Fetching Psychographics...');
  const psychoRes = await db.query(`
    SELECT DISTINCT ON (user_id)
      user_id, tone, professionalism, verbosity, responsiveness, decision_style, 
      seniority_signal, commercial_archetype, approachability, 
      quirks, notable_topics, pain_points, crypto_values, connection_requests, fingerprint_tags,
      based_in, attended_events, preferred_contact_style, reasoning, created_at,
      generated_bio_professional, generated_bio_personal, primary_role, primary_company,
      deep_skills, affiliations, social_platforms, social_urls, buying_power, languages,
      scam_risk_score, confidence_score, career_stage, tribe_affiliations,
      reputation_score, driving_values, technical_specifics, business_focus,
      fifo, group_tags, reputation_summary,
      total_messages, avg_msg_length, peak_hours, most_active_days,
      total_reactions, avg_reactions_per_msg, total_replies_received, avg_replies_per_msg, engagement_rate,
      last_active_days, top_conversation_partners
    FROM user_psychographics
    ORDER BY user_id, created_at DESC
  `);

  // 5. Activity heatmap: day-of-week × hour-of-day from raw messages (enriched users only)
  console.log('Computing activity heatmap...');
  const heatmapRes = await db.query(`
    SELECT
      EXTRACT(DOW FROM m.sent_at)::int AS dow,
      EXTRACT(HOUR FROM m.sent_at)::int AS hour,
      m.user_id,
      count(*)::int AS cnt
    FROM messages m
    WHERE m.user_id IN (SELECT user_id FROM user_psychographics)
    GROUP BY m.user_id, dow, hour
  `);

  // Build per-user activity grids: { user_id -> [[h0..h23] x 7 days] }
  const userActivity: Record<number, number[][]> = {};
  for (const row of heatmapRes.rows) {
    if (!userActivity[row.user_id]) {
      userActivity[row.user_id] = Array.from({ length: 7 }, () => new Array(24).fill(0));
    }
    userActivity[row.user_id][row.dow][row.hour] += row.cnt;
  }

  // 6. Stats
  const stats = {
     total_claims: claimsRes.rows.length,
     supported_claims: claimsRes.rows.filter(r => r.status === 'supported').length,
     total_abstentions: abstentionRes.rows.length,
     enrichment_count: enrichmentsRes.rows.length,
     psycho_count: psychoRes.rows.length,
     generated_at: new Date().toISOString()
  };

  const data = {
      stats,
      claims: claimsRes.rows,
      abstentions: abstentionRes.rows,
      enrichments: enrichmentsRes.rows,
      psychographics: psychoRes.rows,
      activity: userActivity
  };

  // Write as a JS file to allow opening via file:// protocol without CORS
  const outputPath = path.resolve(process.cwd(), 'viewer/data.js');
  const fileContent = `window.TELETHON_DATA = ${JSON.stringify(data, null, 2)};`;
  
  fs.writeFileSync(outputPath, fileContent);
  console.log(`\n✅ Data exported to ${outputPath}`);
  console.log(`\nNow open viewer/index.html in your browser to view the dashboard.`);
  
  process.exit(0);
}

main().catch(e => {
    console.error(e);
    process.exit(1);
});
