
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
    SELECT DISTINCT ON (p.user_id)
      p.user_id, u.display_name, u.bio,
      p.tone, p.professionalism, p.verbosity, p.responsiveness, p.decision_style, 
      p.seniority_signal, p.commercial_archetype, p.approachability, 
      p.quirks, p.notable_topics, p.pain_points, p.crypto_values, p.connection_requests, p.fingerprint_tags,
      p.based_in, p.attended_events, p.preferred_contact_style, p.reasoning, p.created_at,
      p.generated_bio_professional, p.generated_bio_personal, p.primary_role, p.primary_company,
      p.deep_skills, p.affiliations, p.social_platforms, p.social_urls, p.buying_power, p.languages,
      p.scam_risk_score, p.confidence_score, p.career_stage, p.tribe_affiliations,
      p.reputation_score, p.driving_values, p.technical_specifics, p.business_focus,
      p.fifo, p.group_tags, p.reputation_summary,
      p.total_messages, p.avg_msg_length, p.peak_hours, p.most_active_days,
      p.total_reactions, p.avg_reactions_per_msg, p.total_replies_received, p.avg_replies_per_msg, p.engagement_rate,
      p.last_active_days, p.top_conversation_partners
    FROM user_psychographics p
    JOIN users u ON u.id = p.user_id
    ORDER BY p.user_id, p.created_at DESC
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
