import { db } from '../db/index.js';
import fs from 'fs';
import path from 'path';

async function main() {
  const version = 'v0.6.0';
  console.log(`Exporting data for version ${version}...`);

  // 1) Claims
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

  // 2) Abstentions (deprecated)
  console.log('Fetching abstentions (skipped)...');
  const abstentionRes = { rows: [] };

  // 3) LLM enrichments (deprecated)
  console.log('Fetching AI enrichments (skipped)...');
  const enrichmentsRes = { rows: [] };

  // 4) Psychographics
  console.log('Fetching psychographics...');
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

  // 5) Activity heatmap from group messages only
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

  const userActivity: Record<number, number[][]> = {};
  for (const row of heatmapRes.rows) {
    if (!userActivity[row.user_id]) {
      userActivity[row.user_id] = Array.from({ length: 7 }, () => new Array(24).fill(0));
    }
    userActivity[row.user_id][row.dow][row.hour] += row.cnt;
  }

  // 6) DM Conversations
  console.log('Fetching DM conversations...');
  const dmConvosRes = await db.query(`
    SELECT
      dc.id,
      dc.account_user_id,
      dc.subject_user_id,
      ac.display_name AS account_display_name,
      su.display_name AS subject_display_name,
      su.handle AS subject_handle,
      su.bio AS subject_bio,
      dc.platform,
      dc.external_chat_id,
      dc.title,
      dc.status,
      dc.source,
      dc.priority,
      dc.next_followup_at,
      dc.last_activity_at,
      dc.created_at,
      dc.updated_at
    FROM dm_conversations dc
    JOIN users ac ON ac.id = dc.account_user_id
    JOIN users su ON su.id = dc.subject_user_id
    ORDER BY dc.last_activity_at DESC
  `);

  // 7) DM messages and simple derived aggregates
  console.log('Fetching DM messages...');
  const dmMessagesRes = await db.query(`
    SELECT
      dm.id,
      dm.conversation_id,
      dm.external_message_id,
      dm.direction,
      dm.message_text,
      dm.sent_at,
      dm.has_links,
      dm.has_mentions,
      dm.extracted_handles,
      dm.response_to_external_message_id,
      dm.text_hash
    FROM dm_messages dm
    ORDER BY dm.conversation_id, dm.sent_at ASC
  `);

  console.log('Fetching DM interpretations...');
  const dmInterpretationsRes = await db.query(`
    SELECT
      di.id,
      di.dm_message_id,
      di.kind,
      di.summary,
      di.sentiment_score,
      di.confidence,
      di.requires_followup,
      di.followup_reason,
      di.metadata,
      di.created_at
    FROM dm_interpretations di
    ORDER BY di.dm_message_id, di.created_at ASC
  `);

  const dmMessagesByConversation = new Map<number, any[]>();
  for (const row of dmMessagesRes.rows) {
    const k = Number(row.conversation_id);
    if (!dmMessagesByConversation.has(k)) dmMessagesByConversation.set(k, []);
    dmMessagesByConversation.get(k)!.push(row);
  }

  const dmInterpretationsByMessage = new Map<number, any[]>();
  for (const row of dmInterpretationsRes.rows) {
    const k = Number(row.dm_message_id);
    if (!dmInterpretationsByMessage.has(k)) dmInterpretationsByMessage.set(k, []);
    dmInterpretationsByMessage.get(k)!.push(row);
  }

  const dmConversations = dmConvosRes.rows.map((c) => {
    const convId = Number(c.id);
    const messages = dmMessagesByConversation.get(convId) || [];
    const lastMessage = messages.length > 0 ? messages[messages.length - 1] : null;
    const inboundCount = messages.filter(m => m.direction === 'inbound').length;
    const outboundCount = messages.filter(m => m.direction === 'outbound').length;
    const followupNeeded = messages
      .map(m => dmInterpretationsByMessage.get(Number(m.id)) || [])
      .flat()
      .some(i => i.requires_followup);

    return {
      ...c,
      messages,
      inbound_count: inboundCount,
      outbound_count: outboundCount,
      followup_needed: followupNeeded,
      last_message: lastMessage
    };
  });

  const dmConvoById = new Map<number, any>(dmConversations.map((c: any) => [Number(c.id), c]));
  const dmMessages = dmMessagesRes.rows.map((m) => {
    const conv = dmConvoById.get(Number(m.conversation_id));
    const interpretations = dmInterpretationsByMessage.get(Number(m.id)) || [];
    return {
      ...m,
      interpretations,
      conversation: conv
        ? {
            id: conv.id,
            status: conv.status,
            subject_display_name: conv.subject_display_name,
            subject_handle: conv.subject_handle,
            platform: conv.platform,
          }
        : null,
    };
  });

  // 8) Stats
  const stats = {
    total_claims: claimsRes.rows.length,
    supported_claims: claimsRes.rows.filter(r => r.status === 'supported').length,
    total_abstentions: abstentionRes.rows.length,
    enrichment_count: enrichmentsRes.rows.length,
    psycho_count: psychoRes.rows.length,
    dm_conversations: dmConvosRes.rows.length,
    dm_messages: dmMessagesRes.rows.length,
    generated_at: new Date().toISOString()
  };

  const data = {
    stats,
    claims: claimsRes.rows,
    abstentions: abstentionRes.rows,
    enrichments: enrichmentsRes.rows,
    psychographics: psychoRes.rows,
    activity: userActivity,
    dm: {
      conversations: dmConversations,
      messages: dmMessages,
      interpretation_count: dmInterpretationsRes.rows.length,
    }
  };

  // Write as JS for file:// compatibility
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
