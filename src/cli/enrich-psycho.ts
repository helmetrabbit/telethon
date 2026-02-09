/**
 * CLI: Run psychographic profiling for user communication style.
 *
 * Usage:
 *   npm run enrich-psycho
 *   npm run enrich-psycho -- --model nvidia/nemotron-3-nano-30b-a3b:free
 *   npm run enrich-psycho -- --limit 10 --skip-existing
 */

import { db } from '../db/index.js';
import { parseArgs } from '../utils.js';
import { createLLMClient, promptHash } from '../inference/llm-client.js';
import {
  buildPsychoPrompt,
  TONE_VALUES,
  PROFESSIONALISM_VALUES,
  VERBOSITY_VALUES,
  RESPONSIVENESS_VALUES,
  DECISION_STYLE_VALUES,
  SENIORITY_VALUES,
  COMMERCIAL_ARCHETYPE_VALUES,
  type PsychographicProfile,
  type EvidenceItem,
} from '../inference/psycho-prompt.js';
import type { UserBriefing } from '../inference/llm-prompt.js';

// ── Config ──────────────────────────────────────────────

// Force DeepSeek (Paid Model)
const DEFAULT_MODEL = 'deepseek/deepseek-chat';

// Use env key if present, otherwise fall back to project keys (which may have credit)
const API_KEYS = [
  process.env.OPENROUTER_API_KEY,
  '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',
].filter(Boolean) as string[];

const MIN_MESSAGES_FOR_PSYCHO = 5; // need enough messages to detect style
const MIN_CHARS_FOR_PSYCHO = 30; // OR enough volume of text (e.g. one long intro)
const CONCURRENCY = 25; // Process 25 users in parallel

// ── Main ────────────────────────────────────────────────

async function main(): Promise<void> {
  const args = parseArgs();
  const model = args['model'] || DEFAULT_MODEL;
  const limit = parseInt(args['limit'] || '0', 10);
  const skipExisting = args['skip-existing'] === 'true';

  console.log(`\n🧠 Psychographic Profiling — ${model}\n`);

  const llm = createLLMClient({
    apiKeys: API_KEYS,
    model,
    maxRetries: 3,
    retryDelayMs: 500,
    requestDelayMs: 100,
  });

  console.log(`   🔑 Keys Available: ${API_KEYS.length}`);

  // ── 1. Find users to process ──────────────────────
  // Prioritize "needs_enrichment=true", then fallback to those who have enough messages but no profile.
  let userQuery = `
    SELECT u.id, u.handle, u.display_name, u.bio, COUNT(m.id)::int AS msg_count
    FROM users u
    LEFT JOIN messages m ON m.user_id = u.id
    WHERE (
      (SELECT COUNT(*) FROM messages m2 WHERE m2.user_id = u.id) >= ${MIN_MESSAGES_FOR_PSYCHO}
      AND (SELECT COALESCE(SUM(LENGTH(m3.text)), 0) FROM messages m3 WHERE m3.user_id = u.id) >= ${MIN_CHARS_FOR_PSYCHO}
    )
    AND (
      u.needs_enrichment = true
      OR NOT EXISTS (SELECT 1 FROM user_psychographics up WHERE up.user_id = u.id)
    )
    GROUP BY u.id
    ORDER BY u.needs_enrichment DESC, COUNT(m.id) DESC
  `;
  if (limit > 0) userQuery += ` LIMIT ${limit}`;

  const { rows: users } = await db.query<{
    id: number; handle: string | null; display_name: string | null; bio: string | null; msg_count: number;
  }>(userQuery);

  console.log(`   Found ${users.length} users prioritized for enrichment.`);

  let toProcess = users;
  if (skipExisting) {
    // If skipping existing, we still want to process those marked 'needs_enrichment'
    // This logic might need refinement if 'needs_enrichment' isn't cleared.
    // We will clear 'needs_enrichment' after successful processing.
  }

  const concurrency = CONCURRENCY; 

  console.log(`   Concurrency: ${concurrency} threads`);

  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < toProcess.length; i += concurrency) {
    const chunk = toProcess.slice(i, i + concurrency);
    
    // Process chunk in parallel
    await Promise.all(chunk.map(async (user, idx) => {
      const globalIdx = i + idx;
      const pct = Math.round(((globalIdx + 1) / toProcess.length) * 100);
      
      try {
        const briefing = await loadUserBriefing(user.id, user.handle, user.display_name, user.bio);
        const prompt = buildPsychoPrompt(briefing);
        const hash = promptHash(prompt);

        const { rows: cached } = await db.query(
          'SELECT id FROM user_psychographics WHERE user_id = $1 AND model_name = $2 AND prompt_hash = $3',
          [user.id, model, hash],
        );

        if (cached.length > 0) {
          process.stdout.write('.'); // Dot progress for cache
          successCount++;
          return;
        }

        const response = await llm.complete(prompt);
        // console.log(`   ⏱ LLM responded in ${response.latencyMs}ms`);

        const parsed = extractPsychoJSON(response.content);
        const validated = validateProfile(parsed);

        // ── Computed fields (non-LLM) ──────────────────
        // 1. Inject Telegram social URL from handle
        if (user.handle) {
          const tgUrl = `t.me/${user.handle}`;
          if (!validated.social_urls) validated.social_urls = [];
          if (!validated.social_urls.some(u => u.includes('t.me/'))) {
            validated.social_urls.unshift(tgUrl);
          }
          if (!validated.social_platforms) validated.social_platforms = [];
          if (!validated.social_platforms.includes('Telegram')) {
            validated.social_platforms.push('Telegram');
          }
        }

        // 2. FIFO — first/last message dates from memberships
        const { rows: fifoRows } = await db.query<{ first_msg: string; last_msg: string }>(`
          SELECT MIN(first_seen_at) as first_msg, MAX(last_seen_at) as last_msg
          FROM memberships WHERE user_id = $1
        `, [user.id]);
        if (fifoRows[0]?.first_msg && fifoRows[0]?.last_msg) {
          const fmt = (d: string) => {
            const dt = new Date(d);
            return `${String(dt.getMonth() + 1).padStart(2, '0')}/${String(dt.getFullYear()).slice(-2)}`;
          };
          validated.fifo = `${fmt(fifoRows[0].first_msg)} - ${fmt(fifoRows[0].last_msg)}`;
        }

        // 3. Group tags — groups the user is a member of
        const { rows: groupRows } = await db.query<{ title: string }>(`
          SELECT g.title FROM memberships m JOIN groups g ON g.id = m.group_id
          WHERE m.user_id = $1 ORDER BY m.msg_count DESC
        `, [user.id]);
        validated.group_tags = groupRows.map(g => g.title).filter(Boolean);

        // 4. Reputation summary — computed from viral stats
        const { rows: repStats } = await db.query<{
          total_msgs: string; total_reactions: string; total_replies: string;
          avg_reactions: string; avg_replies: string;
        }>(`
          SELECT COUNT(*)::text as total_msgs,
                 COALESCE(SUM(reaction_count), 0)::text as total_reactions,
                 COALESCE(SUM(reply_count), 0)::text as total_replies,
                 ROUND(COALESCE(AVG(NULLIF(reaction_count, 0)), 0), 1)::text as avg_reactions,
                 ROUND(COALESCE(AVG(NULLIF(reply_count, 0)), 0), 1)::text as avg_replies
          FROM messages WHERE user_id = $1
        `, [user.id]);
        const rs = repStats[0];
        const totalReactions = parseInt(rs.total_reactions);
        const totalReplies = parseInt(rs.total_replies);
        const totalMsgs = parseInt(rs.total_msgs);
        if (totalMsgs > 0) {
          validated.total_reactions = totalReactions;
          validated.avg_reactions_per_msg = parseFloat(rs.avg_reactions);
          validated.total_replies_received = totalReplies;
          validated.avg_replies_per_msg = parseFloat(rs.avg_replies);
          validated.engagement_rate = parseFloat(((totalReactions + totalReplies) / totalMsgs * 100).toFixed(1));
        }

        // 5. Activity window — peak hours, active days, avg message size
        const { rows: actRows } = await db.query<{
          hour: string; msg_count: string;
        }>(`
          SELECT EXTRACT(HOUR FROM sent_at AT TIME ZONE 'UTC')::int as hour,
                 COUNT(*)::int as msg_count
          FROM messages WHERE user_id = $1 AND sent_at IS NOT NULL
          GROUP BY hour ORDER BY msg_count DESC
        `, [user.id]);

        const { rows: dayRows } = await db.query<{
          dow: string; day_name: string; msg_count: string;
        }>(`
          SELECT EXTRACT(DOW FROM sent_at AT TIME ZONE 'UTC')::int as dow,
                 TO_CHAR(sent_at AT TIME ZONE 'UTC', 'Dy') as day_name,
                 COUNT(*)::int as msg_count
          FROM messages WHERE user_id = $1 AND sent_at IS NOT NULL
          GROUP BY dow, day_name ORDER BY msg_count DESC
        `, [user.id]);

        const { rows: avgRow } = await db.query<{ avg_len: string }>(`
          SELECT ROUND(AVG(LENGTH(text)))::text as avg_len
          FROM messages WHERE user_id = $1 AND text IS NOT NULL
        `, [user.id]);

        validated.total_msgs = totalMsgs;
        validated.avg_msg_length = parseInt(avgRow[0]?.avg_len ?? '0');
        if (actRows.length > 0) {
          validated.peak_hours = actRows.slice(0, 3).map(r => parseInt(r.hour));
        }
        if (dayRows.length > 0) {
          validated.active_days = dayRows.slice(0, 3).map(r => r.day_name);
        }

        // 6. Last active days — days since most recent message
        const { rows: lastActiveRows } = await db.query<{ days: string }>(`
          SELECT EXTRACT(DAY FROM now() - MAX(sent_at))::int::text as days
          FROM messages WHERE user_id = $1
        `, [user.id]);
        if (lastActiveRows[0]?.days) {
          validated.last_active_days = parseInt(lastActiveRows[0].days);
        }

        // 7. Top conversation partners — bidirectional reply chains
        const { rows: partnerRows } = await db.query<{
          partner_id: string; handle: string; display_name: string;
          replies_sent: string; replies_received: string;
        }>(`
          WITH outbound AS (
            SELECT orig.user_id as partner_id, COUNT(*) as cnt
            FROM messages r
            JOIN messages orig ON r.reply_to_external_message_id = orig.external_message_id
                                AND r.group_id = orig.group_id
            WHERE r.user_id = $1 AND orig.user_id != $1
            GROUP BY orig.user_id
          ),
          inbound AS (
            SELECT r.user_id as partner_id, COUNT(*) as cnt
            FROM messages r
            JOIN messages orig ON r.reply_to_external_message_id = orig.external_message_id
                                AND r.group_id = orig.group_id
            WHERE orig.user_id = $1 AND r.user_id != $1
            GROUP BY r.user_id
          )
          SELECT COALESCE(o.partner_id, i.partner_id)::text as partner_id,
                 u.handle, u.display_name,
                 COALESCE(o.cnt, 0)::text as replies_sent,
                 COALESCE(i.cnt, 0)::text as replies_received
          FROM outbound o
          FULL OUTER JOIN inbound i ON o.partner_id = i.partner_id
          JOIN users u ON u.id = COALESCE(o.partner_id, i.partner_id)
          ORDER BY COALESCE(o.cnt,0) + COALESCE(i.cnt,0) DESC
          LIMIT 5
        `, [user.id]);
        if (partnerRows.length > 0) {
          validated.top_conversation_partners = partnerRows.map(r => ({
            handle: r.handle || r.display_name || `user_${r.partner_id}`,
            display_name: r.display_name || r.handle || `User ${r.partner_id}`,
            replies_sent: parseInt(r.replies_sent),
            replies_received: parseInt(r.replies_received),
          }));
        }

        // Persist
        await db.query(
          `INSERT INTO user_psychographics
             (user_id, model_name, prompt_hash, tone, professionalism, verbosity,
              responsiveness, decision_style, seniority_signal, commercial_archetype, approachability,
              quirks, notable_topics, pain_points, crypto_values, connection_requests, fingerprint_tags, based_in, attended_events, preferred_contact_style, 
              reasoning, raw_response, latency_ms,
              generated_bio_professional, generated_bio_personal, primary_role, primary_company, deep_skills, affiliations, social_platforms, social_urls, buying_power, languages, scam_risk_score, confidence_score, career_stage, tribe_affiliations, reputation_score, driving_values, technical_specifics, business_focus,
              fifo, group_tags, total_msgs, avg_msg_length, peak_hours, active_days, last_active_days,
              total_reactions, avg_reactions_per_msg, total_replies_received, avg_replies_per_msg, engagement_rate,
              top_conversation_partners
             )
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,
                   $24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54)
           ON CONFLICT (user_id, model_name, prompt_hash) DO UPDATE
           SET tone=EXCLUDED.tone, professionalism=EXCLUDED.professionalism,
               verbosity=EXCLUDED.verbosity, responsiveness=EXCLUDED.responsiveness,
               decision_style=EXCLUDED.decision_style, seniority_signal=EXCLUDED.seniority_signal,
               commercial_archetype=EXCLUDED.commercial_archetype,
               approachability=EXCLUDED.approachability, quirks=EXCLUDED.quirks,
               notable_topics=EXCLUDED.notable_topics, pain_points=EXCLUDED.pain_points, crypto_values=EXCLUDED.crypto_values,
               connection_requests=EXCLUDED.connection_requests, fingerprint_tags=EXCLUDED.fingerprint_tags,
               based_in=EXCLUDED.based_in,
               attended_events=EXCLUDED.attended_events, preferred_contact_style=EXCLUDED.preferred_contact_style,
               reasoning=EXCLUDED.reasoning, raw_response=EXCLUDED.raw_response,
               latency_ms=EXCLUDED.latency_ms, created_at=now(),
               generated_bio_professional=EXCLUDED.generated_bio_professional, generated_bio_personal=EXCLUDED.generated_bio_personal,
               primary_role=EXCLUDED.primary_role, primary_company=EXCLUDED.primary_company,
               deep_skills=EXCLUDED.deep_skills, affiliations=EXCLUDED.affiliations, social_platforms=EXCLUDED.social_platforms, social_urls=EXCLUDED.social_urls,
               buying_power=EXCLUDED.buying_power, languages=EXCLUDED.languages, scam_risk_score=EXCLUDED.scam_risk_score,
               confidence_score=EXCLUDED.confidence_score, career_stage=EXCLUDED.career_stage, tribe_affiliations=EXCLUDED.tribe_affiliations,
               reputation_score=EXCLUDED.reputation_score, driving_values=EXCLUDED.driving_values,
               technical_specifics=EXCLUDED.technical_specifics, business_focus=EXCLUDED.business_focus,
               fifo=EXCLUDED.fifo, group_tags=EXCLUDED.group_tags,
               total_msgs=EXCLUDED.total_msgs, avg_msg_length=EXCLUDED.avg_msg_length, peak_hours=EXCLUDED.peak_hours, active_days=EXCLUDED.active_days,
               last_active_days=EXCLUDED.last_active_days,
               total_reactions=EXCLUDED.total_reactions, avg_reactions_per_msg=EXCLUDED.avg_reactions_per_msg,
               total_replies_received=EXCLUDED.total_replies_received, avg_replies_per_msg=EXCLUDED.avg_replies_per_msg,
               engagement_rate=EXCLUDED.engagement_rate,
               top_conversation_partners=EXCLUDED.top_conversation_partners
           `,
          [
            user.id, model, hash,
            validated.tone, validated.professionalism, validated.verbosity,
            validated.responsiveness, validated.decision_style, validated.seniority_signal,
            validated.commercial_archetype, validated.approachability,
            JSON.stringify(validated.quirks ?? []),
            JSON.stringify(validated.notable_topics ?? []),
            JSON.stringify(validated.pain_points ?? []),
            JSON.stringify(validated.crypto_values ?? []),
            JSON.stringify(validated.connection_requests ?? []),
            validated.fingerprint_tags ?? [],
            validated.based_in,
            JSON.stringify(validated.attended_events ?? []),
            validated.preferred_contact_style, validated.reasoning,
            response.content, response.latencyMs,
            validated.generated_bio_professional, validated.generated_bio_personal,
            validated.primary_role, validated.primary_company,
            validated.deep_skills ?? [], validated.affiliations ?? [], validated.social_platforms ?? [], validated.social_urls ?? [],
            validated.buying_power, validated.languages ?? [], validated.scam_risk_score,
            validated.confidence_score, validated.career_stage, validated.tribe_affiliations ?? [],
            validated.reputation_score, validated.driving_values ?? [],
            validated.technical_specifics ?? [], validated.business_focus ?? [],
            validated.fifo ?? null, validated.group_tags ?? [],
            validated.total_msgs ?? null, validated.avg_msg_length ?? null, validated.peak_hours ?? [], validated.active_days ?? [],
            validated.last_active_days ?? null,
            validated.total_reactions ?? null, validated.avg_reactions_per_msg ?? null,
            validated.total_replies_received ?? null, validated.avg_replies_per_msg ?? null, validated.engagement_rate ?? null,
            JSON.stringify(validated.top_conversation_partners ?? [])
          ],
        );

        // Turn off dirty flag and update last enriched timestamp
        await db.query(`UPDATE users SET needs_enrichment = false, last_enriched_at = now() WHERE id = $1`, [user.id]);

        // Detailed Log (Restored)
        const label = user.display_name || user.handle || `User ${user.id}`;
        console.log(`\n  [${globalIdx + 1}/${toProcess.length}] ${label} (id=${user.id}, ${user.msg_count} msgs) - ${response.latencyMs}ms`);
        if (validated.generated_bio_professional) console.log(`   Bio (Prof): ${validated.generated_bio_professional}`);
        if (validated.generated_bio_personal)     console.log(`   Bio (Pers): ${validated.generated_bio_personal}`);
        
        if (validated.primary_role)    console.log(`   Role: ${validated.primary_role}`);
        if (validated.primary_company) console.log(`   Company: ${validated.primary_company}`);
        
        if (validated.commercial_archetype) console.log(`   Archetype: ${validated.commercial_archetype} (${validated.career_stage ?? 'unknown level'})`);
        console.log(`   Tone: ${validated.tone}`);
        console.log(`   Professionalism: ${validated.professionalism}`);
        console.log(`   Verbosity: ${validated.verbosity}`);
        console.log(`   Decision style: ${validated.decision_style}`);
        console.log(`   Seniority: ${validated.seniority_signal}`);
        console.log(`   Approachability: ${validated.approachability?.toFixed(2)}`);
        
        if (validated.scam_risk_score !== undefined) console.log(`   Scam risk: ${validated.scam_risk_score}/100`);
        if (validated.buying_power) console.log(`   Buying power: ${validated.buying_power}`);
        if (validated.fifo) console.log(`   FIFO: ${validated.fifo}`);
        if (validated.total_msgs) console.log(`   Total msgs: ${validated.total_msgs}`);
        if (validated.avg_msg_length) console.log(`   Avg msg length: ${validated.avg_msg_length}`);
        if (validated.total_reactions !== undefined) console.log(`   Total reactions: ${validated.total_reactions}`);
        if (validated.avg_reactions_per_msg !== undefined) console.log(`   Avg reactions/msg: ${validated.avg_reactions_per_msg}`);
        if (validated.total_replies_received !== undefined) console.log(`   Total replies received: ${validated.total_replies_received}`);
        if (validated.avg_replies_per_msg !== undefined) console.log(`   Avg replies/msg: ${validated.avg_replies_per_msg}`);
        if (validated.engagement_rate !== undefined) console.log(`   Engagement rate: ${validated.engagement_rate}%`);
        if (validated.peak_hours && validated.peak_hours.length > 0) console.log(`   Peak hours: ${validated.peak_hours.map(h => `${h.toString().padStart(2, '0')}:00`).join(', ')} UTC`);
        if (validated.active_days && validated.active_days.length > 0) console.log(`   Active days: ${validated.active_days.join(', ')}`);
        if (validated.last_active_days !== undefined && validated.last_active_days !== null) console.log(`   Days since active: ${validated.last_active_days}`);
        if (validated.group_tags && validated.group_tags.length > 0) console.log(`   Groups: ${validated.group_tags.join(', ')}`);
        if (validated.top_conversation_partners && validated.top_conversation_partners.length > 0) {
          const partners = validated.top_conversation_partners.map(p => `${p.display_name} (${p.replies_sent}/${p.replies_received})`).join(', ');
          console.log(`   Top partners: ${partners}`);
        }
        if (validated.based_in) console.log(`   Based in: ${validated.based_in}`);
        if (validated.languages && validated.languages.length > 0) console.log(`   Languages: ${validated.languages.join(', ')}`);
        if (validated.deep_skills && validated.deep_skills.length > 0) console.log(`   Skills: ${validated.deep_skills.join(', ')}`);
        if (validated.social_platforms && validated.social_platforms.length > 0) console.log(`   Platforms: ${validated.social_platforms.join(', ')}`);
        if (validated.social_urls && validated.social_urls.length > 0) console.log(`   Social URLs: ${validated.social_urls.join(', ')}`);
        if (validated.tribe_affiliations && validated.tribe_affiliations.length > 0) console.log(`   Tribes: ${validated.tribe_affiliations.join(', ')}`);
        if (validated.driving_values && validated.driving_values.length > 0) console.log(`   Driving Values: ${validated.driving_values.join(', ')}`);
        
        if (validated.technical_specifics && validated.technical_specifics.length > 0) console.log(`   Tech: ${validated.technical_specifics.join(', ')}`);
        if (validated.business_focus && validated.business_focus.length > 0) console.log(`   Biz Focus: ${validated.business_focus.join(', ')}`);
        
        if (validated.quirks && validated.quirks.length > 0) {
          // Use 'any' cast if TS complains during transition, or check type at runtime
          const qList = validated.quirks.map((q: any) => {
             const val = typeof q === 'string' ? `"${q}"` : `"${q.value}"`;
             const quote = q.quote ? ` [Ev: "${q.quote}"]` : '';
             const date = q.date ? ` (${q.date})` : '';
             return `${val}${quote}${date}`;
          });
          console.log(`   Quirks: ${qList.join(', ')}`);
        }
        
        if (validated.notable_topics && validated.notable_topics.length > 0) {
          const tList = validated.notable_topics.map((t: any) => {
             const val = typeof t === 'string' ? t : t.value;
             const quote = t.quote ? ` [Ev: "${t.quote}"]` : '';
             const date = t.date ? ` (${t.date})` : '';
             return `${val}${quote}${date}`;
          });
          console.log(`   Topics: ${tList.join(', ')}`);
        }

        if (validated.pain_points && validated.pain_points.length > 0) {
          const pList = validated.pain_points.map((p: any) => {
             const val = p.value;
             const quote = p.quote ? ` [Ev: "${p.quote}"]` : '';
             return `${val}${quote}`;
          });
          console.log(`   Pain points: ${pList.join(', ')}`);
        }

        if (validated.crypto_values && validated.crypto_values.length > 0) {
          const vList = validated.crypto_values.map((v: any) => {
             const val = v.value;
             const quote = v.quote ? ` [Ev: "${v.quote}"]` : '';
             return `${val}${quote}`;
          });
          console.log(`   Crypto values: ${vList.join(', ')}`);
        }
        
        if (validated.connection_requests && validated.connection_requests.length > 0) {
          const cList = validated.connection_requests.map((c: any) => {
             const val = c.value;
             const quote = c.quote ? ` [Ev: "${c.quote}"]` : '';
             return `${val}${quote}`;
          });
          console.log(`   Connection requests: ${cList.join(', ')}`);
        }

        if (validated.fingerprint_tags && validated.fingerprint_tags.length > 0) {
          console.log(`   Tags: ${validated.fingerprint_tags.join(', ')}`);
        }

        if (validated.preferred_contact_style) {
          console.log(`   Contact style: "${validated.preferred_contact_style}"`);
        }

        successCount++;
      } catch (err) {
        failCount++;
        console.error(`❌ [${globalIdx + 1}] User ${user.id} Failed: ${(err as Error).message}`);
      }
    }));
  }


  console.log('\n' + '━'.repeat(50));
  console.log(`✅ Psychographic profiling complete (${model}):`);
  console.log(`   Succeeded: ${successCount}`);
  console.log(`   Failed:    ${failCount}`);
  console.log(`   Skipped:   ${toProcess.length - successCount - failCount}`);

  await db.close();
}

// ── Briefing loader ─────────────────────────────────────

const MSG_SAMPLE_LIMIT = 2000;
const MIN_MSG_LENGTH = 5; // Only apply when user exceeds sample limit

async function loadUserBriefing(
  userId: number,
  handle: string | null,
  displayName: string | null,
  bio: string | null,
): Promise<UserBriefing> {
  // 1. Get groups this user is actually in, with importance weights
  const { rows: groups } = await db.query<{ group_id: number; title: string; kind: string; msg_count: number; importance_weight: number; group_description: string | null }>(`
    SELECT g.id as group_id, g.title, g.kind, m.msg_count, g.importance_weight, g.group_description
    FROM memberships m JOIN groups g ON g.id = m.group_id
    WHERE m.user_id = $1
  `, [userId]);

  // 2. Total message count
  const { rows: countRow } = await db.query<{ cnt: string }>(
    'SELECT COUNT(*)::text AS cnt FROM messages WHERE user_id = $1', [userId],
  );
  const totalMessages = parseInt(countRow[0]?.cnt ?? '0', 10);

  // 3. Smart message sampling — proportional per group, ranked by engagement
  type MsgRow = { id: number; sent_at: string; text: string; group_title: string; group_id: number; reaction_count: number; reply_count: number };
  let allMsgs: MsgRow[] = [];

  if (groups.length > 0) {
    const hasExcess = totalMessages > MSG_SAMPLE_LIMIT;
    const minLen = hasExcess ? MIN_MSG_LENGTH : 3;

    // Phase 1: Fetch ALL eligible messages per group (we need counts for redistribution)
    const groupMsgMap = new Map<number, MsgRow[]>();
    await Promise.all(groups.map(async (g) => {
      const { rows } = await db.query<MsgRow>(`
        SELECT m.id, m.sent_at, m.text, g.title as group_title, m.group_id,
               COALESCE(m.reaction_count, 0) as reaction_count,
               COALESCE(m.reply_count, 0) as reply_count
        FROM messages m
        JOIN groups g ON m.group_id = g.id
        WHERE m.user_id = $1 AND m.group_id = $2
          AND m.text IS NOT NULL AND LENGTH(m.text) > $3
        ORDER BY m.reaction_count DESC NULLS LAST,
                 m.reply_count DESC NULLS LAST,
                 m.sent_at DESC
      `, [userId, g.group_id, minLen]);
      groupMsgMap.set(g.group_id, rows);
    }));

    if (!hasExcess) {
      // Under the limit — just take everything, no proportional logic needed
      allMsgs = Array.from(groupMsgMap.values()).flat();
    } else {
      // Phase 2: Proportional allocation with overflow redistribution
      const totalWeight = groups.reduce((sum, g) => sum + g.importance_weight, 0);
      const allocations = groups.map(g => ({
        groupId: g.group_id,
        weight: g.importance_weight,
        idealSlots: Math.round((g.importance_weight / totalWeight) * MSG_SAMPLE_LIMIT),
        available: groupMsgMap.get(g.group_id)?.length ?? 0,
      }));

      // Redistribute: groups that can't fill their slots donate overflow to others
      let remaining = MSG_SAMPLE_LIMIT;
      const finalSlots = new Map<number, number>();

      // Pass 1: Assign to groups that have fewer msgs than their ideal allocation
      const underflow = allocations.filter(a => a.available <= a.idealSlots);
      const overflow = allocations.filter(a => a.available > a.idealSlots);

      for (const g of underflow) {
        const take = Math.min(g.available, remaining);
        finalSlots.set(g.groupId, take);
        remaining -= take;
      }

      // Pass 2: Distribute remaining budget proportionally among groups with surplus
      if (remaining > 0 && overflow.length > 0) {
        const overflowWeight = overflow.reduce((sum, g) => sum + g.weight, 0);
        for (const g of overflow) {
          const share = Math.round((g.weight / overflowWeight) * remaining);
          const take = Math.min(g.available, share);
          finalSlots.set(g.groupId, take);
        }
      }

      // Phase 3: Slice each group's messages to its final allocation
      for (const [groupId, slots] of finalSlots) {
        const msgs = groupMsgMap.get(groupId) ?? [];
        allMsgs.push(...msgs.slice(0, slots));
      }
    }
  }

  // 4. Final safety cap
  if (allMsgs.length > MSG_SAMPLE_LIMIT) {
    allMsgs.sort((a, b) => (b.reaction_count - a.reaction_count) || (b.reply_count - a.reply_count));
    allMsgs = allMsgs.slice(0, MSG_SAMPLE_LIMIT);
  }

  // 5. Aggregate viral stats
  const { rows: stats } = await db.query<{ total_views: string; avg_views: string; total_reactions: string; total_replies: string }>(`
    SELECT 
      COALESCE(SUM(views), 0) as total_views,
      COALESCE(AVG(views), 0) as avg_views,
      COALESCE(SUM(reaction_count), 0) as total_reactions,
      COALESCE(SUM(reply_count), 0) as total_replies
    FROM messages 
    WHERE user_id = $1
  `, [userId]);

  const viralStats = {
      totalViews: parseInt(stats[0].total_views),
      avgViews: parseFloat(stats[0].avg_views),
      totalReactions: parseInt(stats[0].total_reactions),
      repliesReceived: parseInt(stats[0].total_replies),
      totalReplies: 0
  };

  // 6. Sort chronological for the LLM (oldest → newest)
  allMsgs.sort((a, b) => new Date(a.sent_at).getTime() - new Date(b.sent_at).getTime());

  // 7. Load existing claims
  const { rows: claims } = await db.query<{
    predicate: string; object_value: string; confidence: number; status: string;
  }>(`
    SELECT predicate, object_value, confidence, status
    FROM claims WHERE subject_user_id = $1
    AND model_version NOT LIKE 'psycho:%'
    ORDER BY confidence DESC
  `, [userId]);

  return {
    userId, handle, displayName, bio,
    groups: groups.map((g) => ({ title: g.title || '(untitled)', kind: g.kind, msgCount: g.msg_count, description: g.group_description ?? undefined })),
    totalMessages,
    sampleMessages: allMsgs.map((m) => ({
      sent_at: new Date(m.sent_at).toISOString().slice(0, 10),
      text: m.text,
      groupTitle: m.group_title,
    })),
    existingClaims: claims.map((c) => ({
      predicate: c.predicate, value: c.object_value, confidence: c.confidence, status: c.status,
    })),
    viralStats,
  };
}

// ── JSON extraction ─────────────────────────────────────

function extractPsychoJSON(raw: string): PsychographicProfile {
  const clean = raw.replace(/<think>[\s\S]*?<\/think>/gi, '').trim();
  const fenceMatch = clean.match(/```(?:json)?\s*([\s\S]*?)\s*```/);
  const jsonStr = fenceMatch ? fenceMatch[1] : clean;
  const braceStart = jsonStr.indexOf('{');
  const braceEnd = jsonStr.lastIndexOf('}');

  if (braceStart === -1 || braceEnd === -1) {
    console.warn('   ⚠ No JSON object in psycho response.');
    return {};
  }

  try {
    return JSON.parse(jsonStr.slice(braceStart, braceEnd + 1));
  } catch (e) {
    console.warn(`   ⚠ Failed to parse psycho JSON: ${(e as Error).message}`);
    return {};
  }
}

// ── Validation ──────────────────────────────────────────

function validateProfile(p: PsychographicProfile): PsychographicProfile {
  const validSet = <T extends string>(val: string | undefined, allowed: readonly T[]): T | undefined =>
    val && (allowed as readonly string[]).includes(val) ? (val as T) : undefined;

  // Helper to validate EvidenceItem arrays
  const validEvidence = (arr: any[] | undefined): EvidenceItem[] => {
      if (!Array.isArray(arr)) return [];
      return arr.map(item => {
          if (typeof item === 'string') return { value: item, quote: '', date: '' }; // Backward compat
          if (typeof item === 'object' && item && item.value) {
              return { 
                value: item.value, 
                quote: item.quote || '',
                date: item.date || '' 
              };
          }
          return null;
      }).filter(Boolean) as EvidenceItem[];
  };

  return {
    tone: validSet(p.tone, TONE_VALUES),
    professionalism: validSet(p.professionalism, PROFESSIONALISM_VALUES),
    verbosity: validSet(p.verbosity, VERBOSITY_VALUES),
    responsiveness: validSet(p.responsiveness, RESPONSIVENESS_VALUES),
    decision_style: validSet(p.decision_style, DECISION_STYLE_VALUES),
    seniority_signal: validSet(p.seniority_signal, SENIORITY_VALUES),
    commercial_archetype: validSet(p.commercial_archetype, COMMERCIAL_ARCHETYPE_VALUES),
    approachability: typeof p.approachability === 'number'
      ? Math.min(1.0, Math.max(0.0, p.approachability))
      : undefined,
    quirks: validEvidence(p.quirks as any),
    notable_topics: validEvidence(p.notable_topics as any),
    pain_points: validEvidence(p.pain_points as any),
    crypto_values: validEvidence(p.crypto_values as any),
    connection_requests: validEvidence(p.connection_requests as any),
    fingerprint_tags: Array.isArray(p.fingerprint_tags) ? p.fingerprint_tags.filter(t => typeof t === 'string' && t.length > 0) : [],
    based_in: typeof p.based_in === 'string' ? p.based_in : undefined,
    attended_events: Array.isArray(p.attended_events) ? p.attended_events.filter((e) => typeof e === 'string' && e.length > 0) : [],
    preferred_contact_style: typeof p.preferred_contact_style === 'string' ? p.preferred_contact_style : undefined,
    reasoning: typeof p.reasoning === 'string' ? p.reasoning : undefined,

    // Omni-Profile Passthrough
    generated_bio_professional: typeof p.generated_bio_professional === 'string' ? p.generated_bio_professional : undefined,
    generated_bio_personal: typeof p.generated_bio_personal === 'string' ? p.generated_bio_personal : undefined,
    primary_role: typeof p.primary_role === 'string' ? p.primary_role : undefined,
    primary_company: typeof p.primary_company === 'string' ? p.primary_company : undefined,
    deep_skills: Array.isArray(p.deep_skills) ? p.deep_skills : [],
    affiliations: Array.isArray(p.affiliations) ? p.affiliations : [],
    social_platforms: Array.isArray(p.social_platforms) ? p.social_platforms : [],
    social_urls: Array.isArray(p.social_urls) ? p.social_urls : [],
    buying_power: typeof p.buying_power === 'string' ? p.buying_power : undefined,
    languages: Array.isArray(p.languages) ? p.languages : [],
    scam_risk_score: typeof p.scam_risk_score === 'number' ? p.scam_risk_score : undefined,
    confidence_score: typeof p.confidence_score === 'number' ? p.confidence_score : undefined,
    career_stage: typeof p.career_stage === 'string' ? p.career_stage : undefined,
    tribe_affiliations: Array.isArray(p.tribe_affiliations) ? p.tribe_affiliations : [],
    reputation_score: typeof p.reputation_score === 'number' ? p.reputation_score : undefined,
    driving_values: Array.isArray(p.driving_values) ? p.driving_values : [],
  };
}

main().catch((err) => {
  console.error('❌ enrich-psycho failed:', err);
  process.exit(1);
});
