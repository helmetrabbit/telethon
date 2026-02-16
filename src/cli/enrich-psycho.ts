/**
 * CLI: Run psychographic profiling for user communication style.
 *
 * Usage:
 *   npm run enrich-psycho
 *   npm run enrich-psycho -- --model nvidia/nemotron-3-nano-30b-a3b:free
 *   npm run enrich-psycho -- --limit 10 --skip-existing
 *   npm run enrich-psycho -- --user-ids 954,2011
 *   npm run enrich-psycho -- --dry-run true --user-ids 954,2011
 *   npm run enrich-psycho -- --dry-run true --dry-run-verbose true --user-ids 954,2011
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
import {
  buildUserEvidenceBundle,
  type EvidenceBundle,
  type EvidenceItem as BundleEvidenceItem,
} from '../analysis/evidence-bundle.js';

// ── Config ──────────────────────────────────────────────

// Force DeepSeek (Paid Model)
const DEFAULT_MODEL = 'deepseek/deepseek-chat';

const MIN_MESSAGES_FOR_PSYCHO = 5; // need enough messages to detect style
const MIN_CHARS_FOR_PSYCHO = 30; // OR enough volume of text (e.g. one long intro)
const CONCURRENCY = 25; // Process 25 users in parallel
const CLASSIFIER_VERSION = 'v2';
const RICH_PROFILE_MIN_MESSAGES = 40;
const RICH_PROFILE_MIN_PACK_ITEMS = 12;
const RICH_QUALITY_RETRY_LIMIT = 2;
const RICH_FAIL_CLOSED_MIN_MESSAGES = 2000;
const GENERIC_BIO_PHRASES = [
  'blockchain enthusiast',
  'community builder',
  'always on the lookout',
  'passionate about',
  'next big thing',
  'driving engagement',
  'fostering connections',
  'crypto enthusiast',
  'event enthusiast',
  'web3 enthusiast',
  'always ready',
  'helping projects',
];
const BUNDLE_PAIN_CUE_RE = /\b(issue|problem|bug|broken|hard|difficult|frustrat|annoy|wtf|fail(?:ed|ing|ure)?|not sure|can't|cant|stuck)\b/i;
const BUNDLE_QUIRK_CUE_RE = /\b(owo|uwu|gm|ser|ngmi|wagmi|lol|lmao|bruh)\b/i;
const BUNDLE_SEEK_CUE_RE = /\b(looking for|need(?:ing)?|want(?:ing)?|seeking|dm me|reach out|hmu|hire|hiring|help(?:ing)?)\b/i;
const SELF_SEEK_RE = /\b(?:i(?:'m| am)?|we(?:'re| are)?|our team|my team)\s+(?:looking for|seeking|need(?:ing)?|hiring|hire)\b|\b(?:dm me|reach out to me|contact me)\b|\b(?:we|our|my)\s+(?:would|d)\s+appreciate\s+(?:support|help)\b/i;
const FORWARDED_SEEK_RE = /\b(frens?\s+from|friends?\s+from|they\s+are\s+looking|is\s+looking\s+for|are\s+looking\s+for|someone\s+is\s+looking|them\s+looking\s+for|i\s+have\s+seen\s+them\s+looking)\b/i;
const SELF_SUPPORT_RE = /\b(we|our|i|my)\b.*\b(support|like|rt|repost|follow|share)\b|\b(support|like|rt|repost|follow|share)\b.*\b(we|our|i|my)\b/i;
const DIRECT_ASK_RE = /\b(we'?d\s+be\s+grateful|would\s+appreciate|please|dm me|reach out|contact me|looking for|hiring|hire|join us|follow us|like|repost|rt|share)\b/i;
const ACTIONABLE_ASK_RE = /\b(would\s+appreciate|we'?d\s+be\s+grateful|please|you\s+could|dm me|reach out(?: to me)?|contact me|we(?:'re| are)?\s+looking\s+for|i(?:'m| am)\s+looking\s+for|hiring|hire)\b/i;
const HISTORICAL_ASK_RE = /\b(i recall|previous workplace|at one of my previous|we were looking|used to look|someone was looking)\b/i;
const GENERIC_DRIVING_VALUE_RE = /^(growth|execution|professionalism|innovation|community|transparency|collaboration|partnerships?|integrity|trust)$/i;
const TECH_SPECIFIC_STOPWORDS = new Set([
  'x',
  'twitter',
  'x (twitter)',
  'linkedin',
  'telegram',
  'web3',
  'blockchain',
  'startups',
  'startup',
  'bd',
  'business development',
  'fundraising',
  'events',
]);

// ── Main ────────────────────────────────────────────────

async function main(): Promise<void> {
  const args = parseArgs();
  const model = args['model'] || DEFAULT_MODEL;
  const limit = parseInt(args['limit'] || '0', 10);
  const skipExisting = args['skip-existing'] === 'true';
  const strictInsights = args['strict-insights'] === 'true';
  const failClosedRich = args['fail-closed-rich'] !== 'false';
  const dryRun = args['dry-run'] === 'true';
  const dryRunVerbose = args['dry-run-verbose'] === 'true';
  const targetUserIds = parseUserIds(args['user-ids']);
  const apiKey = process.env.OPENROUTER_API_KEY;

  if (!dryRun && !apiKey) {
    console.error('❌ Missing OPENROUTER_API_KEY. Set it in your environment before running enrich-psycho.');
    process.exit(1);
  }

  console.log(`\n🧠 Psychographic Profiling — ${model}${dryRun ? ' (dry-run)' : ''}\n`);

  const llm = dryRun ? null : createLLMClient({
    apiKeys: [apiKey!],
    model,
    maxRetries: 3,
    retryDelayMs: 500,
    requestDelayMs: 100,
  });

  if (dryRun) {
    console.log('   🧪 Dry-run enabled: no LLM calls, no user_psychographics writes.');
  } else {
    console.log('   🔑 Keys Available: 1');
  }
  if (targetUserIds.length > 0) {
    console.log(`   🎯 Target users: ${targetUserIds.join(', ')}`);
  }

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
    AND ${
      targetUserIds.length > 0
        ? `u.id = ANY($1)`
        : `(u.needs_enrichment = true
           OR NOT EXISTS (SELECT 1 FROM user_psychographics up WHERE up.user_id = u.id))`
    }
    GROUP BY u.id
    ORDER BY u.needs_enrichment DESC, COUNT(m.id) DESC
  `;
  const userQueryParams: unknown[] = targetUserIds.length > 0 ? [targetUserIds] : [];
  if (limit > 0) {
    userQueryParams.push(limit);
    userQuery += ` LIMIT $${userQueryParams.length}`;
  }

  const { rows: users } = await db.query<{
    id: number; handle: string | null; display_name: string | null; bio: string | null; msg_count: number;
  }>(userQuery, userQueryParams);

  console.log(`   Found ${users.length} users prioritized for enrichment.`);
  if (targetUserIds.length > 0) {
    const found = new Set(users.map((u) => Number(u.id)));
    const missingTargets = targetUserIds.filter((id) => !found.has(id));
    if (missingTargets.length > 0) {
      console.warn(`   ⚠ Target users not selected (missing or below message thresholds): ${missingTargets.join(', ')}`);
    }
  }

  // ── Preflight: message_insights coverage ──────────────────
  if (users.length > 0) {
    const userIds = users.map((u) => u.id);
    const { rows: coverage } = await db.query<{ cnt: string }>(
      `SELECT COUNT(DISTINCT user_id)::text AS cnt
       FROM message_insights
       WHERE user_id = ANY($1)
         AND classifier_version = $2`,
      [userIds, CLASSIFIER_VERSION],
    );
    const covered = parseInt(coverage[0]?.cnt ?? '0', 10);
    const missing = users.length - covered;
    if (missing > 0) {
      const msg = `Missing message_insights for ${missing}/${users.length} users. Run: npm run compute-message-insights`;
      if (strictInsights) {
        throw new Error(msg);
      } else {
        console.warn(`   ⚠ ${msg}`);
      }
    }
  }

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
        // Enforce message_insights coverage (fail fast)
        const { rows: insightCountRows } = await db.query<{ cnt: string }>(
          'SELECT COUNT(*)::text AS cnt FROM message_insights WHERE user_id = $1 AND classifier_version = $2',
          [user.id, CLASSIFIER_VERSION],
        );
        if (parseInt(insightCountRows[0]?.cnt ?? '0', 10) === 0) {
          if (!dryRun) {
            await recordAbstention(user.id, model, 'missing_message_insights', {
              pack_counts: {},
              thresholds: {},
              evidence_summary_json: {},
              classifier_version: CLASSIFIER_VERSION,
            });
            await db.query(`UPDATE users SET needs_enrichment = false, last_enriched_at = now() WHERE id = $1`, [user.id]);
          }
          console.warn(`   ⚠ No message_insights for user ${user.id}. Skipping.`);
          return;
        }

        const bundle = await buildUserEvidenceBundle(user.id);
        const prompt = buildPsychoPrompt(bundle);
        const hash = promptHash(prompt);

        if (!dryRun) {
          const { rows: cached } = await db.query<{ id: number }>(
            'SELECT id FROM user_psychographics WHERE user_id = $1 AND model_name = $2 AND prompt_hash = $3',
            [user.id, model, hash],
          );

          if (cached.length > 0) {
            // Cache hit: still apply deterministic timeline patch so old rows self-heal
            const { rows: latestRows } = await db.query<{
              id: number;
              role_company_timeline: unknown;
              conflict_notes: unknown;
              primary_company: string | null;
              primary_role: string | null;
              tone: string | null;
              professionalism: string | null;
              preferred_contact_style: string | null;
              notable_topics: unknown;
              pain_points: unknown;
              affiliations: string[] | null;
              attended_events: unknown;
              connection_requests: unknown;
              evidence_summary_json: unknown;
            }>(
              `SELECT id, role_company_timeline, conflict_notes, primary_company, primary_role,
                      tone, professionalism, preferred_contact_style,
                      notable_topics, pain_points, affiliations, attended_events, connection_requests,
                      evidence_summary_json
               FROM user_psychographics
               WHERE user_id = $1
               ORDER BY created_at DESC
               LIMIT 1`,
              [user.id],
            );

            const latest = latestRows[0];
            if (latest) {
              const existing = validateProfile({
                primary_role: latest.primary_role ?? undefined,
                primary_company: latest.primary_company ?? undefined,
                tone: latest.tone ?? undefined,
                professionalism: latest.professionalism ?? undefined,
                preferred_contact_style: latest.preferred_contact_style ?? undefined,
                notable_topics: latest.notable_topics as any,
                pain_points: latest.pain_points as any,
                affiliations: latest.affiliations ?? [],
                attended_events: latest.attended_events as any,
                connection_requests: latest.connection_requests as any,
                role_company_timeline: latest.role_company_timeline as any,
              });
              const { profile: patchedCachedProfile, timelinePatch: cachedTimelinePatch } =
                applyBioSnapshotTimelinePatch(existing, bundle);

              const existingConflictNotes = normalizeConflictNotes(latest.conflict_notes);
              const nextConflictNotes = {
                role_company_conflict: bundle.current_claims.conflicts,
                bio_source: bundle.user.bio_source,
                bio_updated_at: bundle.user.bio_updated_at,
                ...(existingConflictNotes.timeline_patch ? { timeline_patch: existingConflictNotes.timeline_patch } : {}),
                ...(cachedTimelinePatch ? { timeline_patch: cachedTimelinePatch } : {}),
              };

              const nextTimeline = patchedCachedProfile.role_company_timeline ?? [];
              const nextPrimaryCompany = patchedCachedProfile.primary_company ?? null;
              const nextConnectionRequests = patchedCachedProfile.connection_requests ?? [];
              const nextEvidenceSummary = {
                ...bundle.evidence_summary,
                agent_playbook: buildAgentPlaybook(patchedCachedProfile, bundle),
              };

              const timelineChanged = !jsonEqual(latest.role_company_timeline, nextTimeline);
              const conflictChanged = !jsonEqual(existingConflictNotes, nextConflictNotes);
              const primaryChanged = (latest.primary_company ?? null) !== nextPrimaryCompany;
              const connectionChanged = !jsonEqual(latest.connection_requests, nextConnectionRequests);
              const evidenceSummaryChanged = !jsonEqual(latest.evidence_summary_json, nextEvidenceSummary);

              if (timelineChanged || conflictChanged || primaryChanged || connectionChanged || evidenceSummaryChanged) {
                await db.query(
                  `UPDATE user_psychographics
                   SET role_company_timeline = $1,
                       conflict_notes = $2,
                       primary_company = $3,
                       connection_requests = $4,
                       evidence_summary_json = $5
                   WHERE id = $6`,
                  [
                    JSON.stringify(nextTimeline),
                    JSON.stringify(nextConflictNotes),
                    nextPrimaryCompany,
                    JSON.stringify(nextConnectionRequests),
                    JSON.stringify(nextEvidenceSummary),
                    latest.id,
                  ],
                );
              }
            }

            await db.query(`UPDATE users SET needs_enrichment = false, last_enriched_at = now() WHERE id = $1`, [user.id]);
            process.stdout.write('.'); // Dot progress for cache
            successCount++;
            return;
          }
        }

        // Invariants: evidence packs must meet minimums
        const minRolePack = (bundle.user.bio && bundle.user.bio_updated_at) ? 1 : 3;
        const minTopicsPack = bundle.stats.total_messages > 200 ? 8 : 3;
        const minValuesPack = bundle.stats.total_messages > 200 ? 2 : 1;
        const minLinksPack = 1;

        const roleOk = bundle.packs.role_company.length >= minRolePack;
        const topicsOk = bundle.packs.topics.length >= minTopicsPack;
        const valuesOk = bundle.packs.values_seeking.length >= minValuesPack;
        const linksOk = bundle.packs.links.length >= minLinksPack || bundle.packs.links.length === 0;

        if (!roleOk || !topicsOk || !valuesOk || !linksOk) {
          const packCounts = {
            role_company: bundle.packs.role_company.length,
            topics: bundle.packs.topics.length,
            values_seeking: bundle.packs.values_seeking.length,
            links: bundle.packs.links.length,
          };
          const thresholds = {
            role_company: minRolePack,
            topics: minTopicsPack,
            values_seeking: minValuesPack,
            links: minLinksPack,
          };

          const reason =
            !roleOk ? 'insufficient_role_pack' :
            !topicsOk ? 'insufficient_topics_pack' :
            !valuesOk ? 'insufficient_values_pack' :
            'insufficient_links_pack';

          if (!dryRun) {
            await recordAbstention(user.id, model, reason, {
              pack_counts: packCounts,
              thresholds,
              evidence_summary_json: bundle.evidence_summary,
              classifier_version: CLASSIFIER_VERSION,
            });
            await db.query(`UPDATE users SET needs_enrichment = false, last_enriched_at = now() WHERE id = $1`, [user.id]);
          }
          console.warn(`   ⚠ Insufficient evidence packs for user ${user.id}. Skipping.`);
          return;
        }

        // Recency share check for role/company pack
        if (bundle.evidence_summary.role_company.recent_share_12m < 0.5 && bundle.packs.role_company.length > 0) {
          console.warn(`   ⚠ Low recency in role/company pack for user ${user.id}: ${bundle.evidence_summary.role_company.recent_share_12m}`);
        }

        if (dryRun) {
          const packCounts = {
            role_company: bundle.packs.role_company.length,
            topics: bundle.packs.topics.length,
            values_seeking: bundle.packs.values_seeking.length,
            links: bundle.packs.links.length,
            events_affiliations: bundle.packs.events_affiliations.length,
          };
          const thresholds = {
            role_company: minRolePack,
            topics: minTopicsPack,
            values_seeking: minValuesPack,
            links: minLinksPack,
          };
          const conflictNotes = {
            role_company_conflict: bundle.current_claims.conflicts,
            bio_source: bundle.user.bio_source,
            bio_updated_at: bundle.user.bio_updated_at,
          };

          const label = user.display_name || user.handle || `User ${user.id}`;
          console.log(`\n  [${globalIdx + 1}/${toProcess.length}] ${label} (id=${user.id}, ${user.msg_count} msgs) [dry-run]`);
          console.log(`   Pack counts: ${JSON.stringify(packCounts)}`);
          console.log(`   Thresholds: ${JSON.stringify(thresholds)}`);
          console.log(`   evidence_summary_json: ${JSON.stringify(bundle.evidence_summary)}`);
          console.log(`   conflict_notes: ${JSON.stringify(conflictNotes)}`);
          if (dryRunVerbose) {
            logDryRunEvidence(bundle.packs);
          }
          successCount++;
          return;
        }

        if (!llm) {
          throw new Error('LLM client not initialized');
        }

        let response = await llm.complete(prompt);
        // console.log(`   ⏱ LLM responded in ${response.latencyMs}ms`);

        let { profile: validated, timelinePatch } = finalizeGeneratedProfile(
          extractPsychoJSON(response.content),
          bundle,
        );
        let bestQuality = assessRichProfileQuality(validated, bundle);
        const maxQualityRetries = bestQuality.isRich ? RICH_QUALITY_RETRY_LIMIT : 1;
        for (let attempt = 1; bestQuality.shouldRetry && attempt <= maxQualityRetries; attempt++) {
          const retryPrompt = buildQualityRetryPrompt(prompt, bestQuality.failures, attempt, maxQualityRetries);
          const retryResponse = await llm.complete(retryPrompt);
          const retryResult = finalizeGeneratedProfile(
            extractPsychoJSON(retryResponse.content),
            bundle,
          );
          const retryQuality = assessRichProfileQuality(retryResult.profile, bundle);

          if (retryQuality.score >= bestQuality.score) {
            response = retryResponse;
            validated = retryResult.profile;
            timelinePatch = retryResult.timelinePatch;
            bestQuality = retryQuality;
          }
        }
        const hardFailures = bestQuality.failures.filter(isHardRichFailure);
        if (
          bestQuality.isRich
          && failClosedRich
          && bundle.stats.total_messages >= RICH_FAIL_CLOSED_MIN_MESSAGES
          && hardFailures.length > 0
        ) {
          await recordAbstention(user.id, model, 'rich_quality_gate_failed', {
            hard_failures: hardFailures,
            failures: bestQuality.failures,
            quality_score: bestQuality.score,
            evidence_summary_json: bundle.evidence_summary,
            classifier_version: CLASSIFIER_VERSION,
            total_messages: bundle.stats.total_messages,
          });
          await db.query(`UPDATE users SET needs_enrichment = false, last_enriched_at = now() WHERE id = $1`, [user.id]);
          console.warn(`   ⚠ Rich-profile hard quality gate failed for user ${user.id}: ${hardFailures.join(' | ')}`);
          return;
        }
        if (bestQuality.isRich && bestQuality.failures.length > 0) {
          console.warn(`   ⚠ Rich-profile quality still imperfect for user ${user.id}: ${bestQuality.failures.join(' | ')}`);
        }

        // Hydrate deterministic stats from evidence bundle
        validated.total_msgs = bundle.stats.total_messages;
        validated.avg_msg_length = bundle.stats.avg_msg_length;
        validated.peak_hours = bundle.stats.peak_hours;
        validated.active_days = bundle.stats.active_days;
        validated.last_active_days = bundle.stats.last_active_days ?? undefined;
        validated.total_reactions = bundle.stats.total_reactions;
        validated.total_replies_received = bundle.stats.total_replies_received;
        validated.engagement_rate = bundle.stats.engagement_rate;
        validated.top_conversation_partners = bundle.stats.top_conversation_partners;

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

        const conflictNotes = {
          role_company_conflict: bundle.current_claims.conflicts,
          bio_source: bundle.user.bio_source,
          bio_updated_at: bundle.user.bio_updated_at,
          ...(timelinePatch ? { timeline_patch: timelinePatch } : {}),
        };
        const evidenceSummaryForWrite = {
          ...bundle.evidence_summary,
          agent_playbook: buildAgentPlaybook(validated, bundle),
        };

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
              top_conversation_partners, role_company_timeline, conflict_notes, evidence_summary_json
             )
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,
                   $24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57)
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
               top_conversation_partners=EXCLUDED.top_conversation_partners,
               role_company_timeline=EXCLUDED.role_company_timeline,
               conflict_notes=EXCLUDED.conflict_notes,
               evidence_summary_json=EXCLUDED.evidence_summary_json
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
            JSON.stringify(validated.top_conversation_partners ?? []),
            JSON.stringify(validated.role_company_timeline ?? []),
            JSON.stringify(conflictNotes),
            JSON.stringify(evidenceSummaryForWrite)
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

async function recordAbstention(
  userId: number,
  model: string,
  reasonCode: string,
  details: Record<string, unknown>,
): Promise<void> {
  const modelVersion = `psycho:${model}`;
  await db.query(
    `INSERT INTO abstention_log (subject_user_id, predicate, reason_code, details, model_version)
     VALUES ($1, $2, $3, $4, $5)`,
    [userId, 'has_role', reasonCode, JSON.stringify(details), modelVersion],
  );
}

type TimelinePatchNote = {
  applied: true;
  added_org: string;
  promoted_to_current: boolean;
  bio_source: string | null;
  bio_updated_at: string | null;
  reason: 'bio_org_missing_from_timeline';
};

function applyBioSnapshotTimelinePatch(
  validated: PsychographicProfile,
  bundle: EvidenceBundle,
): { profile: PsychographicProfile; timelinePatch: TimelinePatchNote | null } {
  const hasConflict = bundle.current_claims.conflicts.some((conflict) => (
    conflict.type === 'role_company'
    || (conflict.bio_orgs.length > 0 && conflict.message_orgs.length > 0)
  ));
  if (!hasConflict) {
    return { profile: validated, timelinePatch: null };
  }

  const bioOrg = pickBioOrgCandidate(bundle);
  if (!bioOrg) {
    return { profile: validated, timelinePatch: null };
  }

  const timeline = Array.isArray(validated.role_company_timeline)
    ? validated.role_company_timeline.map((entry) => ({
      org: entry.org ?? null,
      role: entry.role ?? null,
      start_hint: entry.start_hint ?? null,
      end_hint: entry.end_hint ?? null,
      is_current: Boolean(entry.is_current),
      confidence: typeof entry.confidence === 'number' ? entry.confidence : 0,
      evidence_message_ids: Array.isArray(entry.evidence_message_ids)
        ? entry.evidence_message_ids.filter((id) => Number.isFinite(id))
        : [],
    }))
    : [];

  const bioOrgNorm = normalizeOrg(bioOrg);
  const hasBioOrgInTimeline = timeline.some((entry) => normalizeOrg(entry.org) === bioOrgNorm);
  if (hasBioOrgInTimeline) {
    return { profile: validated, timelinePatch: null };
  }

  const currentTimelineOrg = timeline.find((entry) => entry.is_current)?.org ?? null;
  const messageMostRecent = getOrgMostRecent(bundle, currentTimelineOrg);
  const promoteToCurrent = isBioNewerThanMessageEvidence(bundle.user.bio_updated_at, messageMostRecent);

  const injectedEntry = {
    org: bioOrg,
    role: null,
    start_hint: null,
    end_hint: null,
    is_current: false,
    confidence: 0.6,
    evidence_message_ids: [] as number[],
  };
  timeline.push(injectedEntry);

  if (promoteToCurrent) {
    for (const entry of timeline) {
      entry.is_current = false;
    }
    injectedEntry.is_current = true;
    validated.primary_company = bioOrg;
  }

  validated.role_company_timeline = timeline;

  return {
    profile: validated,
    timelinePatch: {
      applied: true,
      added_org: bioOrg,
      promoted_to_current: promoteToCurrent,
      bio_source: bundle.user.bio_source,
      bio_updated_at: bundle.user.bio_updated_at,
      reason: 'bio_org_missing_from_timeline',
    },
  };
}

function pickBioOrgCandidate(bundle: EvidenceBundle): string | null {
  const candidates = bundle.evidence_summary.bio_org_candidates
    .map((org) => org.trim())
    .filter((org) => org.length > 0);
  if (candidates.length === 0) return null;

  const candidateSet = new Set(candidates.map(normalizeOrg));
  const scored = bundle.evidence_summary.role_company_org_scores
    .filter((score) => score.bio_hits > 0 && candidateSet.has(normalizeOrg(score.org)))
    .sort((a, b) => b.bio_hits - a.bio_hits);

  if (scored.length > 0) {
    return scored[0].org;
  }
  return candidates[0];
}

function getOrgMostRecent(bundle: EvidenceBundle, org: string | null | undefined): string | null {
  if (!org) return null;
  const orgNorm = normalizeOrg(org);
  const found = bundle.evidence_summary.role_company_org_scores.find(
    (score) => normalizeOrg(score.org) === orgNorm,
  );
  return found?.most_recent ?? null;
}

function isBioNewerThanMessageEvidence(
  bioUpdatedAtRaw: string | null | undefined,
  messageMostRecentRaw: string | null | undefined,
): boolean {
  const bioUpdatedAt = parseDateOrNull(bioUpdatedAtRaw);
  const messageMostRecent = parseDateOrNull(messageMostRecentRaw);
  if (!bioUpdatedAt || !messageMostRecent) {
    return false;
  }
  const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
  return (bioUpdatedAt.getTime() - messageMostRecent.getTime()) >= sevenDaysMs;
}

function parseDateOrNull(raw: string | null | undefined): Date | null {
  if (!raw) return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function normalizeOrg(raw: string | null | undefined): string {
  if (!raw) return '';
  return raw
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function normalizeConflictNotes(raw: unknown): Record<string, unknown> {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return {};
  }
  return raw as Record<string, unknown>;
}

type ProfileQualityAssessment = {
  isRich: boolean;
  score: number;
  failures: string[];
  shouldRetry: boolean;
};

function finalizeGeneratedProfile(
  parsed: PsychographicProfile,
  bundle: EvidenceBundle,
): { profile: PsychographicProfile; timelinePatch: TimelinePatchNote | null } {
  let validated = validateProfile(parsed);
  const patched = applyBioSnapshotTimelinePatch(validated, bundle);
  validated = patched.profile;

  // Prefer current_role_company for primary role/company if missing.
  if (!validated.primary_role && validated.current_role_company?.role) {
    validated.primary_role = validated.current_role_company.role;
  }
  if (!validated.primary_company && validated.current_role_company?.org) {
    validated.primary_company = validated.current_role_company.org;
  }
  if ((!validated.role_company_timeline || validated.role_company_timeline.length === 0) && validated.current_role_company) {
    validated.role_company_timeline = [{
      org: validated.current_role_company.org ?? null,
      role: validated.current_role_company.role ?? null,
      start_hint: null,
      end_hint: null,
      is_current: true,
      evidence_message_ids: validated.current_role_company.evidence_message_ids ?? [],
      confidence: validated.current_role_company.confidence ?? 0,
    }];
  }

  return { profile: validated, timelinePatch: patched.timelinePatch };
}

function isRichEvidenceBundle(bundle: EvidenceBundle): boolean {
  const packCount =
    bundle.packs.role_company.length
    + bundle.packs.topics.length
    + bundle.packs.values_seeking.length
    + bundle.packs.events_affiliations.length;
  return bundle.stats.total_messages >= RICH_PROFILE_MIN_MESSAGES && packCount >= RICH_PROFILE_MIN_PACK_ITEMS;
}

type RichTargets = {
  minSupportedEvidenceItems: number;
  minTopics: number;
  minQuirks: number;
  minPainPoints: number;
  minDrivingValues: number;
  minConnectionRequests: number;
  minDeepSkills: number;
  minTechnicalSpecifics: number;
  minAffiliations: number;
  minFingerprintTags: number;
};

function getRichTargetsForBundle(bundle: EvidenceBundle): RichTargets {
  const n = bundle.stats.total_messages;
  if (n >= 10000) {
    return {
      minSupportedEvidenceItems: 6,
      minTopics: 6,
      minQuirks: 5,
      minPainPoints: 2,
      minDrivingValues: 4,
      minConnectionRequests: 2,
      minDeepSkills: 6,
      minTechnicalSpecifics: 10,
      minAffiliations: 8,
      minFingerprintTags: 12,
    };
  }
  if (n >= 5000) {
    return {
      minSupportedEvidenceItems: 7,
      minTopics: 5,
      minQuirks: 3,
      minPainPoints: 1,
      minDrivingValues: 3,
      minConnectionRequests: 1,
      minDeepSkills: 4,
      minTechnicalSpecifics: 8,
      minAffiliations: 6,
      minFingerprintTags: 10,
    };
  }
  if (n >= 2000) {
    return {
      minSupportedEvidenceItems: 5,
      minTopics: 4,
      minQuirks: 2,
      minPainPoints: 1,
      minDrivingValues: 2,
      minConnectionRequests: 1,
      minDeepSkills: 3,
      minTechnicalSpecifics: 6,
      minAffiliations: 4,
      minFingerprintTags: 8,
    };
  }
  return {
    minSupportedEvidenceItems: 3,
    minTopics: 2,
    minQuirks: 2,
    minPainPoints: 1,
    minDrivingValues: 2,
    minConnectionRequests: 1,
    minDeepSkills: 2,
    minTechnicalSpecifics: 4,
    minAffiliations: 3,
    minFingerprintTags: 6,
  };
}

function hasGenericBioPhrase(text: string | undefined): boolean {
  if (!text) return false;
  const normalized = text.toLowerCase().replace(/\s+/g, ' ').trim();
  return GENERIC_BIO_PHRASES.some((phrase) => normalized.includes(phrase));
}

function collectBioAnchorPhrases(bundle: EvidenceBundle): string[] {
  const phrases: string[] = [];
  for (const score of bundle.evidence_summary.role_company_org_scores.slice(0, 8)) {
    if (typeof score.org === 'string' && score.org.trim().length >= 3) {
      phrases.push(score.org.trim());
    }
  }
  for (const group of bundle.stats.top_groups.slice(0, 5)) {
    if (group.title && group.title.trim().length >= 4) {
      phrases.push(group.title.trim());
    }
  }
  const deduped = new Set<string>();
  for (const phrase of phrases) {
    const normalized = phrase.toLowerCase();
    if (normalized === 'official') continue;
    deduped.add(normalized);
  }
  return [...deduped];
}

function hasAnyAnchorPhrase(text: string | undefined, anchors: string[]): boolean {
  if (!text || anchors.length === 0) return false;
  const normalized = text.toLowerCase();
  return anchors.some((anchor) => normalized.includes(anchor));
}

function hasSupportedEvidence(item: EvidenceItem | undefined): boolean {
  if (!item) return false;
  const quote = typeof item.quote === 'string' ? item.quote.trim() : '';
  const date = typeof item.date === 'string' ? item.date.trim() : '';
  return quote.length >= 12 && /^\d{4}-\d{2}-\d{2}$/.test(date);
}

function countSupportedEvidenceItems(profile: PsychographicProfile): number {
  const buckets = [
    profile.quirks ?? [],
    profile.notable_topics ?? [],
    profile.pain_points ?? [],
    profile.crypto_values ?? [],
    profile.connection_requests ?? [],
  ];
  let count = 0;
  for (const bucket of buckets) {
    for (const item of bucket) {
      if (hasSupportedEvidence(item)) count++;
    }
  }
  return count;
}

function textFromBundleItem(item: BundleEvidenceItem): string {
  const parts: string[] = [];
  if (item.short_text) parts.push(item.short_text);
  if (item.context_parent) parts.push(item.context_parent);
  if (Array.isArray(item.context_nearby)) parts.push(...item.context_nearby);
  return parts.join(' ').replace(/\s+/g, ' ').trim();
}

function countBundleCueMatches(bundle: EvidenceBundle, cue: RegExp): number {
  const seen = new Set<number>();
  const items = [
    ...bundle.packs.topics,
    ...bundle.packs.values_seeking,
    ...bundle.packs.events_affiliations,
  ];
  for (const item of items) {
    if (seen.has(item.message_id)) continue;
    const text = textFromBundleItem(item);
    if (!text) continue;
    if (cue.test(text)) {
      seen.add(item.message_id);
    }
  }
  return seen.size;
}

function countSelfOwnedSeekCueMatches(bundle: EvidenceBundle): number {
  const seen = new Set<number>();
  const items = [
    ...bundle.packs.values_seeking,
    ...bundle.packs.topics,
    ...bundle.packs.events_affiliations,
  ];
  for (const item of items) {
    if (seen.has(item.message_id)) continue;
    const text = textFromBundleItem(item);
    if (!text) continue;
    if (!DIRECT_ASK_RE.test(text)) continue;
    if (!ACTIONABLE_ASK_RE.test(text)) continue;
    if (HISTORICAL_ASK_RE.test(text)) continue;
    if (FORWARDED_SEEK_RE.test(text) && !SELF_SEEK_RE.test(text)) continue;
    if (SELF_SEEK_RE.test(text) || SELF_SUPPORT_RE.test(text)) {
      seen.add(item.message_id);
    }
  }
  return seen.size;
}

function isHardRichFailure(failure: string): boolean {
  if (failure.includes('generated_bio_professional missing concrete org/event anchor')) return false;
  if (failure.includes('generated_bio_personal missing concrete org/event anchor')) return false;
  return true;
}

function assessRichProfileQuality(
  profile: PsychographicProfile,
  bundle: EvidenceBundle,
): ProfileQualityAssessment {
  const isRich = isRichEvidenceBundle(bundle);
  if (!isRich) {
    return { isRich: false, score: 100, failures: [], shouldRetry: false };
  }

  const targets = getRichTargetsForBundle(bundle);
  const failures: string[] = [];
  const profBio = profile.generated_bio_professional?.trim();
  const persBio = profile.generated_bio_personal?.trim();
  const anchors = collectBioAnchorPhrases(bundle);
  if (!profBio) failures.push('missing generated_bio_professional');
  if (!persBio) failures.push('missing generated_bio_personal');
  if (hasGenericBioPhrase(profBio)) failures.push('generated_bio_professional contains generic boilerplate');
  if (hasGenericBioPhrase(persBio)) failures.push('generated_bio_personal contains generic boilerplate');
  if (anchors.length > 0) {
    if (profBio && !hasAnyAnchorPhrase(profBio, anchors)) {
      failures.push('generated_bio_professional missing concrete org/event anchor');
    }
    if (persBio && !hasAnyAnchorPhrase(persBio, anchors)) {
      failures.push('generated_bio_personal missing concrete org/event anchor');
    }
  }

  const hasRoleCompanySignal = Boolean(
    profile.primary_role
    || profile.primary_company
    || profile.current_role_company?.role
    || profile.current_role_company?.org,
  );
  if (!hasRoleCompanySignal) failures.push('missing role/company identity');
  if ((profile.role_company_timeline?.length ?? 0) === 0 && bundle.packs.role_company.length >= 3) {
    failures.push('missing role_company_timeline');
  }

  const supportedEvidenceCount = countSupportedEvidenceItems(profile);
  if (supportedEvidenceCount < targets.minSupportedEvidenceItems) {
    failures.push(`insufficient evidence-backed personality items (${supportedEvidenceCount}/${targets.minSupportedEvidenceItems})`);
  }

  const topicsCount = profile.notable_topics?.length ?? 0;
  if (bundle.packs.topics.length >= targets.minTopics && topicsCount < targets.minTopics) {
    failures.push(`too few notable_topics for rich profile (${topicsCount}/${targets.minTopics})`);
  }

  const painCueCount = countBundleCueMatches(bundle, BUNDLE_PAIN_CUE_RE);
  const painCount = profile.pain_points?.length ?? 0;
  const minPainRequired = Math.min(targets.minPainPoints, painCueCount);
  if (painCueCount >= 2 && painCount < minPainRequired) {
    failures.push(`missing pain_points despite pain cues (${painCount}/${minPainRequired}; cues=${painCueCount})`);
  }

  const quirkCueCount = countBundleCueMatches(bundle, BUNDLE_QUIRK_CUE_RE);
  const quirkCount = profile.quirks?.length ?? 0;
  const minQuirkRequired = Math.min(targets.minQuirks, Math.max(2, quirkCueCount));
  if (quirkCueCount >= 2 && quirkCount < minQuirkRequired) {
    failures.push(`too few quirks despite stylistic cues (${quirkCount}/${minQuirkRequired})`);
  }

  const seekCueCount = countSelfOwnedSeekCueMatches(bundle);
  const seekCount = profile.connection_requests?.length ?? 0;
  const minSeekRequired = Math.min(targets.minConnectionRequests, seekCueCount);
  if (seekCueCount >= 2 && seekCount < minSeekRequired) {
    failures.push(`missing connection_requests despite seeking cues (${seekCount}/${minSeekRequired}; cues=${seekCueCount})`);
  }

  const drivingValuesCount = profile.driving_values?.length ?? 0;
  if (bundle.packs.values_seeking.length >= targets.minDrivingValues && drivingValuesCount < targets.minDrivingValues) {
    failures.push(`too few driving_values for rich profile (${drivingValuesCount}/${targets.minDrivingValues})`);
  }

  const deepSkillsCount = profile.deep_skills?.length ?? 0;
  const minDeepSkills = Math.min(targets.minDeepSkills, Math.max(2, Math.floor(bundle.packs.topics.length / 3)));
  if (bundle.packs.topics.length >= 8 && deepSkillsCount < minDeepSkills) {
    failures.push(`too few deep_skills for rich profile (${deepSkillsCount}/${minDeepSkills})`);
  }

  const technicalSpecificsCount = profile.technical_specifics?.length ?? 0;
  const technicalEvidenceBudget = bundle.packs.topics.length + bundle.packs.links.length;
  const minTechnicalSpecifics = Math.min(
    targets.minTechnicalSpecifics,
    Math.max(3, Math.floor(technicalEvidenceBudget / 2)),
  );
  if (technicalEvidenceBudget >= 10 && technicalSpecificsCount < minTechnicalSpecifics) {
    failures.push(`too few technical_specifics for rich profile (${technicalSpecificsCount}/${minTechnicalSpecifics})`);
  }

  const affiliationsCount = profile.affiliations?.length ?? 0;
  const orgEvidenceCount = bundle.evidence_summary.role_company_org_scores.length;
  const minAffiliations = Math.min(targets.minAffiliations, Math.max(2, orgEvidenceCount));
  if (orgEvidenceCount >= 4 && affiliationsCount < minAffiliations) {
    failures.push(`too few affiliations for rich profile (${affiliationsCount}/${minAffiliations})`);
  }

  const fingerprintCount = profile.fingerprint_tags?.length ?? 0;
  const minFingerprintTags = Math.min(
    targets.minFingerprintTags,
    Math.max(5, bundle.packs.topics.length + Math.floor(bundle.packs.events_affiliations.length / 2)),
  );
  if (bundle.stats.total_messages >= 2000 && fingerprintCount < minFingerprintTags) {
    failures.push(`too few fingerprint_tags for rich profile (${fingerprintCount}/${minFingerprintTags})`);
  }

  if (!profile.preferred_contact_style || profile.preferred_contact_style.trim().length < 24) {
    failures.push('preferred_contact_style too thin');
  }

  const score = Math.max(0, 100 - (failures.length * 16));
  return {
    isRich,
    score,
    failures,
    shouldRetry: failures.length > 0,
  };
}

function buildAgentPlaybook(profile: PsychographicProfile, bundle: EvidenceBundle): Record<string, unknown> {
  const role = profile.current_role_company?.role || profile.primary_role || null;
  const org = profile.current_role_company?.org || profile.primary_company || null;
  const identity = [role, org ? `@ ${org}` : null].filter(Boolean).join(' ') || 'Unknown role';

  const topic = profile.notable_topics?.[0]?.value ?? null;
  const pain = profile.pain_points?.[0]?.value ?? null;
  const style = profile.preferred_contact_style || 'Direct and concrete';

  const openers: string[] = [];
  if (topic) openers.push(`Your take on ${topic.toLowerCase()} stood out. Curious how that view changed recently.`);
  if (pain) openers.push(`You called out ${pain.toLowerCase()}. What does a practical fix look like to you?`);
  if (org) openers.push(`Saw your recent updates around ${org}. What partnership angle feels highest leverage right now?`);

  const avoid: string[] = [
    'Avoid vague networking intros',
    'Avoid generic "tell me about yourself" asks',
  ];
  if ((profile.tone ?? '').toLowerCase() === 'blunt') avoid.push('Avoid fluffy praise; keep it concise');
  if ((profile.professionalism ?? '').toLowerCase() === 'professional') avoid.push('Avoid unserious or meme-heavy opener tone');

  const trustSignals = [
    ...(org ? [org] : []),
    ...((profile.affiliations ?? []).slice(0, 3)),
    ...((profile.attended_events ?? []).slice(0, 2)),
  ];

  return {
    identity_anchor: identity,
    openers: openers.slice(0, 3),
    contact_style: style,
    trust_signals: [...new Set(trustSignals)].filter(Boolean).slice(0, 8),
    avoid,
    conversation_tracks: [
      {
        lane: 'business',
        objective: 'Partnership or fundraising context',
        starter: topic
          ? `Start with ${topic.toLowerCase()} and ask for execution criteria.`
          : 'Start with one concrete partnership objective.',
      },
      {
        lane: 'market',
        objective: 'Industry perspective',
        starter: pain
          ? `Anchor on ${pain.toLowerCase()} and ask what should change.`
          : 'Ask for one high-conviction market view and rationale.',
      },
      {
        lane: 'rapport',
        objective: 'Natural warm-up',
        starter: (profile.attended_events?.length ?? 0) > 0
          ? `Reference ${profile.attended_events![0]} and ask what made it useful.`
          : 'Reference a recent ecosystem event with a specific angle.',
      },
    ],
  };
}

function buildQualityRetryPrompt(
  basePrompt: string,
  failures: string[],
  attempt: number,
  maxRetries: number,
): string {
  const failureList = failures.map((failure) => `- ${failure}`).join('\n');
  const bannedPhrases = GENERIC_BIO_PHRASES.map((phrase) => `"${phrase}"`).join(', ');
  return `${basePrompt}

## QUALITY REPAIR REQUIRED
Retry attempt ${attempt}/${maxRetries}.
The previous response failed these quality checks:
${failureList}

Fix every failed item in this retry.
Hard requirements:
- Do not use these boilerplate phrases: ${bannedPhrases}
- For rich profiles, satisfy all minimum-count failures listed above exactly.
- If complaint/problem cues exist, include at least 1 pain_points item.
- If stylistic/slang cues exist, include at least 2 quirks items.
- Keep role/company outputs specific and grounded to the provided evidence IDs.

Return JSON only.`;
}

function jsonEqual(a: unknown, b: unknown): boolean {
  return JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
}

function parseUserIds(raw: string | undefined): number[] {
  if (!raw) return [];
  const ids = raw
    .split(',')
    .map((value) => parseInt(value.trim(), 10))
    .filter((id) => Number.isFinite(id) && id > 0);
  return [...new Set(ids)];
}

function logDryRunEvidence(packs: {
  role_company: BundleEvidenceItem[];
  links: BundleEvidenceItem[];
  topics: BundleEvidenceItem[];
  values_seeking: BundleEvidenceItem[];
  events_affiliations: BundleEvidenceItem[];
}): void {
  const logPack = (name: string, items: BundleEvidenceItem[]) => {
    console.log(`   ${name} top evidence (max 5):`);
    if (items.length === 0) {
      console.log('      (none)');
      return;
    }
    for (const item of items.slice(0, 5)) {
      const snippet = (item.short_text ?? '').replace(/\s+/g, ' ').trim();
      const orgs = item.extracted_orgs.length > 0 ? item.extracted_orgs.join(', ') : '(none)';
      const roles = item.extracted_roles.length > 0 ? item.extracted_roles.join(', ') : '(none)';
      const orgStrict = item.org_candidates_strict.length > 0 ? item.org_candidates_strict.join(', ') : '(none)';
      const orgUrl = item.org_candidates_url_anchored.length > 0 ? item.org_candidates_url_anchored.join(', ') : '(none)';
      const orgLoose = item.org_candidates_loose.length > 0 ? item.org_candidates_loose.join(', ') : '(none)';
      const urls = item.urls.length > 0 ? item.urls.join(', ') : '(none)';
      console.log(
        `      id=${item.message_id} sent_at=${item.sent_at} group=${item.group_title ?? '(none)'} orgs=${orgs} org_strict=${orgStrict} org_url=${orgUrl} org_loose=${orgLoose} roles=${roles} urls=${urls} snippet="${snippet}"`,
      );
    }
  };

  logPack('role_company', packs.role_company);
  logPack('topics', packs.topics);
  logPack('links', packs.links);
  logPack('values_seeking', packs.values_seeking);
  logPack('events_affiliations', packs.events_affiliations);
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

  const dedupeStrings = (arr: string[]): string[] => {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const raw of arr) {
      const v = raw.trim();
      if (!v) continue;
      const key = v.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(v);
    }
    return out;
  };

  const sanitizeTechnicalSpecifics = (arr: string[] | undefined): string[] => {
    if (!Array.isArray(arr)) return [];
    const cleaned = arr
      .map((v) => (typeof v === 'string' ? v.trim() : ''))
      .filter(Boolean)
      .filter((v) => !TECH_SPECIFIC_STOPWORDS.has(v.toLowerCase()))
      .filter((v) => v.length >= 2);
    return dedupeStrings(cleaned).slice(0, 15);
  };

  const sanitizeConnectionRequests = (arr: EvidenceItem[] | undefined): EvidenceItem[] => {
    if (!Array.isArray(arr)) return [];
    const filtered = arr.filter((item) => {
      const quote = (item.quote ?? '').trim();
      const value = (item.value ?? '').trim();
      const text = `${value} ${quote}`.trim();
      if (!text) return false;
      if (!DIRECT_ASK_RE.test(text)) return false;
      if (!ACTIONABLE_ASK_RE.test(text)) return false;
      if (HISTORICAL_ASK_RE.test(text)) return false;

      // Keep explicit first-person seeking/support asks; drop pure forwarded asks.
      if (SELF_SEEK_RE.test(text)) return true;
      if (FORWARDED_SEEK_RE.test(text)) return false;
      return false;
    });
    return filtered.slice(0, 8);
  };

  const sanitizeDrivingValues = (arr: string[] | undefined, cryptoValues: EvidenceItem[]): string[] => {
    const raw = Array.isArray(arr) ? arr : [];
    const cleaned = dedupeStrings(
      raw
        .map((v) => (typeof v === 'string' ? v.trim() : ''))
        .filter(Boolean),
    ).slice(0, 8);

    const hasSpecific = cleaned.some((v) => !GENERIC_DRIVING_VALUE_RE.test(v));
    if (hasSpecific) return cleaned;

    const specificFromEvidence = dedupeStrings(
      cryptoValues
        .map((v) => (v.value ?? '').trim())
        .filter((v) => v.length > 0 && !GENERIC_DRIVING_VALUE_RE.test(v)),
    ).slice(0, 8);

    return specificFromEvidence.length > 0 ? specificFromEvidence : cleaned;
  };

  const validCurrentRole = (obj: any) => {
    if (!obj || typeof obj !== 'object') return undefined;
    return {
      role: typeof obj.role === 'string' ? obj.role : null,
      org: typeof obj.org === 'string' ? obj.org : null,
      confidence: typeof obj.confidence === 'number' ? obj.confidence : 0,
      evidence_message_ids: Array.isArray(obj.evidence_message_ids) ? obj.evidence_message_ids.filter((x: any) => Number.isFinite(x)) : [],
    };
  };

  const validPrevRoles = (arr: any) => {
    if (!Array.isArray(arr)) return [];
    return arr.map((o) => ({
      role: typeof o?.role === 'string' ? o.role : null,
      org: typeof o?.org === 'string' ? o.org : null,
      end_hint: typeof o?.end_hint === 'string' ? o.end_hint : null,
      confidence: typeof o?.confidence === 'number' ? o.confidence : 0,
      evidence_message_ids: Array.isArray(o?.evidence_message_ids) ? o.evidence_message_ids.filter((x: any) => Number.isFinite(x)) : [],
    }));
  };

  const validTimeline = (arr: any) => {
    if (!Array.isArray(arr)) return [];
    return arr.map((o) => ({
      org: typeof o?.org === 'string' ? o.org : null,
      role: typeof o?.role === 'string' ? o.role : null,
      start_hint: typeof o?.start_hint === 'string' ? o.start_hint : null,
      end_hint: typeof o?.end_hint === 'string' ? o.end_hint : null,
      is_current: Boolean(o?.is_current),
      evidence_message_ids: Array.isArray(o?.evidence_message_ids) ? o.evidence_message_ids.filter((x: any) => Number.isFinite(x)) : [],
      confidence: typeof o?.confidence === 'number' ? o.confidence : 0,
    }));
  };

  const normalizedCryptoValues = validEvidence(p.crypto_values as any);
  const normalizedConnectionRequests = sanitizeConnectionRequests(validEvidence(p.connection_requests as any));

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
    crypto_values: normalizedCryptoValues,
    connection_requests: normalizedConnectionRequests,
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
    driving_values: sanitizeDrivingValues(Array.isArray(p.driving_values) ? p.driving_values : [], normalizedCryptoValues),
    technical_specifics: sanitizeTechnicalSpecifics(Array.isArray(p.technical_specifics) ? p.technical_specifics : []),
    business_focus: Array.isArray(p.business_focus) ? p.business_focus : [],

    current_role_company: validCurrentRole(p.current_role_company),
    previous_roles_companies: validPrevRoles(p.previous_roles_companies),
    role_company_timeline: validTimeline(p.role_company_timeline),
    field_confidence: typeof p.field_confidence === 'object' && p.field_confidence !== null ? p.field_confidence : undefined,
  };
}

main().catch((err) => {
  console.error('❌ enrich-psycho failed:', err);
  process.exit(1);
});
