
import { db } from '../db/index.js';
import fs from 'fs';
import path from 'path';

const DEFAULT_MODEL_VERSION = 'v0.6.0';
const DEFAULT_SCOPE_MODE = 'enriched_only';
const VALID_SCOPE_MODES = ['enriched_only', 'profiles_only', 'all_data'] as const;
type ScopeMode = typeof VALID_SCOPE_MODES[number];

function keyOf(id: unknown): string {
  if (typeof id === 'string') return id;
  if (typeof id === 'number') return String(id);
  if (typeof id === 'bigint') return id.toString();
  return String(id);
}

function parsePositiveInt(raw: string | undefined): number | null {
  if (!raw) return null;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) return null;
  return n;
}

function parseStatuses(raw: string | undefined): string[] {
  const src = raw && raw.trim() ? raw : 'supported';
  return Array.from(
    new Set(
      src
        .split(',')
        .map((s) => s.trim().toLowerCase())
        .filter(Boolean),
    ),
  );
}

function parseScopeMode(raw: string | undefined): ScopeMode {
  if (!raw) return DEFAULT_SCOPE_MODE;
  const normalized = raw.trim().toLowerCase();
  if ((VALID_SCOPE_MODES as readonly string[]).includes(normalized)) {
    return normalized as ScopeMode;
  }
  return DEFAULT_SCOPE_MODE;
}

function sortUserIds(ids: Iterable<string>): string[] {
  return Array.from(ids).sort((a, b) => {
    const an = Number(a);
    const bn = Number(b);
    const aNum = Number.isFinite(an);
    const bNum = Number.isFinite(bn);
    if (aNum && bNum) return an - bn;
    return a.localeCompare(b);
  });
}

function withLimit(ids: string[], limit: number | null): string[] {
  if (!limit || ids.length <= limit) return ids;
  return ids.slice(0, limit);
}

function union(...sets: Set<string>[]): Set<string> {
  const out = new Set<string>();
  for (const s of sets) {
    for (const v of s) out.add(v);
  }
  return out;
}

function difference(a: Set<string>, b: Set<string>): Set<string> {
  const out = new Set<string>();
  for (const v of a) {
    if (!b.has(v)) out.add(v);
  }
  return out;
}

async function main() {
  const version = process.env.VIEWER_MODEL_VERSION?.trim() || DEFAULT_MODEL_VERSION;
  const qualifyingClaimStatuses = parseStatuses(process.env.VIEWER_QUALIFYING_CLAIM_STATUSES);
  const scopeUserLimit = parsePositiveInt(process.env.VIEWER_SCOPE_USER_LIMIT);
  const defaultScopeMode = parseScopeMode(process.env.VIEWER_DEFAULT_SCOPE_MODE);

  console.log(`Exporting data for version ${version}...`);
  console.log(`Qualifying claim statuses: ${qualifyingClaimStatuses.join(', ')}`);
  console.log(`Default scope mode: ${defaultScopeMode}`);
  if (scopeUserLimit) {
    console.log(`Applying scope user limit: ${scopeUserLimit}`);
  }

  // 1. Get Claims
  console.log('Fetching claims...');
  const claimsRes = await db.query(`
    SELECT 
      c.id, c.subject_user_id, u.display_name, u.handle, u.bio, u.bio_source, u.bio_updated_at,
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
      p.user_id, u.display_name, u.handle, u.bio, u.bio_source, u.bio_updated_at,
      p.tone, p.professionalism, p.verbosity, p.responsiveness, p.decision_style, 
      p.seniority_signal, p.commercial_archetype, p.approachability, 
      p.quirks, p.notable_topics, p.pain_points, p.crypto_values, p.connection_requests, p.fingerprint_tags,
      p.based_in, p.attended_events, p.preferred_contact_style, p.reasoning, p.created_at,
      p.generated_bio_professional, p.generated_bio_personal, p.primary_role, p.primary_company,
      p.deep_skills, p.affiliations, p.social_platforms, p.social_urls, p.buying_power, p.languages,
      p.scam_risk_score, p.confidence_score, p.career_stage, p.tribe_affiliations,
      p.reputation_score, p.driving_values, p.technical_specifics, p.business_focus,
      p.fifo, p.group_tags, p.reputation_summary,
      p.total_msgs, p.avg_msg_length, p.peak_hours, p.active_days,
      p.total_reactions, p.avg_reactions_per_msg, p.total_replies_received, p.avg_replies_per_msg, p.engagement_rate,
      p.last_active_days, p.top_conversation_partners, p.role_company_timeline, p.conflict_notes, p.evidence_summary_json
    FROM user_psychographics p
    JOIN users u ON u.id = p.user_id
    ORDER BY p.user_id, p.created_at DESC
  `);

  const claimUserIds = new Set<string>();
  const qualifyingClaimUserIds = new Set<string>();
  for (const row of claimsRes.rows as any[]) {
    const uid = keyOf(row.subject_user_id);
    claimUserIds.add(uid);
    const status = String(row.status || '').toLowerCase();
    if (qualifyingClaimStatuses.includes(status)) {
      qualifyingClaimUserIds.add(uid);
    }
  }

  const psychoUserIds = new Set<string>();
  for (const row of psychoRes.rows as any[]) {
    psychoUserIds.add(keyOf(row.user_id));
  }

  const scopeRaw = {
    enriched_only: sortUserIds(psychoUserIds),
    profiles_only: sortUserIds(union(psychoUserIds, qualifyingClaimUserIds)),
    all_data: sortUserIds(union(psychoUserIds, claimUserIds)),
  } as const;

  const scopeVisible = {
    enriched_only: withLimit(scopeRaw.enriched_only, scopeUserLimit),
    profiles_only: withLimit(scopeRaw.profiles_only, scopeUserLimit),
    all_data: withLimit(scopeRaw.all_data, scopeUserLimit),
  } as const;

  const claimOnlyAll = difference(claimUserIds, psychoUserIds);
  const claimOnlyQualifying = difference(qualifyingClaimUserIds, psychoUserIds);

  // 4b. Build evidence lookup from psychographics
  console.log('Building evidence lookup...');
  const evidenceIds = new Set<number>();
  for (const row of psychoRes.rows as any[]) {
    const timeline = row.role_company_timeline;
    if (Array.isArray(timeline)) {
      for (const t of timeline) {
        const ids = t?.evidence_message_ids;
        if (Array.isArray(ids)) {
          for (const id of ids) {
            if (Number.isFinite(id)) evidenceIds.add(id);
          }
        }
      }
    }
    const conflicts = row.conflict_notes;
    if (conflicts && typeof conflicts === 'object') {
      const walk = (obj: any) => {
        if (!obj || typeof obj !== 'object') return;
        if (Array.isArray(obj)) {
          obj.forEach(walk);
          return;
        }
        if (Array.isArray(obj.evidence_message_ids)) {
          for (const id of obj.evidence_message_ids) {
            if (Number.isFinite(id)) evidenceIds.add(id);
          }
        }
        for (const v of Object.values(obj)) walk(v);
      };
      walk(conflicts);
    }
  }

  const evidenceLookup: Record<number, { sent_at: string; group_title: string; snippet: string }> = {};
  if (evidenceIds.size > 0) {
    const idList = Array.from(evidenceIds);
    const { rows: evidenceRows } = await db.query<{
      message_id: number; sent_at: string; group_title: string; snippet: string;
    }>(`
      SELECT m.id as message_id, m.sent_at::text as sent_at, g.title as group_title,
             LEFT(COALESCE(m.text, ''), 240) as snippet
      FROM messages m
      JOIN groups g ON g.id = m.group_id
      WHERE m.id = ANY($1)
    `, [idList]);

    for (const r of evidenceRows) {
      evidenceLookup[r.message_id] = {
        sent_at: r.sent_at,
        group_title: r.group_title,
        snippet: r.snippet,
      };
    }
  }

  // 5. Activity heatmap: day-of-week × hour-of-day from raw messages (enriched users only)
  console.log('Computing activity heatmap...');
  const heatmapRes = await db.query<{
    dow: number;
    hour: number;
    user_id: unknown;
    cnt: number;
  }>(`
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
  const userActivity: Record<string, number[][]> = {};
  for (const row of heatmapRes.rows) {
    const uid = keyOf(row.user_id);
    if (!userActivity[uid]) {
      userActivity[uid] = Array.from({ length: 7 }, () => new Array(24).fill(0));
    }
    userActivity[uid][row.dow][row.hour] += row.cnt;
  }

  // 6. Lifecycle metrics: first/last message, volume, active months
  console.log('Computing lifecycle metrics...');
  const lifecycleRes = await db.query<{
    user_id: unknown;
    first_msg_at: string;
    last_msg_at: string;
    total_msgs: number;
    active_months: string[] | null;
  }>(`
    SELECT
      m.user_id,
      MIN(m.sent_at)::text AS first_msg_at,
      MAX(m.sent_at)::text AS last_msg_at,
      COUNT(*)::int AS total_msgs,
      ARRAY_AGG(
        DISTINCT to_char(date_trunc('month', m.sent_at), 'YYYY-MM')
        ORDER BY to_char(date_trunc('month', m.sent_at), 'YYYY-MM')
      ) AS active_months
    FROM messages m
    WHERE m.user_id IN (SELECT user_id FROM user_psychographics)
    GROUP BY m.user_id
  `);

  const lifecycle: Record<string, {
    first_msg_at: string;
    last_msg_at: string;
    total_msgs: number;
    active_months: string[];
  }> = {};

  for (const row of lifecycleRes.rows) {
    const uid = keyOf(row.user_id);
    lifecycle[uid] = {
      first_msg_at: row.first_msg_at,
      last_msg_at: row.last_msg_at,
      total_msgs: row.total_msgs,
      active_months: Array.isArray(row.active_months) ? row.active_months : [],
    };
  }

  // 7. Stats
  const stats = {
     generated_at: new Date().toISOString(),
     model_version: version,
     total_claims: claimsRes.rows.length,
     supported_claims: claimsRes.rows.filter(r => String(r.status).toLowerCase() === 'supported').length,
     total_abstentions: abstentionRes.rows.length,
     enrichment_count: enrichmentsRes.rows.length,
     psycho_count: psychoRes.rows.length,
     psycho_users: psychoUserIds.size,
     claim_users: claimUserIds.size,
     qualifying_claim_users: qualifyingClaimUserIds.size,
     claim_only_users: claimOnlyAll.size,
     qualifying_claim_only_users: claimOnlyQualifying.size,
     qualifying_claim_statuses: qualifyingClaimStatuses,
     scope_default_mode: defaultScopeMode,
     scope_limit_env: scopeUserLimit,
     scope_limit_applied: Boolean(scopeUserLimit),
     scope_raw_counts: {
      enriched_only: scopeRaw.enriched_only.length,
      profiles_only: scopeRaw.profiles_only.length,
      all_data: scopeRaw.all_data.length,
     },
     scope_visible_counts: {
      enriched_only: scopeVisible.enriched_only.length,
      profiles_only: scopeVisible.profiles_only.length,
      all_data: scopeVisible.all_data.length,
     },
     scope_visible_size_used: scopeVisible[defaultScopeMode].length,
  };

  const data = {
      stats,
      scope: {
        default_mode: defaultScopeMode,
        available_modes: VALID_SCOPE_MODES,
        qualifying_claim_statuses: qualifyingClaimStatuses,
        user_limit: scopeUserLimit,
        raw_counts: stats.scope_raw_counts,
        visible_counts: stats.scope_visible_counts,
        modes: {
          enriched_only: { user_ids: scopeVisible.enriched_only },
          profiles_only: { user_ids: scopeVisible.profiles_only },
          all_data: { user_ids: scopeVisible.all_data },
        },
      },
      claims: claimsRes.rows,
      abstentions: abstentionRes.rows,
      enrichments: enrichmentsRes.rows,
      psychographics: psychoRes.rows,
      activity: userActivity,
      lifecycle,
      evidence_lookup: evidenceLookup
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
