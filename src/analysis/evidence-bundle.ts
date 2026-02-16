/**
 * Pass C: Evidence bundle builder for enrichment.
 * Produces compact, high-signal evidence packs with recency bias.
 */

import { db } from '../db/index.js';
import { truncate } from '../utils.js';

export type EvidenceItem = {
  message_id: number;
  sent_at: string;
  group_title: string | null;
  short_text: string;
  context_parent?: string | null;
  context_nearby?: string[] | null;
  urls: string[];
  extracted_orgs: string[];
  extracted_roles: string[];
  org_candidates_strict: string[];
  org_candidates_url_anchored: string[];
  org_candidates_loose: string[];
  signal_types: string[];
  engagement: number;
};

export type EvidenceBundle = {
  user: {
    user_id: number;
    handle: string | null;
    display_name: string | null;
    bio: string | null;
    bio_source: string | null;
    bio_updated_at: string | null;
  };
  stats: {
    total_messages: number;
    avg_msg_length: number;
    total_reactions: number;
    total_replies_received: number;
    engagement_rate: number;
    last_active_days: number | null;
    peak_hours: number[];
    active_days: string[];
    top_groups: { title: string; kind: string; msg_count: number }[];
    top_conversation_partners: { handle: string; display_name: string; replies_sent: number; replies_received: number }[];
  };
  current_claims: {
    current_role_company: { role: string | null; org: string | null; evidence_message_ids: number[] };
    conflicts: { type: string; bio_orgs: string[]; message_orgs: string[] }[];
  };
  evidence_summary: {
    role_company: { count: number; recent_share_12m: number; min_date?: string | null; max_date?: string | null };
    links: { count: number; recent_share_12m: number; min_date?: string | null; max_date?: string | null };
    topics: { count: number; recent_share_12m: number; min_date?: string | null; max_date?: string | null };
    values_seeking: { count: number; recent_share_12m: number; min_date?: string | null; max_date?: string | null };
    events_affiliations: { count: number; recent_share_12m: number; min_date?: string | null; max_date?: string | null };
    bio_org_candidates: string[];
    role_company_org_scores: {
      org: string;
      strict_hits: number;
      url_hits: number;
      loose_hits: number;
      bio_hits: number;
      most_recent: string | null;
    }[];
  };
  packs: {
    role_company: EvidenceItem[];
    links: EvidenceItem[];
    topics: EvidenceItem[];
    values_seeking: EvidenceItem[];
    events_affiliations: EvidenceItem[];
  };
};

type InsightRow = {
  message_id: number;
  sent_at: string;
  text: string | null;
  group_title: string | null;
  reply_to_external_message_id?: string | null;
  signal_types: string[] | null;
  extracted_urls: string[] | null;
  extracted_orgs: string[] | null;
  extracted_roles: string[] | null;
  first_person_score: number | null;
  is_noise: boolean | null;
  reaction_count: number | null;
  reply_count: number | null;
};

const ORG_RE = /\b(at|from|with|joining|joined|work(?:ing)? at|work(?:ing)? for|now at|currently at)\s+([A-Z][\w&.\-]{1,40}(?:\s+[A-Z][\w&.\-]{1,40}){0,4})/g;
const ORG_BEFORE_ROLE_RE = /\b([A-Z][\w&.\-]{1,40}(?:\s+[A-Z][\w&.\-]{1,40}){0,4})\s+(?:Social|Community|Marketing|Business|Developer|Engineering|Product|Growth|Operations|Partnerships?|Research|Content|Project|Program|Sales|Account|People|Talent|Finance|Legal|Data|Security|Support)\s+(?:Manager|Lead|Director|Head|Specialist|Analyst|Engineer|Coordinator|Strategist|Advocate)\b/i;
const ROLE_AT_ORG_RE = /\b(?:Founder|Co-Founder|CEO|CTO|COO|CMO|CPO|CRO|VP|Head|Lead|Manager|Director|Engineer|Developer|Analyst|Researcher|Advisor|Contributor|Ambassador|Advocate)\b[\w\s\/&-]{0,35}\s(?:@|at)\s([A-Z][\w&.\-]{1,40}(?:\s+[A-Z][\w&.\-]{1,40}){0,4})/i;
const ROLE_KEYWORD_RE = /\b(founder|co-?founder|ceo|cto|coo|cmo|cpo|cro|vp|head|lead|manager|director|engineer|developer|analyst|researcher|advisor|contributor|ambassador|advocate|specialist|strategist)\b/i;
const ORG_WORD_RE = /^[A-Z][A-Za-z0-9&.\-]*$/;
const SELF_INTRO_RE = /\b(i am|i'm|im|my role(?: is)?|my title(?: is)?|i work at|i work for|working at|joined|joining|now at|currently at)\b/i;
const SELF_ROLE_RE = /\b(head of|lead|manager|director|founder|co-?founder|engineer|developer|content creator|community manager|growth|marketing|bd|sales)\b/i;
const SELF_AFFILIATION_ORG_RE = /\b(?:i work at|i work for|i'm working at|im working at|i am working at|i'm working for|im working for|i am working for|i joined|i'm joining|im joining|i am joining|i'm now at|im now at|i am now at|i'm currently at|im currently at|i am currently at|my first day at|first day at)\s+([A-Z][\w&.\-]{1,40}(?:\s+[A-Z][\w&.\-]{1,40}){0,4})/gi;
const ROLE_AFFILIATION_ORG_RE = /\b(?:head of(?:\s+[A-Za-z]+){0,3}|lead(?:\s+[A-Za-z]+){0,3}|manager|director|founder|co-?founder|engineer|developer|content creator|community manager|growth(?:\s+[A-Za-z]+){0,2}|marketing(?:\s+[A-Za-z]+){0,2}|bd|sales)\s+(?:at|for|@)\s+([A-Z][\w&.\-]{1,40}(?:\s+[A-Z][\w&.\-]{1,40}){0,4})/gi;
const WE_AFFILIATION_ORG_RE = /\b(?:we|our team|our)\s+(?:at|from)\s+([A-Z][\w&.\-]{1,40}(?:\s+[A-Z][\w&.\-]{1,40}){0,4})/gi;
const ROLE_AT_ORG_MESSAGE_RE = /\b(?:Founder|Co-Founder|CEO|CTO|COO|CMO|CPO|CRO|VP|Head|Lead|Manager|Director|Engineer|Developer|Analyst|Researcher|Advisor|Contributor|Ambassador|Advocate|Content Creator|Community Manager)\b(?:\s+[A-Za-z]+){0,4}\s*(?:@|at|for)\s+([A-Z][\w&.\-]{1,40}(?:\s+[A-Z][\w&.\-]{1,40}){0,4})/gi;
const URL_HOST_STOPWORDS = new Set([
  'www', 'app', 'docs', 'blog', 'api', 'cdn', 'x', 'twitter', 'discord', 'telegram',
  'youtube', 'youtu', 'google', 'forms', 'drive', 'linktr', 'bit', 'tinyurl', 'loom',
  'medium', 'mirror', 'substack', 'linkedin',
]);
const URL_PATH_STOPWORDS = new Set([
  'status', 'i', 'home', 'search', 'explore', 'intent', 'hashtag', 'reel', 'p', 'posts', 'activity',
]);
const ORG_TRAIL_STOPWORDS = new Set([
  'if', 'and', 'or', 'but', 'we', 'our', 'you', 'your', 'they', 'them', 'to', 'for',
  'with', 'in', 'on', 'at', 'the', 'a', 'an', 'of', 'is', 'are',
  'youre', 'were', 'im', 'next', 'one', 'then', 'there',
]);
const ORG_LEAD_STOPWORDS = new Set([
  'the', 'a', 'an', 'our', 'we', 'my', 'this', 'that', 'more', 'for',
]);
const ORG_STOPWORDS = new Set([
  'Social', 'Media', 'Manager', 'Lead', 'Head', 'Director', 'Founder', 'Co-Founder',
  'Community', 'Marketing', 'Business', 'Developer', 'Engineering', 'Product', 'Growth',
  'Operations', 'Partnerships', 'Research', 'Content', 'Project', 'Program', 'Sales',
  'Account', 'People', 'Talent', 'Finance', 'Legal', 'Data', 'Security', 'Support',
]);
const INSIGHTS_CLASSIFIER_VERSION = 'v2';
const PAIN_CUE_RE = /\b(issue|problem|bug|broken|hard|difficult|frustrat|annoy|wtf|fail(?:ed|ing|ure)?|not sure|can't|cant|stuck)\b/i;
const SEEK_CUE_RE = /\b(looking for|need(?:ing)?|want(?:ing)?|seeking|dm me|reach out|hmu|hire|hiring|help(?:ing)?)\b/i;
const OPINION_CUE_RE = /\b(i think|i feel|imo|imho|not sure|i guess|i believe)\b/i;
const QUIRK_CUE_RE = /\b(owo|uwu|gm|ser|ngmi|wagmi|lol|lmao|bruh)\b/i;
const SELF_OWNERSHIP_CUE_RE = /\b(i|i'm|im|we|we're|were|we've|our|my)\b/i;
const POLICY_NOISE_RE = /\b(don't forget to follow the rules|follow the rules|rule(?:s)?\b|introductory searches|clear communication of needs and goals|location precision can be adjusted|share your location with the other members|search function\s*\(ctrl\/cmd\+f\)|lead to (?:a|an) (?:ban|immediate ban)|you will be punished|if you'd like to share your location)\b/i;

function normalizeOrg(org: string): string {
  return org
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function looksLikeRole(s: string): boolean {
  return ROLE_KEYWORD_RE.test(s);
}

function looksLikeOrgToken(s: string): boolean {
  if (!ORG_WORD_RE.test(s)) return false;
  if (ORG_STOPWORDS.has(s)) return false;
  return true;
}

function cleanOrg(raw: string): string {
  return raw.replace(/^[^A-Za-z0-9]+|[^A-Za-z0-9]+$/g, '').trim();
}

function uniqueOrgs(orgs: string[]): string[] {
  const dedup = new Map<string, string>();
  for (const org of orgs) {
    const cleaned = sanitizeOrgCandidate(org);
    if (!cleaned) continue;
    if (ORG_STOPWORDS.has(cleaned)) continue;
    const tokens = cleaned.split(' ').filter(Boolean);
    if (!tokens.some((token) => /^[A-Z0-9]/.test(token))) continue;
    const key = normalizeOrg(cleaned);
    if (!key) continue;
    if (!dedup.has(key)) dedup.set(key, cleaned);
  }
  return [...dedup.values()];
}

function sanitizeOrgCandidate(raw: string): string {
  const normalized = cleanOrg(raw).replace(/[^A-Za-z0-9&\-\s]+/g, ' ').replace(/\s+/g, ' ').trim();
  if (!normalized) return '';
  const tokens = normalized.split(' ').filter(Boolean);
  while (tokens.length > 1) {
    const head = tokens[0].toLowerCase().replace(/[^a-z0-9]/g, '');
    if (!ORG_LEAD_STOPWORDS.has(head)) break;
    tokens.shift();
  }
  while (tokens.length > 1) {
    const tail = tokens[tokens.length - 1].toLowerCase().replace(/[^a-z0-9]/g, '');
    if (!ORG_TRAIL_STOPWORDS.has(tail)) break;
    tokens.pop();
  }
  return tokens.join(' ').trim();
}

function titleToken(token: string): string {
  if (!token) return token;
  return token[0].toUpperCase() + token.slice(1);
}

function escapeRegex(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function mentionsOrgInText(text: string, org: string): boolean {
  if (!text || !org) return false;
  const safe = escapeRegex(org).replace(/\s+/g, '\\s+');
  const re = new RegExp(`\\b${safe}\\b`, 'i');
  return re.test(text);
}

function isPolicyBoilerplate(text: string | null): boolean {
  return POLICY_NOISE_RE.test(normalizeMessageText(text));
}

function orgFromToken(token: string): string | null {
  const cleaned = token.replace(/[^A-Za-z0-9]/g, '');
  if (cleaned.length < 3 || cleaned.length > 30) return null;
  return titleToken(cleaned);
}

function extractUrlOrgCandidates(urls: string[]): string[] {
  const out: string[] = [];
  for (const raw of urls) {
    try {
      const u = new URL(raw);
      const host = u.hostname.toLowerCase();
      const parts = host.split('.').filter(Boolean);
      if (parts.length >= 2) {
        const sld = parts[parts.length - 2];
        if (!URL_HOST_STOPWORDS.has(sld)) {
          const sldOrg = orgFromToken(sld);
          if (sldOrg) out.push(sldOrg);
        }
      }

      const pathSegs = u.pathname.split('/').filter(Boolean);
      if (pathSegs.length > 0) {
        const firstSeg = pathSegs[0];
        if (!URL_PATH_STOPWORDS.has(firstSeg.toLowerCase())) {
          const segToken = firstSeg.replace(/\.eth$/i, '');
          const pathOrg = orgFromToken(segToken);
          if (pathOrg) out.push(pathOrg);
        }
      }
    } catch {
      // Skip malformed URL candidates.
    }
  }
  return uniqueOrgs(out);
}

type RoleCompanySignal = {
  strict_orgs: string[];
  url_orgs: string[];
  loose_orgs: string[];
  self_claim: boolean;
};

function extractStrictOrgsFromMessage(
  text: string,
  urls: string[],
  rowOrgs: string[],
  knownOrgNorms: Set<string>,
): RoleCompanySignal {
  const patternOrgs: string[] = [];

  SELF_AFFILIATION_ORG_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = SELF_AFFILIATION_ORG_RE.exec(text)) !== null) patternOrgs.push(cleanOrg(m[1]));

  ROLE_AFFILIATION_ORG_RE.lastIndex = 0;
  while ((m = ROLE_AFFILIATION_ORG_RE.exec(text)) !== null) patternOrgs.push(cleanOrg(m[1]));

  WE_AFFILIATION_ORG_RE.lastIndex = 0;
  while ((m = WE_AFFILIATION_ORG_RE.exec(text)) !== null) patternOrgs.push(cleanOrg(m[1]));

  ROLE_AT_ORG_MESSAGE_RE.lastIndex = 0;
  while ((m = ROLE_AT_ORG_MESSAGE_RE.exec(text)) !== null) patternOrgs.push(cleanOrg(m[1]));

  const explicitSelfRole = SELF_INTRO_RE.test(text) && SELF_ROLE_RE.test(text);
  const url_orgs = uniqueOrgs(extractUrlOrgCandidates(urls))
    .filter((org) => knownOrgNorms.has(normalizeOrg(org)));
  const strictFromPattern = uniqueOrgs(patternOrgs).filter((org) =>
    knownOrgNorms.has(normalizeOrg(org)) || explicitSelfRole,
  );
  const promotedUrlStrict = SELF_OWNERSHIP_CUE_RE.test(text)
    ? url_orgs.filter((org) => mentionsOrgInText(text, org) || /our website|our product|our team|we'?ve officially|we have officially/i.test(text))
    : [];
  const strict_orgs = uniqueOrgs([...strictFromPattern, ...promotedUrlStrict]);
  const strictNorm = new Set(strict_orgs.map((o) => normalizeOrg(o)));
  const urlNorm = new Set(url_orgs.map((o) => normalizeOrg(o)));
  const loose_orgs = uniqueOrgs(rowOrgs.filter((org) => !strictNorm.has(normalizeOrg(org)) && !urlNorm.has(normalizeOrg(org))));
  const self_claim = strict_orgs.length > 0 && (SELF_INTRO_RE.test(text) || SELF_OWNERSHIP_CUE_RE.test(text));

  return { strict_orgs, url_orgs, loose_orgs, self_claim };
}

function isThirdPartyEmploymentMention(text: string): boolean {
  const hasThirdParty = /\b(someone|person|he|she|they)\b/i.test(text);
  const hasEmploymentVerb = /\b(started working at|working at|work at|joined|joining|now at|currently at)\b/i.test(text);
  return hasThirdParty && hasEmploymentVerb;
}

function extractOrgsFromBio(bio: string): string[] {
  const out: string[] = [];

  // Explicit "at/from/with Org" patterns.
  ORG_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = ORG_RE.exec(bio)) !== null) {
    out.push(cleanOrg(m[2]));
  }

  // "Org Social Media Manager", etc.
  const orgBeforeRole = bio.match(ORG_BEFORE_ROLE_RE);
  if (orgBeforeRole?.[1]) out.push(cleanOrg(orgBeforeRole[1]));

  // "Role @ Org" / "Role at Org".
  const roleAtOrg = bio.match(ROLE_AT_ORG_RE);
  if (roleAtOrg?.[1]) out.push(cleanOrg(roleAtOrg[1]));

  // "Org | Role" / "Org - Role" and mirrored formats.
  const sepMatch = bio.match(/^(.+?)\s*[|@-]\s*(.+)$/);
  if (sepMatch) {
    const left = sepMatch[1].trim();
    const right = sepMatch[2].trim();
    if (looksLikeRole(right)) out.push(cleanOrg(left));
    if (looksLikeRole(left)) out.push(cleanOrg(right));
  }

  // Fallback for single-token orgs in short bios, e.g. "Zenrock Social Media Manager".
  if (out.length === 0 && looksLikeRole(bio)) {
    const firstToken = bio.split(/\s+/).find((tok) => looksLikeOrgToken(tok));
    if (firstToken) out.push(cleanOrg(firstToken));
  }

  // Deduplicate by normalized form while preserving original display value.
  return uniqueOrgs(out);
}

function scoreItem(row: InsightRow): number {
  const now = Date.now();
  const ageDays = (now - new Date(row.sent_at).getTime()) / (24 * 60 * 60 * 1000);
  const recency = 1 / (1 + ageDays / 365);
  const signalStrength =
    (row.signal_types?.length ?? 0) +
    (row.extracted_roles?.length ?? 0) +
    (row.extracted_orgs?.length ?? 0) +
    (row.extracted_urls?.length ?? 0) +
    (row.first_person_score ?? 0);
  const engagement = ((row.reaction_count ?? 0) + (row.reply_count ?? 0)) / 10;
  return (recency * 0.5) + (signalStrength * 0.3) + (engagement * 0.2);
}

function normalizeMessageText(text: string | null): string {
  return (text ?? '').replace(/\s+/g, ' ').trim();
}

function stripUrls(text: string): string {
  return text.replace(/https?:\/\/\S+/g, ' ').replace(/\s+/g, ' ').trim();
}

function isLinkOnlyish(row: InsightRow): boolean {
  const text = normalizeMessageText(row.text);
  if (!text) return true;
  const withoutUrls = stripUrls(text);
  const hasUrl = (row.extracted_urls?.length ?? 0) > 0 || /https?:\/\//i.test(text);
  if (!hasUrl) return false;
  return withoutUrls.length < 24;
}

function hasPainCue(text: string | null): boolean {
  return PAIN_CUE_RE.test(normalizeMessageText(text));
}

function hasSeekCue(text: string | null): boolean {
  return SEEK_CUE_RE.test(normalizeMessageText(text));
}

function hasOpinionCue(text: string | null): boolean {
  return OPINION_CUE_RE.test(normalizeMessageText(text));
}

function hasQuirkCue(text: string | null): boolean {
  return QUIRK_CUE_RE.test(normalizeMessageText(text));
}

function isPersonalCue(row: InsightRow): boolean {
  const text = row.text ?? '';
  if (isPolicyBoilerplate(text)) return false;
  return (row.first_person_score ?? 0) >= 0.5
    || hasOpinionCue(text)
    || hasPainCue(text)
    || hasSeekCue(text)
    || hasQuirkCue(text);
}

function scoreTopicItem(row: InsightRow): number {
  const base = scoreItem(row);
  const text = normalizeMessageText(row.text);
  const textLenBoost = Math.min(1.2, text.length / 180);
  const personalBoost = isPersonalCue(row) ? 1.0 : 0;
  const opinionBoost = hasOpinionCue(text) ? 0.8 : 0;
  const painBoost = hasPainCue(text) ? 0.6 : 0;
  const linkOnlyPenalty = isLinkOnlyish(row) ? 1.4 : 0;
  const policyPenalty = isPolicyBoilerplate(text) ? 2.5 : 0;
  return base + textLenBoost + personalBoost + opinionBoost + painBoost - linkOnlyPenalty - policyPenalty;
}

function scoreValuesItem(row: InsightRow): number {
  const base = scoreItem(row);
  const text = normalizeMessageText(row.text);
  const personalBoost = isPersonalCue(row) ? 0.9 : 0;
  const seekingBoost = hasSeekCue(text) ? 1.2 : 0;
  const painBoost = hasPainCue(text) ? 1.1 : 0;
  const opinionBoost = hasOpinionCue(text) ? 0.7 : 0;
  const linkOnlyPenalty = isLinkOnlyish(row) ? 1.5 : 0;
  const policyPenalty = isPolicyBoilerplate(text) ? 3 : 0;
  return base + personalBoost + seekingBoost + painBoost + opinionBoost - linkOnlyPenalty - policyPenalty;
}

function toEvidenceItem(
  row: InsightRow,
  signal?: RoleCompanySignal,
  contextParent?: string | null,
  contextNearby?: string[] | null,
): EvidenceItem {
  return {
    message_id: row.message_id,
    sent_at: row.sent_at,
    group_title: row.group_title,
    short_text: truncate(row.text ?? '', 240),
    context_parent: contextParent ? truncate(contextParent, 240) : null,
    context_nearby: contextNearby && contextNearby.length > 0 ? contextNearby.map((t) => truncate(t, 240)) : null,
    urls: (row.extracted_urls ?? []).slice(0, 3),
    extracted_orgs: row.extracted_orgs ?? [],
    extracted_roles: row.extracted_roles ?? [],
    org_candidates_strict: (signal?.strict_orgs ?? []).slice(0, 3),
    org_candidates_url_anchored: (signal?.url_orgs ?? []).slice(0, 3),
    org_candidates_loose: (signal?.loose_orgs ?? []).slice(0, 3),
    signal_types: row.signal_types ?? [],
    engagement: (row.reaction_count ?? 0) + (row.reply_count ?? 0),
  };
}

type SelectionOptions = {
  scoreFn?: (row: InsightRow) => number;
  mustIncludeIds?: Set<number>;
  roleSignals?: Map<number, RoleCompanySignal>;
};

async function selectWithRecencyBias(rows: InsightRow[], max: number, options?: SelectionOptions): Promise<EvidenceItem[]> {
  if (rows.length === 0) return [];
  const now = Date.now();
  const recent = rows.filter((r) => (now - new Date(r.sent_at).getTime()) <= 365 * 24 * 60 * 60 * 1000);
  const older = rows.filter((r) => !recent.includes(r));
  const recentTarget = Math.min(recent.length, Math.ceil(max * 0.5));

  const scoreFn = options?.scoreFn ?? scoreItem;
  const rank = (arr: InsightRow[]) => [...arr].sort((a, b) => scoreFn(b) - scoreFn(a));
  const selectedRows: InsightRow[] = [];
  const selectedIds = new Set<number>();

  const addRow = (row: InsightRow) => {
    if (selectedRows.length >= max) return;
    if (selectedIds.has(row.message_id)) return;
    selectedRows.push(row);
    selectedIds.add(row.message_id);
  };

  if (options?.mustIncludeIds && options.mustIncludeIds.size > 0) {
    for (const row of rank(rows.filter((r) => options.mustIncludeIds!.has(r.message_id)))) {
      addRow(row);
    }
  }

  let selectedRecent = selectedRows.filter((r) => recent.includes(r)).length;
  for (const row of rank(recent)) {
    if (selectedRecent >= recentTarget) break;
    const before = selectedRows.length;
    addRow(row);
    if (selectedRows.length > before) selectedRecent++;
  }

  for (const row of rank(older)) {
    if (selectedRows.length >= max) break;
    addRow(row);
  }

  if (selectedRows.length < max) {
    for (const row of rank(recent)) {
      if (selectedRows.length >= max) break;
      addRow(row);
    }
  }

  // Attach reply context and nearby context for short messages
  const withContext: EvidenceItem[] = [];
  for (const item of selectedRows.map((r) => toEvidenceItem(r, options?.roleSignals?.get(r.message_id))).slice(0, max)) {
    if (!item.short_text || item.short_text.length < 20) {
      const { rows: ctx } = await db.query<{ text: string }>(`
        SELECT m2.text FROM messages m
        JOIN messages m2
          ON m.reply_to_external_message_id = m2.external_message_id
         AND m.group_id = m2.group_id
        WHERE m.id = $1
      `, [item.message_id]);
      const contextParent = ctx[0]?.text ?? null;

      const { rows: near } = await db.query<{ text: string }>(`
        SELECT m2.text FROM messages m
        JOIN messages m2
          ON m2.group_id = m.group_id
         AND m2.sent_at BETWEEN (m.sent_at - interval '60 seconds') AND (m.sent_at + interval '60 seconds')
         AND m2.id != m.id
        WHERE m.id = $1
        ORDER BY abs(extract(epoch from (m2.sent_at - m.sent_at))) ASC
        LIMIT 2
      `, [item.message_id]);
      const contextNearby = near.map((r) => r.text).filter(Boolean);

      withContext.push({ ...item, context_parent: contextParent, context_nearby: contextNearby });
    } else {
      withContext.push(item);
    }
  }

  return withContext;
}

function buildRoleCompanyOrgScores(items: EvidenceItem[], bioOrgs: string[]): {
  org: string;
  strict_hits: number;
  url_hits: number;
  loose_hits: number;
  bio_hits: number;
  most_recent: string | null;
}[] {
  type OrgAgg = { org: string; strict_hits: number; url_hits: number; loose_hits: number; bio_hits: number; most_recent: string | null };
  const agg = new Map<string, OrgAgg>();

  const upsert = (org: string, kind: 'strict' | 'url' | 'loose' | 'bio', sentAt?: string) => {
    const key = normalizeOrg(org);
    if (!key) return;
    const date = sentAt ? new Date(sentAt).toISOString().slice(0, 10) : null;
    const row = agg.get(key) ?? { org, strict_hits: 0, url_hits: 0, loose_hits: 0, bio_hits: 0, most_recent: null };
    if (!row.org) row.org = org;
    if (kind === 'strict') row.strict_hits += 1;
    if (kind === 'url') row.url_hits += 1;
    if (kind === 'loose') row.loose_hits += 1;
    if (kind === 'bio') row.bio_hits += 1;
    if (date && (!row.most_recent || date > row.most_recent)) row.most_recent = date;
    agg.set(key, row);
  };

  for (const item of items) {
    const strict = [...new Set(item.org_candidates_strict ?? [])];
    const url = [...new Set(item.org_candidates_url_anchored ?? [])];
    const loose = [...new Set(item.org_candidates_loose ?? [])];
    for (const org of strict) upsert(org, 'strict', item.sent_at);
    for (const org of url) upsert(org, 'url', item.sent_at);
    for (const org of loose) upsert(org, 'loose', item.sent_at);
  }
  for (const org of bioOrgs) upsert(org, 'bio');

  return [...agg.values()]
    .sort((a, b) => {
      const scoreA = a.strict_hits * 3 + a.url_hits * 2 + a.loose_hits + a.bio_hits * 2;
      const scoreB = b.strict_hits * 3 + b.url_hits * 2 + b.loose_hits + b.bio_hits * 2;
      if (scoreB !== scoreA) return scoreB - scoreA;
      return (b.most_recent ?? '').localeCompare(a.most_recent ?? '');
    })
    .slice(0, 12);
}

export async function buildUserEvidenceBundle(userId: number): Promise<EvidenceBundle> {
  const { rows: userRows } = await db.query<{
    id: number; handle: string | null; display_name: string | null; bio: string | null; bio_source: string | null; bio_updated_at: string | null;
  }>(
    `SELECT id, handle, display_name, bio, bio_source, bio_updated_at FROM users WHERE id = $1`,
    [userId],
  );
  if (userRows.length === 0) throw new Error(`User ${userId} not found`);
  const user = userRows[0];

  const { rows: statsRows } = await db.query<{
    total_messages: string; avg_msg_length: string;
    total_reactions: string; total_replies_received: string;
    last_active: string | null;
  }>(`
    SELECT
      COUNT(*)::text AS total_messages,
      ROUND(AVG(text_len))::text AS avg_msg_length,
      COALESCE(SUM(reaction_count), 0)::text AS total_reactions,
      COALESCE(SUM(reply_count), 0)::text AS total_replies_received,
      MAX(sent_at) AS last_active
    FROM messages WHERE user_id = $1
  `, [userId]);

  const totalMessages = parseInt(statsRows[0]?.total_messages ?? '0', 10);
  const avgMsgLen = parseInt(statsRows[0]?.avg_msg_length ?? '0', 10);
  const totalReactions = parseInt(statsRows[0]?.total_reactions ?? '0', 10);
  const totalReplies = parseInt(statsRows[0]?.total_replies_received ?? '0', 10);
  const lastActive = statsRows[0]?.last_active ? new Date(statsRows[0].last_active) : null;
  const lastActiveDays = lastActive ? Math.round((Date.now() - lastActive.getTime()) / (24 * 60 * 60 * 1000)) : null;
  const engagementRate = totalMessages > 0 ? Number((((totalReactions + totalReplies) / totalMessages) * 100).toFixed(1)) : 0;

  const { rows: peakRows } = await db.query<{ hour: string; cnt: string }>(`
    SELECT EXTRACT(HOUR FROM sent_at AT TIME ZONE 'UTC')::int as hour, COUNT(*)::int as cnt
    FROM messages WHERE user_id = $1
    GROUP BY hour ORDER BY cnt DESC LIMIT 5
  `, [userId]);
  const peakHours = peakRows.map((r) => parseInt(r.hour, 10));

  const { rows: dayRows } = await db.query<{ day_name: string; cnt: string }>(`
    SELECT to_char(sent_at, 'Dy') as day_name, COUNT(*)::int as cnt
    FROM messages WHERE user_id = $1
    GROUP BY day_name ORDER BY cnt DESC LIMIT 5
  `, [userId]);
  const activeDays = dayRows.map((r) => r.day_name);

  const { rows: topGroups } = await db.query<{ title: string; kind: string; msg_count: string }>(`
    SELECT g.title, g.kind, m.msg_count::text as msg_count
    FROM memberships m JOIN groups g ON g.id = m.group_id
    WHERE m.user_id = $1
    ORDER BY m.msg_count DESC
    LIMIT 5
  `, [userId]);

  const { rows: partnerRows } = await db.query<{
    partner_id: string; handle: string | null; display_name: string | null; replies_sent: string; replies_received: string;
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
  `, [userId]);

  const { rows: insightRows } = await db.query<InsightRow>(`
    SELECT mi.message_id, mi.sent_at::text as sent_at, m.text, g.title as group_title,
           m.reply_to_external_message_id,
           mi.signal_types, mi.extracted_urls, mi.extracted_orgs, mi.extracted_roles,
           mi.first_person_score, mi.is_noise,
           COALESCE(m.reaction_count,0) as reaction_count,
           COALESCE(m.reply_count,0) as reply_count
    FROM message_insights mi
    JOIN messages m ON m.id = mi.message_id
    JOIN groups g ON g.id = mi.group_id
    WHERE mi.user_id = $1
      AND mi.classifier_version = $2
  `, [userId, INSIGHTS_CLASSIFIER_VERSION]);

  const usable = insightRows.filter((r) => !r.is_noise);
  const knownOrgNorms = new Set(
    usable.flatMap((r) => (r.extracted_orgs ?? []).map((org) => normalizeOrg(cleanOrg(org)))).filter(Boolean),
  );
  const originalOrgCounts = new Map<number, number>(insightRows.map((r) => [r.message_id, r.extracted_orgs?.length ?? 0]));
  const roleSignals = new Map<number, RoleCompanySignal>();
  const roleCompany = usable
    .map((row) => {
      const text = row.text ?? '';
      const urls = row.extracted_urls ?? [];
      const extractedOrgs = row.extracted_orgs ?? [];
      const signal = extractStrictOrgsFromMessage(text, urls, extractedOrgs, knownOrgNorms);
      roleSignals.set(row.message_id, signal);
      return {
        ...row,
        extracted_orgs: uniqueOrgs([...signal.strict_orgs, ...signal.url_orgs, ...extractedOrgs]),
      };
    })
    .filter((r) => {
      const signal = roleSignals.get(r.message_id);
      const hasStrictOrg = (signal?.strict_orgs.length ?? 0) > 0;
      const hasAnchoredUrlOrg = (signal?.url_orgs.length ?? 0) > 0;
      const hasOriginalOrg = (originalOrgCounts.get(r.message_id) ?? 0) > 0;
      const hasOrgEvidence = hasStrictOrg || hasAnchoredUrlOrg || hasOriginalOrg;
      const thirdPartyMention = isThirdPartyEmploymentMention(r.text ?? '');
      if (thirdPartyMention && !hasStrictOrg && !signal?.self_claim) return false;
      return hasOrgEvidence || Boolean(signal?.self_claim);
    });
  const links = usable.filter((r) => (r.extracted_urls?.length ?? 0) > 0);
  const topics = usable.filter((r) =>
    !isPolicyBoilerplate(r.text) && (
      (r.signal_types ?? []).some((t) => ['technical', 'opinion', 'events', 'social'].includes(t))
      || isPersonalCue(r)
    ),
  );
  const valuesSeeking = usable.filter((r) =>
    !isPolicyBoilerplate(r.text) && (
      (r.signal_types ?? []).some((t) => ['hiring', 'contact', 'opinion'].includes(t))
      || hasPainCue(r.text)
      || hasSeekCue(r.text)
      || hasOpinionCue(r.text)
    ),
  );
  const eventsAff = usable.filter((r) =>
    (r.signal_types ?? []).includes('events') || (r.extracted_orgs?.length ?? 0) > 0,
  );

  const scoreRoleCompany = (row: InsightRow): number => {
    const signal = roleSignals.get(row.message_id);
    const strictCount = signal?.strict_orgs.length ?? 0;
    const urlCount = signal?.url_orgs.length ?? 0;
    const looseCount = signal?.loose_orgs.length ?? 0;
    const selfClaimBoost = signal?.self_claim ? 3 : 0;
    const strictBoost = strictCount > 0 ? (2 + strictCount * 0.3) : 0;
    const urlBoost = strictCount === 0 && urlCount > 0 ? 1.0 : 0;
    const roleBoost = (row.extracted_roles?.length ?? 0) > 0 && (strictCount > 0 || urlCount > 0) ? 0.4 : 0;
    const loosePenalty = strictCount === 0 && urlCount === 0 && looseCount > 0 ? 2.2 : 0;
    const thirdPartyPenalty = strictCount === 0 && isThirdPartyEmploymentMention(row.text ?? '') ? 1.5 : 0;
    return scoreItem(row) + strictBoost + urlBoost + selfClaimBoost + roleBoost - loosePenalty - thirdPartyPenalty;
  };
  const mustIncludeRoleIds = new Set(
    roleCompany
      .filter((r) => roleSignals.get(r.message_id)?.self_claim)
      .map((r) => r.message_id),
  );

  const roleCompanyPack = await selectWithRecencyBias(roleCompany, 10, {
    scoreFn: scoreRoleCompany,
    mustIncludeIds: mustIncludeRoleIds,
    roleSignals,
  });
  const linksPack = await selectWithRecencyBias(links, 10);
  const topicsPackSize = totalMessages >= 10000 ? 30 : totalMessages >= 2000 ? 26 : 20;
  const valuesPackSize = totalMessages >= 10000 ? 18 : totalMessages >= 2000 ? 14 : 10;
  const valuesMustIncludeIds = new Set(
    valuesSeeking
      .filter((r) => hasPainCue(r.text) || hasSeekCue(r.text))
      .map((r) => r.message_id),
  );
  const topicsPack = await selectWithRecencyBias(topics, topicsPackSize, {
    scoreFn: scoreTopicItem,
  });
  const valuesPack = await selectWithRecencyBias(valuesSeeking, valuesPackSize, {
    scoreFn: scoreValuesItem,
    mustIncludeIds: valuesMustIncludeIds,
  });
  const eventsPack = await selectWithRecencyBias(eventsAff, 10);

  // Current role/company hints from recent evidence
  const recentRole = roleCompany
    .filter((r) => (Date.now() - new Date(r.sent_at).getTime()) <= 365 * 24 * 60 * 60 * 1000)
    .sort((a, b) => scoreRoleCompany(b) - scoreRoleCompany(a))
    .slice(0, 3);
  const currentOrg = recentRole.find((r) => (r.extracted_orgs?.length ?? 0) > 0)?.extracted_orgs?.[0] ?? null;
  const currentRole = recentRole.find((r) => (r.extracted_roles?.length ?? 0) > 0)?.extracted_roles?.[0] ?? null;
  const currentIds = recentRole.map((r) => r.message_id);

  // Bio vs message conflict detection (orgs)
  const bioOrgs = user.bio ? extractOrgsFromBio(user.bio) : [];
  const msgOrgs = uniqueOrgs(roleCompany.flatMap((r) => r.extracted_orgs ?? []).map((org) => cleanOrg(org)).filter(Boolean));
  const normalizedBio = new Set(bioOrgs.map((org) => normalizeOrg(org)).filter(Boolean));
  const normalizedMsg = new Set(msgOrgs.map((org) => normalizeOrg(org)).filter(Boolean));
  const overlaps = [...normalizedBio].some((org) => normalizedMsg.has(org));
  const conflicts = (bioOrgs.length > 0 && msgOrgs.length > 0 && !overlaps)
    ? [{ type: 'role_company', bio_orgs: bioOrgs, message_orgs: msgOrgs }]
    : [];

  const summary = (items: EvidenceItem[]) => {
    if (items.length === 0) return { count: 0, recent_share_12m: 0, min_date: null, max_date: null };
    const now = Date.now();
    const recent = items.filter((i) => (now - new Date(i.sent_at).getTime()) <= 365 * 24 * 60 * 60 * 1000).length;
    const dates = items.map((i) => new Date(i.sent_at).toISOString().slice(0, 10)).sort();
    return {
      count: items.length,
      recent_share_12m: Number((recent / items.length).toFixed(2)),
      min_date: dates[0] ?? null,
      max_date: dates[dates.length - 1] ?? null,
    };
  };

  return {
    user: {
      user_id: user.id,
      handle: user.handle,
      display_name: user.display_name,
      bio: user.bio,
      bio_source: user.bio_source,
      bio_updated_at: user.bio_updated_at,
    },
    stats: {
      total_messages: totalMessages,
      avg_msg_length: avgMsgLen,
      total_reactions: totalReactions,
      total_replies_received: totalReplies,
      engagement_rate: engagementRate,
      last_active_days: lastActiveDays,
      peak_hours: peakHours,
      active_days: activeDays,
      top_groups: topGroups.map((g) => ({ title: g.title, kind: g.kind, msg_count: parseInt(g.msg_count, 10) })),
      top_conversation_partners: partnerRows.map((r) => ({
        handle: r.handle || r.display_name || `user_${r.partner_id}`,
        display_name: r.display_name || r.handle || `User ${r.partner_id}`,
        replies_sent: parseInt(r.replies_sent, 10),
        replies_received: parseInt(r.replies_received, 10),
      })),
    },
    current_claims: {
      current_role_company: { role: currentRole, org: currentOrg, evidence_message_ids: currentIds },
      conflicts,
    },
    evidence_summary: {
      role_company: summary(roleCompanyPack),
      links: summary(linksPack),
      topics: summary(topicsPack),
      values_seeking: summary(valuesPack),
      events_affiliations: summary(eventsPack),
      bio_org_candidates: bioOrgs,
      role_company_org_scores: buildRoleCompanyOrgScores(roleCompanyPack, bioOrgs),
    },
    packs: {
      role_company: roleCompanyPack,
      links: linksPack,
      topics: topicsPack,
      values_seeking: valuesPack,
      events_affiliations: eventsPack,
    },
  };
}
