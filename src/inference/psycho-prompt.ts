/**
 * Evidence-grounded prompt for psychographic profiling.
 * Uses evidence bundles to avoid slop and outdated labeling.
 */

import type { EvidenceBundle } from '../analysis/evidence-bundle.js';

// ── Valid enum values (enforced in prompt + parsing) ─────

export const TONE_VALUES = ['formal', 'casual', 'blunt', 'diplomatic', 'enthusiastic', 'dry'] as const;
export type Tone = (typeof TONE_VALUES)[number];

export const PROFESSIONALISM_VALUES = ['corporate', 'professional', 'relaxed', 'street'] as const;
export type Professionalism = (typeof PROFESSIONALISM_VALUES)[number];

export const VERBOSITY_VALUES = ['terse', 'concise', 'moderate', 'verbose', 'walls_of_text'] as const;
export type Verbosity = (typeof VERBOSITY_VALUES)[number];

export const RESPONSIVENESS_VALUES = ['fast_responder', 'deliberate', 'sporadic', 'lurker'] as const;
export type Responsiveness = (typeof RESPONSIVENESS_VALUES)[number];

export const DECISION_STYLE_VALUES = ['data_driven', 'relationship_driven', 'authority_driven', 'consensus_seeker'] as const;
export type DecisionStyle = (typeof DECISION_STYLE_VALUES)[number];

export const SENIORITY_VALUES = ['junior', 'mid', 'senior', 'executive', 'unclear'] as const;
export type SenioritySignal = (typeof SENIORITY_VALUES)[number];

export const COMMERCIAL_ARCHETYPE_VALUES = [
  'founder', 'investor', 'bd_sales', 'developer', 'trader', 'community_manager', 'shiller',
  'researcher', 'analyst', 'ecosystem_lead', 'unclear'
] as const;
export type CommercialArchetype = (typeof COMMERCIAL_ARCHETYPE_VALUES)[number];

// ── Output shape ────────────────────────────────────────

export interface EvidenceItem {
  value: string;
  quote: string;
  date?: string;
}

export interface PsychographicProfile {
  tone?: string;
  professionalism?: string;
  verbosity?: string;
  responsiveness?: string;
  decision_style?: string;
  seniority_signal?: string;
  commercial_archetype?: string;
  approachability?: number;
  quirks?: EvidenceItem[];
  notable_topics?: EvidenceItem[];
  pain_points?: EvidenceItem[];
  crypto_values?: EvidenceItem[];
  connection_requests?: EvidenceItem[];
  fingerprint_tags?: string[];
  based_in?: string;
  attended_events?: string[];
  preferred_contact_style?: string;
  reasoning?: string;

  generated_bio_professional?: string;
  generated_bio_personal?: string;
  primary_role?: string;
  primary_company?: string;
  deep_skills?: string[];
  affiliations?: string[];
  social_platforms?: string[];
  social_urls?: string[];
  buying_power?: string;
  languages?: string[];
  scam_risk_score?: number;
  confidence_score?: number;
  career_stage?: string;
  tribe_affiliations?: string[];
  reputation_score?: number;
  driving_values?: string[];
  technical_specifics?: string[];
  business_focus?: string[];

  // Computed (non-LLM) fields
  fifo?: string;
  group_tags?: string[];
  reputation_summary?: string;
  total_msgs?: number;
  avg_msg_length?: number;
  peak_hours?: number[];
  active_days?: string[];
  last_active_days?: number;
  total_reactions?: number;
  avg_reactions_per_msg?: number;
  total_replies_received?: number;
  avg_replies_per_msg?: number;
  engagement_rate?: number;
  top_conversation_partners?: { handle: string; display_name: string; replies_sent: number; replies_received: number }[];

  // Current/previous role/company resolution
  current_role_company?: { role: string | null; org: string | null; confidence: number; evidence_message_ids: number[] };
  previous_roles_companies?: { role: string | null; org: string | null; end_hint?: string | null; confidence: number; evidence_message_ids: number[] }[];
  role_company_timeline?: {
    org: string | null;
    role: string | null;
    start_hint?: string | null;
    end_hint?: string | null;
    is_current: boolean;
    evidence_message_ids: number[];
    confidence: number;
  }[];

  field_confidence?: {
    current_role_company?: number;
    generated_bio_professional?: number;
    generated_bio_personal?: number;
    commercial_archetype?: number;
    based_in?: number;
  };
}

// ── Prompt builder ──────────────────────────────────────

export const PROMPT_VERSION = 'v2.5';

export function buildPsychoPrompt(bundle: EvidenceBundle): string {
  const richTargets = getRichTargets(bundle.stats.total_messages);
  const groupList = bundle.stats.top_groups
    .map((g) => `  - ${g.title} (${g.kind}, ${g.msg_count} msgs)`)
    .join('\n');

  const packToText = (label: string, items: any[]) => {
    const lines = items.map((m) => {
      const urls = (m.urls && m.urls.length > 0) ? ` urls=${m.urls.join(', ')}` : '';
      const sig = (m.signal_types && m.signal_types.length > 0) ? ` signals=${m.signal_types.join(',')}` : '';
      const orgStrict = (m.org_candidates_strict && m.org_candidates_strict.length > 0) ? ` org_strict=${m.org_candidates_strict.join(', ')}` : '';
      const orgUrl = (m.org_candidates_url_anchored && m.org_candidates_url_anchored.length > 0) ? ` org_url=${m.org_candidates_url_anchored.join(', ')}` : '';
      const orgLoose = (m.org_candidates_loose && m.org_candidates_loose.length > 0) ? ` org_loose=${m.org_candidates_loose.join(', ')}` : '';
      const ctxParent = m.context_parent ? ` parent_context="${m.context_parent}"` : '';
      const ctxNear = m.context_nearby && m.context_nearby.length > 0 ? ` nearby_context="${m.context_nearby.join(' | ')}"` : '';
      return `  - [${m.sent_at}] ${m.group_title ? `[in ${m.group_title}] ` : ''}${m.short_text}${urls}${sig}${orgStrict}${orgUrl}${orgLoose}${ctxParent}${ctxNear} (id=${m.message_id})`;
    });
    return `${label}:\n${lines.length > 0 ? lines.join('\n') : '  (none)'}`;
  };

  const conflicts = bundle.current_claims.conflicts.length > 0
    ? bundle.current_claims.conflicts.map((c) => `  - ${c.type}: bio=${c.bio_orgs.join(', ')} vs msgs=${c.message_orgs.join(', ')}`).join('\n')
    : '  (none)';

  return `You are a cynical, high-precision communication analyst. Your job is to extract highly specific behavioral data.

## Prompt Version
- psycho_prompt_version: ${PROMPT_VERSION}

### 🚫 STRICT PROHIBITIONS (ANTI-SLOP)
- DO NOT say "Uses emojis" → Say WHICH emoji ("Overuses 🔥", "Ends sentences with 🗿").
- DO NOT say "Friendly tone" → Say "Overly apologetic" or "Fake enthusiastic".
- DO NOT say "Uses slang" → Quote the EXACT slang ("Uses 'gm' unironically", "Says 'based'").
- DO NOT give generic advice like "Be polite" → Give a SPECIFIC opening line.
- DO NOT guess location from event attendance (BDs travel constantly). Only set 'based_in' if they explicitly say "I live in X" or "My office in Y".
- DO NOT dump lists of tokens or tickers (BTC, ETH, SOL) into 'tribe_affiliations' or 'driving_values'. Only tag the *concept* (e.g. 'Bitcoin Maxi' instead of 'BTC').
- DO NOT use boilerplate bio phrases like "blockchain enthusiast", "community builder", "always on the lookout", "passionate about", "next big thing", "driving engagement", "fostering connections".
- If the user is generic/boring, return empty arrays. Do not hallucinate interesting traits.

## Current Snapshot (Highest Priority)
- Handle: ${bundle.user.handle || '(none)'}
- Display Name: ${bundle.user.display_name || '(none)'}
- Bio: ${bundle.user.bio || '(none)'}
- Bio Source: ${bundle.user.bio_source || '(unknown)'}
- Bio Updated At: ${bundle.user.bio_updated_at || '(unknown)'}

## Deterministic Stats (Do NOT waste tokens on gm/emoji noise)
- Total Messages: ${bundle.stats.total_messages}
- Avg Msg Length: ${bundle.stats.avg_msg_length}
- Total Reactions: ${bundle.stats.total_reactions}
- Total Replies Received: ${bundle.stats.total_replies_received}
- Engagement Rate: ${bundle.stats.engagement_rate}
- Last Active Days: ${bundle.stats.last_active_days ?? 'unknown'}
- Peak Hours (UTC): ${bundle.stats.peak_hours.join(', ') || '(none)'}
- Active Days: ${bundle.stats.active_days.join(', ') || '(none)'}
- Top Groups:
${groupList || '  (none)'}

## Current Role/Company Hints (from evidence)
- role=${bundle.current_claims.current_role_company.role ?? 'unknown'}
- org=${bundle.current_claims.current_role_company.org ?? 'unknown'}
- evidence_message_ids=${bundle.current_claims.current_role_company.evidence_message_ids.join(', ') || '(none)'}

## Conflicts Detected
${conflicts}

## Evidence Packs (capped, high-signal)
${packToText('ROLE_COMPANY_EVIDENCE', bundle.packs.role_company)}
${packToText('LINKS_EVIDENCE', bundle.packs.links)}
${packToText('TOPICS_EVIDENCE', bundle.packs.topics)}
${packToText('VALUES/SEEKING', bundle.packs.values_seeking)}
${packToText('EVENTS/AFFILIATIONS', bundle.packs.events_affiliations)}

## Richness Signal
- total_messages=${bundle.stats.total_messages}
- role_company_count=${bundle.packs.role_company.length}
- topics_count=${bundle.packs.topics.length}
- values_count=${bundle.packs.values_seeking.length}
- events_count=${bundle.packs.events_affiliations.length}
- rich_profile_expected=${bundle.stats.total_messages >= 40 ? 'yes' : 'no'}
- rich_targets=${JSON.stringify(richTargets)}

## Task
Analyze this person's communication style, location signals, and personality quirks. Produce a JSON object with EXACTLY these fields:

{
  "tone": "one of: formal, casual, blunt, diplomatic, enthusiastic, dry",
  "professionalism": "one of: corporate, professional, relaxed, street",
  "verbosity": "one of: terse, concise, moderate, verbose, walls_of_text",
  "responsiveness": "one of: fast_responder, deliberate, sporadic, lurker",
  "decision_style": "one of: data_driven, relationship_driven, authority_driven, consensus_seeker",
  "seniority_signal": "one of: junior, mid, senior, executive, unclear",
  "commercial_archetype": "one of: founder, investor, bd_sales, developer, trader, community_manager, shiller, researcher, analyst, ecosystem_lead, unclear",
  "approachability": 0.0 to 1.0 (float),

  "generated_bio_professional": "A sharp, 1-sentence LinkedIn headline summarizing their professional identity (<=80 words).",
  "generated_bio_personal": "A casual, 1-sentence Twitter bio capturing their vibe (<=80 words).",
  "primary_role": "Specific Job Title (e.g. 'Head of Growth', 'Solidity Dev', 'Founder'). Return null if unclear.",
  "primary_company": "Main Company/Project they work for (e.g. 'Consensys', 'Solana Foundation'). Return null if unclear.",
  "current_role_company": { "role": "string or null", "org": "string or null", "confidence": 0.0, "evidence_message_ids": [1,2] },
  "previous_roles_companies": [ { "role": "string or null", "org": "string or null", "end_hint": "string or null", "confidence": 0.0, "evidence_message_ids": [1,2] } ],
  "role_company_timeline": [ { "org": "string or null", "role": "string or null", "start_hint": "string or null", "end_hint": "string or null", "is_current": true, "evidence_message_ids": [1,2], "confidence": 0.0 } ],
  "deep_skills": ["Specific technical or domain skills inferred from jargon. Max 8 items."],
  "technical_specifics": ["Specific tools/protocols/frameworks mentioned. Max 15 items."],
  "business_focus": ["High-level sector focus. Max 5 items."],
  "affiliations": ["Associated projects/DAOs. Max 10 items."],
  "tribe_affiliations": ["Cultural tribes/subcultures. Max 8 items."],
  "driving_values": ["Core motivations. Max 8 items."],
  "languages": ["Detected languages."],
  "social_platforms": ["Platform names ONLY. Max 10 items."],
  "social_urls": ["Full social URLs/handles. Max 10 items."],
  "buying_power": "Estimate: 'High', 'Medium', 'Low', 'Unknown'",
  "career_stage": "one of: Junior, Mid-Level, Senior, Executive, Founder, Retired, Student",
  "scam_risk_score": 0 to 100 (Integer),
  "confidence_score": 0.0 to 1.0 (Float),

  "quirks": [ { "value": "Deep behavioral/intellectual quirk", "quote": "context", "date": "YYYY-MM-DD" } ],
  "notable_topics": [ { "value": "Specific interest", "quote": "context", "date": "YYYY-MM-DD" } ],
  "pain_points": [ { "value": "Specific complaint/problem", "quote": "context", "date": "YYYY-MM-DD" } ],
  "crypto_values": [ { "value": "Value system signal", "quote": "context", "date": "YYYY-MM-DD" } ],
  "connection_requests": [ { "value": "Person/Company they are trying to reach", "quote": "context", "date": "YYYY-MM-DD" } ],
  "fingerprint_tags": [ "Keywords: events, people, places, topics, companies. Max 15 items." ],
  "based_in": "Format: 'City, Country (Source: [Evidence])'. Return null if uncertain.",
  "attended_events": ["Specific conferences"],
  "preferred_contact_style": "Psychological analysis of how THEY interact.",
  "reasoning": "Brief justification",
  "field_confidence": { "current_role_company": 0.0, "generated_bio_professional": 0.0, "generated_bio_personal": 0.0, "commercial_archetype": 0.0, "based_in": 0.0 }
}

IMPORTANT:
- Prefer bio for current role/company IF bio is recent; otherwise weigh message evidence recency.
- If bio conflicts with message evidence, mention both and lower confidence.
- Never invent employers; if uncertain, use null/unknown and cite competing evidence.
- For role/company claims, treat org evidence strength as: org_strict > org_url > org_loose.
- For 'quirks' and 'notable_topics', you MUST extract the 'quote' and the 'date' from the message logs provided.
- Do not hallucinate quotes or dates. If evidence is missing, do not include the item.
- Keep arrays short and dedup strings.
- If rich_profile_expected=yes:
  - Each generated bio sentence MUST include at least one concrete org/project/person from evidence.
  - Include at least ${richTargets.min_supported_evidence_items} total entries across quirks/notable_topics/pain_points/crypto_values/connection_requests with both quote and YYYY-MM-DD date.
  - Include >=${richTargets.min_topics} notable_topics if evidence exists.
  - Include >=${richTargets.min_quirks} quirks if stylistic evidence exists.
  - Include >=${richTargets.min_pain_points} pain_points if complaint/problem evidence exists.
  - Include >=${richTargets.min_driving_values} driving_values if value/opinion evidence exists.
  - Include >=${richTargets.min_connection_requests} connection_requests if seeking/contact evidence exists.
  - Include >=${richTargets.min_deep_skills} deep_skills if technical evidence exists.
  - Include >=${richTargets.min_technical_specifics} technical_specifics if protocol/tool/link evidence exists.
  - Include >=${richTargets.min_affiliations} affiliations if multiple org candidates exist.
  - Include >=${richTargets.min_fingerprint_tags} fingerprint_tags; use specific entities (people/projects/events), not generic words.
  - preferred_contact_style must describe concrete interaction behavior, not adjectives.
`;
}

function getRichTargets(totalMessages: number): {
  min_supported_evidence_items: number;
  min_topics: number;
  min_quirks: number;
  min_pain_points: number;
  min_driving_values: number;
  min_connection_requests: number;
  min_deep_skills: number;
  min_technical_specifics: number;
  min_affiliations: number;
  min_fingerprint_tags: number;
} {
  if (totalMessages >= 10000) {
    return {
      min_supported_evidence_items: 6,
      min_topics: 6,
      min_quirks: 5,
      min_pain_points: 2,
      min_driving_values: 4,
      min_connection_requests: 2,
      min_deep_skills: 6,
      min_technical_specifics: 10,
      min_affiliations: 8,
      min_fingerprint_tags: 12,
    };
  }
  if (totalMessages >= 5000) {
    return {
      min_supported_evidence_items: 7,
      min_topics: 5,
      min_quirks: 3,
      min_pain_points: 1,
      min_driving_values: 3,
      min_connection_requests: 1,
      min_deep_skills: 4,
      min_technical_specifics: 8,
      min_affiliations: 6,
      min_fingerprint_tags: 10,
    };
  }
  if (totalMessages >= 2000) {
    return {
      min_supported_evidence_items: 5,
      min_topics: 4,
      min_quirks: 2,
      min_pain_points: 1,
      min_driving_values: 2,
      min_connection_requests: 1,
      min_deep_skills: 3,
      min_technical_specifics: 6,
      min_affiliations: 4,
      min_fingerprint_tags: 8,
    };
  }
  return {
    min_supported_evidence_items: 3,
    min_topics: 2,
    min_quirks: 2,
    min_pain_points: 1,
    min_driving_values: 2,
    min_connection_requests: 1,
    min_deep_skills: 2,
    min_technical_specifics: 4,
    min_affiliations: 3,
    min_fingerprint_tags: 6,
  };
}
