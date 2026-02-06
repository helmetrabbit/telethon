/**
 * Keyword dictionaries for bio and message pattern matching.
 *
 * Each entry maps a regex pattern to a role or intent label + weight.
 * Patterns are case-insensitive and matched against plain text.
 *
 * These are intentionally conservative — we'd rather miss a signal
 * than hallucinate one. Weights are additive on top of priors.
 */

import type { Role, Intent, OrgType } from '../config/taxonomies.js';

// ── Types ───────────────────────────────────────────────

export interface KeywordSignal<T extends string> {
  pattern: RegExp;
  label: T;
  weight: number;
  /** Human-readable tag for evidence_ref (e.g. "keyword:ceo") */
  tag: string;
}

// ── Affiliation reject list ────────────────────────────
// Words that should NOT be treated as org names when extracted
// from display-name pipe segments or message affiliation patterns.

export const AFFILIATION_REJECT_SET = new Set([
  // Short stopwords (residual from title stripping)
  'of', 'the', 'and', 'at', 'in', 'for', 'to', 'or', 'by', 'on', 'is', 'it', 'an',
  // Locations / cities / countries
  'dubai', 'singapore', 'london', 'berlin', 'new york', 'nyc', 'hong kong',
  'tokyo', 'seoul', 'paris', 'amsterdam', 'lisbon', 'miami', 'istanbul',
  'bangkok', 'zurich', 'san francisco', 'sf', 'los angeles', 'la',
  'abu dhabi', 'riyadh', 'lagos', 'nairobi', 'sydney', 'mumbai', 'shanghai',
  'usa', 'uk', 'uae', 'eu', 'asia', 'europe', 'apac', 'mena', 'latam',
  // Industries / verticals (not org names)
  'security', 'defi', 'web3', 'crypto', 'blockchain', 'nft', 'gamefi',
  'ai', 'fintech', 'metaverse', 'dao', 'layer 1', 'layer 2', 'l1', 'l2',
  'socialfi', 'rwa', 'infrastructure',
  // Bare titles (without an attached org)
  'ceo', 'cto', 'coo', 'cfo', 'cmo', 'cpo', 'cro', 'cso',
  'bd', 'vp', 'director', 'head', 'lead', 'manager', 'partner',
  'founder', 'co-founder', 'cofounder',
  'dev', 'developer', 'engineer', 'builder', 'hacker',
  'growth', 'sales', 'marketing', 'operations', 'ops',
  'investor', 'vc', 'analyst', 'researcher', 'research',
  'recruiter', 'talent', 'hiring',
  'ambassador', 'kol', 'influencer', 'moderator', 'mod', 'admin',
  'mm', 'community',
]);

/**
 * Additional regex patterns that reject affiliation candidates
 * (event names, conference names, generic descriptors).
 */
export const AFFILIATION_REJECT_PATTERNS: RegExp[] = [
  /\b(blockchain\s+week|summit|conference|hackathon|meetup|forum|expo)\b/i,
  /\b(devconnect|ethcc|token2049|consensus|mainnet|breakpoint)\b/i,
  /^\d+$/,  // pure numbers
];

// ── Org type detection from display-name segments ──────
// When a display-name company segment matches one of these,
// we emit a has_org_type claim instead of (or in addition to) a function role.

export interface OrgTypeSignal {
  pattern: RegExp;
  orgType: OrgType;
  tag: string;
}

export const ORG_TYPE_SIGNALS: OrgTypeSignal[] = [
  { pattern: /\bMM\b/, orgType: 'market_maker', tag: 'org_mm' },
  { pattern: /\b(market\s*mak|liquidity\s*provid)\b/i, orgType: 'market_maker', tag: 'org_mm_name' },
  { pattern: /\b(exchange|CEX|DEX)\b/i, orgType: 'exchange', tag: 'org_exchange' },
  { pattern: /\b(fund|capital|ventures?|vc\b)\b/i, orgType: 'fund', tag: 'org_fund' },
  { pattern: /\b(agency|studio|consultancy)\b/i, orgType: 'agency', tag: 'org_agency' },
  { pattern: /\b(media|news|press|journal)\b/i, orgType: 'media', tag: 'org_media' },
  { pattern: /\b(event|conference|summit|hackathon)\b/i, orgType: 'event', tag: 'org_event' },
  { pattern: /\b(protocol|labs?|network)\b/i, orgType: 'protocol', tag: 'org_protocol' },
];

// ── Bio → Role signals ─────────────────────────────────

export const BIO_ROLE_KEYWORDS: KeywordSignal<Role>[] = [
  // Founder / Executive
  { pattern: /\b(ceo|cto|coo|cfo|co-?founder|founder)\b/i, label: 'founder_exec', weight: 3.0, tag: 'founder_title' },
  { pattern: /\b(chief\s+(executive|technology|operating|financial))\b/i, label: 'founder_exec', weight: 3.0, tag: 'chief_title' },
  { pattern: /\bhead\s+of\b/i, label: 'founder_exec', weight: 1.5, tag: 'head_of' },

  // Builder / Developer — require hard technical signals
  { pattern: /\b(developer|engineer|full[- ]?stack|backend|frontend|solidity)\b/i, label: 'builder', weight: 2.5, tag: 'dev_title' },
  { pattern: /\b(building|shipped|hacking|coding)\b/i, label: 'builder', weight: 1.5, tag: 'builder_verb' },
  { pattern: /\b(rust|typescript|python|golang|smart\s*contract)\b/i, label: 'builder', weight: 1.5, tag: 'tech_skill' },
  // NOTE: bare "dev" removed from bio — too ambiguous (devconnect, devrel, etc.)

  // BD / Business Development
  { pattern: /\b(bd|biz\s*dev|business\s*development|partnerships?)\b/i, label: 'bd', weight: 2.5, tag: 'bd_title' },
  { pattern: /\b(growth|sales\s*lead|account\s*exec)\b/i, label: 'bd', weight: 1.5, tag: 'bd_role' },

  // Investor / Analyst
  { pattern: /\b(investor|vc|venture|fund|analyst|portfolio|lp|gp)\b/i, label: 'investor_analyst', weight: 2.5, tag: 'investor_title' },
  { pattern: /\b(due\s*diligence|deal\s*flow|thesis)\b/i, label: 'investor_analyst', weight: 2.0, tag: 'investor_activity' },

  // Recruiter
  { pattern: /\b(recruiter|recruiting|talent|headhunter|staffing)\b/i, label: 'recruiter', weight: 2.5, tag: 'recruiter_title' },
  { pattern: /\b(hiring\s*manager|we.re\s*hiring|open\s*roles?)\b/i, label: 'recruiter', weight: 1.5, tag: 'hiring_signal' },

  // Vendor / Agency
  { pattern: /\b(agency|consultancy|consulting|vendor|service\s*provider)\b/i, label: 'vendor_agency', weight: 2.5, tag: 'agency_title' },
  { pattern: /\b(white[- ]?label|managed\s*service|outsourc)\b/i, label: 'vendor_agency', weight: 1.5, tag: 'vendor_signal' },

  // Media / KOL — individual only (agencies route to vendor_agency)
  { pattern: /\b(KOL|influencer|ambassador|content\s*creator)\b/i, label: 'media_kol', weight: 2.5, tag: 'kol_title' },
  { pattern: /\b(journalist|editor|press|PR\s*manager)\b/i, label: 'media_kol', weight: 2.0, tag: 'media_title' },
  // NOTE: bare "media" removed — too ambiguous; "promotion|campaign|social media" moved to vendor detection below

  // NOTE: market_maker removed from role keywords — it's org-type only (has_org_type).
  // People AT market-maker firms get function roles (bd, builder, etc.) + has_org_type=market_maker.
];

// ── Message → Role signals (aggregate patterns) ────────

export const MSG_ROLE_KEYWORDS: KeywordSignal<Role>[] = [
  // Builder — hard technical signals only
  { pattern: /\b(shipped|deployed|merged|refactored|committed|pushed)\b/i, label: 'builder', weight: 1.0, tag: 'builder_action' },
  { pattern: /\b(smart\s*contract|solidity|rust|typescript|API|SDK|RPC|repo|github)\b/i, label: 'builder', weight: 1.0, tag: 'builder_tech' },
  // NOTE: removed "built", "launched" (too generic), "defi"/"tvl"/"protocol" (topic not role),
  //       "mainnet"/"testnet" (everyone discusses these)

  // BD
  { pattern: /\b(partnership|collab|intro\s+to|warm\s+intro|deal)\b/i, label: 'bd', weight: 1.0, tag: 'bd_action' },

  // Investor
  { pattern: /\b(series\s*[a-d]|raise|fundrais|invest|portfolio)\b/i, label: 'investor_analyst', weight: 1.0, tag: 'investor_topic' },
  { pattern: /\b(evaluating|due\s*diligence|thesis)\b/i, label: 'investor_analyst', weight: 1.0, tag: 'investor_action' },

  // Recruiter
  { pattern: /\b(hiring|recruit|talent|open\s*role|job\s*posting)\b/i, label: 'recruiter', weight: 1.0, tag: 'recruiter_action' },

  // NOTE: media_kol removed from msg role signals — discussing/selling KOL services doesn't
  //       make someone a KOL. Erhan (EAK agency) mentions "KOL" 7x but is a vendor, not a KOL.
  //       media_kol should only fire from display_name/bio self-identification.

  // NOTE: market_maker removed from msg role signals — discussing liquidity/spreads doesn't
  //       make someone a market maker. Causes false positives (Rhythm, Marcelo).
  //       Market maker is org-type only (has_org_type).

  // Vendor / Agency — detect selling-services patterns in messages (fix #4)
  { pattern: /\b(we\s+speciali[sz]e|our\s+services?|our\s+solutions?|our\s+agency)\b/i, label: 'vendor_agency', weight: 1.2, tag: 'vendor_service_msg' },
  { pattern: /\b(marketing\s+(?:solutions?|services?|packages?|agency)|PR\s+(?:services?|agency))\b/i, label: 'vendor_agency', weight: 1.2, tag: 'vendor_marketing_msg' },
  { pattern: /\b(white[- ]?label|managed\s+service|full[- ]?service)\b/i, label: 'vendor_agency', weight: 1.0, tag: 'vendor_whitelabel_msg' },
  // KOL/influencer agency signals — selling KOL services = vendor, not media_kol
  { pattern: /\b(KOL\s+(?:agency|network|campaign|services?)|influencer\s+(?:agency|network|campaign))\b/i, label: 'vendor_agency', weight: 1.5, tag: 'vendor_kol_agency_msg' },
  { pattern: /\b(tier\s*[12]\s+KOLs?|contact\s+me\s+for\s+costs?|drop\s+me\s+a\s+DM)\b/i, label: 'vendor_agency', weight: 1.0, tag: 'vendor_kol_selling_msg' },
];

// ── Bio → Intent signals ───────────────────────────────

export const BIO_INTENT_KEYWORDS: KeywordSignal<Intent>[] = [
  { pattern: /\b(connect|network|meet|intro)\b/i, label: 'networking', weight: 1.5, tag: 'networking_bio' },
  { pattern: /\b(evaluat|assess|review|analyz)\b/i, label: 'evaluating', weight: 1.5, tag: 'evaluating_bio' },
  { pattern: /\b(sell|offer|demo|pitch)\b/i, label: 'selling', weight: 1.5, tag: 'selling_bio' },
  { pattern: /\b(hir|recruit|talent)\b/i, label: 'hiring', weight: 1.5, tag: 'hiring_bio' },
  { pattern: /\b(help|support|assist|mentor)\b/i, label: 'support_giving', weight: 1.5, tag: 'support_giving_bio' },
];

// ── Message → Intent signals ───────────────────────────

export const MSG_INTENT_KEYWORDS: KeywordSignal<Intent>[] = [
  { pattern: /\b(connect|intro|meet\s*up|let'?s\s*chat)\b/i, label: 'networking', weight: 0.8, tag: 'networking_msg' },
  // NOTE: removed broad 'review' and 'look into' — fires on broadcasting/showcase messages.
  //       Reserved for explicit vendor/tool comparison and procurement language.
  { pattern: /\b(evaluat|assess|compar(?:e|ing)|shortlist|considering\s+vendors?|RFP|POC|pilot\s+(?:test|program))\b/i, label: 'evaluating', weight: 0.8, tag: 'evaluating_msg' },

  // Selling — strengthened (fix #5)
  { pattern: /\b(sell|pitch|demo|pricing|quote)\b/i, label: 'selling', weight: 0.8, tag: 'selling_msg' },
  { pattern: /\b(discount|offer|promo\b|special\s+price|packages?\s+(?:start|from))\b/i, label: 'selling', weight: 1.0, tag: 'selling_discount_msg' },
  { pattern: /\b(our\s+(?:services?|solutions?|platform|tool)|we\s+(?:offer|provide|deliver))\b/i, label: 'selling', weight: 1.0, tag: 'selling_services_msg' },
  { pattern: /\b(DM\s+(?:me|us)\s+for|reach\s+out\s+for|contact\s+(?:me|us)\s+for)\b/i, label: 'selling', weight: 0.8, tag: 'selling_cta_msg' },

  // Hiring
  { pattern: /\b(hiring|recruit|job|open\s*role|we.re\s*looking)\b/i, label: 'hiring', weight: 0.8, tag: 'hiring_msg' },

  // Support
  { pattern: /\b(help|stuck|issue|bug|problem|how\s*do\s*i)\b/i, label: 'support_seeking', weight: 0.8, tag: 'support_seeking_msg' },
  { pattern: /\b(try\s*this|here.s\s*how|you\s*can|solution|fix)\b/i, label: 'support_giving', weight: 0.8, tag: 'support_giving_msg' },

  // Broadcasting — strengthened (fix #5)
  { pattern: /\b(announce|update|release|congrat)\b/i, label: 'broadcasting', weight: 0.8, tag: 'broadcasting_msg' },
  { pattern: /\b(webinar|live\s+session|ama\b|spaces\b|twitter\s+spaces)\b/i, label: 'broadcasting', weight: 1.0, tag: 'broadcasting_event_msg' },
  { pattern: /\b(we\s+(?:launched|shipped|released|just\s+dropped)|check\s+(?:out|it\s+out))\b/i, label: 'broadcasting', weight: 1.0, tag: 'broadcasting_launch_msg' },
  { pattern: /(?:luma\.com|lu\.ma|eventbrite|meetup\.com)\b/i, label: 'broadcasting', weight: 1.2, tag: 'broadcasting_link_msg' },
  { pattern: /\b(reminder|don'?t\s+miss|register\s+(?:now|here|today)|sign\s+up\s+(?:here|now))\b/i, label: 'broadcasting', weight: 1.0, tag: 'broadcasting_reminder_msg' },

  // Evaluating
  { pattern: /\b(schedule|calendar|call|meeting|calendly)\b/i, label: 'evaluating', weight: 0.6, tag: 'evaluating_schedule' },
  { pattern: /\b(investment|fund|back|series)\b/i, label: 'evaluating', weight: 0.6, tag: 'evaluating_investment' },
];

// ── Affiliation signals ────────────────────────────────

export interface AffiliationSignal {
  pattern: RegExp;
  tag: string;
}

/**
 * Detect self-declared affiliations in bios.
 * These capture "at Company" or "@ Company" or "Company CEO" patterns.
 * We return the matched text, NOT a guessed entity.
 */
export const BIO_AFFILIATION_PATTERNS: AffiliationSignal[] = [
  { pattern: /\b(?:at|@)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)*)\b/, tag: 'affiliation_at' },
  { pattern: /\b([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)*)\s+(?:CEO|CTO|COO|CFO|founder|co-?founder)\b/i, tag: 'affiliation_title' },
];

/**
 * Detect self-declared affiliations in messages (fix #1).
 * Patterns like "we at X", "I'm from X", "I work at X".
 */
export const MSG_AFFILIATION_PATTERNS: AffiliationSignal[] = [
  { pattern: /\b(?:we\s+at|I(?:'m|\s+am)\s+(?:from|at|with)|I\s+work\s+(?:at|for)|representing)\s+([A-Z][A-Za-z0-9.]+(?:\s+[A-Z][A-Za-z0-9.]+){0,3})\b/, tag: 'msg_affiliation_at' },
  { pattern: /\b(?:our\s+(?:company|team|project|protocol))\s+([A-Z][A-Za-z0-9.]+(?:\s+[A-Za-z0-9.]+){0,3})\b/, tag: 'msg_affiliation_our' },
  { pattern: /\bon\s+behalf\s+of\s+([A-Z][A-Za-z0-9.]+(?:\s+[A-Za-z0-9.]+){0,3})\b/, tag: 'msg_affiliation_behalf' },
  // "[name] from [Org]" / "here from the [Org] team" — common self-intro in groups
  { pattern: /\b(?:here\s+)?from\s+(?:the\s+)?([A-Z][A-Za-z0-9./]+(?:\s+[A-Za-z0-9./]+){0,4})\s+team\b/i, tag: 'msg_affiliation_intro' },
];

// ── Display Name → Role signals ────────────────────────
// Telegram users commonly set display names like "Alice | Acme Labs BD Lead"
// These carry strong role and affiliation evidence.

export const DISPLAY_NAME_ROLE_KEYWORDS: KeywordSignal<Role>[] = [
  // Founder / Executive
  { pattern: /\b(CEO|CTO|COO|CFO|CMO|co-?founder|founder)\b/i, label: 'founder_exec', weight: 3.5, tag: 'dn_founder_title' },
  { pattern: /\b(head\s+of|director|VP|managing\s+partner)\b/i, label: 'founder_exec', weight: 2.5, tag: 'dn_exec_title' },

  // BD / Business Development
  { pattern: /\bBD\b/i, label: 'bd', weight: 3.0, tag: 'dn_bd' },
  { pattern: /\b(biz\s*dev|business\s*develop|partnerships?\s*(?:manager|lead|director)?)\b/i, label: 'bd', weight: 3.0, tag: 'dn_bd_title' },
  { pattern: /\b(growth|sales)\b/i, label: 'bd', weight: 2.0, tag: 'dn_bd_role' },

  // Builder / Developer — require explicit dev titles only (fix #3)
  { pattern: /\b(developer|engineer|full[- ]?stack|backend|frontend|solidity)\b/i, label: 'builder', weight: 3.0, tag: 'dn_dev_title' },
  // NOTE: removed loose "builder|building|hacker" — too ambiguous in crypto display names

  // Investor / Analyst
  { pattern: /\b(investor|vc\b|venture|capital|fund|analyst|portfolio)\b/i, label: 'investor_analyst', weight: 3.0, tag: 'dn_investor_title' },

  // Recruiter
  { pattern: /\b(recruiter|recruiting|talent|headhunter|staffing|hiring)\b/i, label: 'recruiter', weight: 3.0, tag: 'dn_recruiter_title' },

  // Vendor / Agency
  { pattern: /\b(agency|consulting|consultant)\b/i, label: 'vendor_agency', weight: 2.5, tag: 'dn_agency' },
  // Marketing in display name → vendor_agency (not media_kol) — fix #4
  { pattern: /\b(marketing)\b/i, label: 'vendor_agency', weight: 2.0, tag: 'dn_marketing_vendor' },

  // Media / KOL — individual-only keywords (fix #4)
  { pattern: /\bKOL\b/i, label: 'media_kol', weight: 3.0, tag: 'dn_kol' },
  { pattern: /\b(influencer|ambassador|content\s*creat)\b/i, label: 'media_kol', weight: 3.0, tag: 'dn_media_title' },
  // NOTE: removed "marketing|PR|press" → media_kol. Marketing routes to vendor_agency.
  //       Individual journalists still caught by content_creat / KOL / influencer.
  { pattern: /\b(journalist|editor)\b/i, label: 'media_kol', weight: 2.5, tag: 'dn_journalist' },

  // NOTE: market_maker removed from display name role keywords entirely.
  //       "MM" and "market maker" in display names are org-type descriptors
  //       handled by ORG_TYPE_SIGNALS → has_org_type=market_maker.

  // Community
  { pattern: /\b(community|moderator|mod\b|admin)\b/i, label: 'community', weight: 2.5, tag: 'dn_community' },

  // Security / Audit → NOT mapped to builder (fix #3)
  // "Hashlock Security" means the user is AT a security firm, not that they're a builder.
  // Security in display name is treated as an org descriptor, not a role signal.
  // (removed: was { label: 'builder', weight: 2.0, tag: 'dn_security' })

  // Research → mapped to investor_analyst
  { pattern: /\b(research|researcher)\b/i, label: 'investor_analyst', weight: 2.0, tag: 'dn_researcher' },
];

// ── Display Name → Affiliation signals ─────────────────
// Parse "Name | Company" and "Name | Company Role" pipe patterns.
// The engine applies AFFILIATION_REJECT_SET filtering after extraction.

export const DISPLAY_NAME_AFFILIATION_PATTERNS: AffiliationSignal[] = [
  // "Alice | Acme Labs" or "Alice | Acme Labs BD Lead"
  // Captures the company segment between the pipe and either end-of-string or a known role word.
  // Title prefixes (CEO, CTO, etc.) at the START of the segment are stripped by the engine.
  { pattern: /\|\s*([A-Z][A-Za-z0-9.]+(?:\s+[A-Z][A-Za-z0-9.]+){0,4}?)(?:\s+(?:CEO|CTO|COO|CFO|CMO|co-?founder|founder|BD|head|director|VP|lead|manager|engineer|dev|developer|builder|investor|vc|recruiter|growth|sales|marketing|KOL|ambassador|MM|community|mod|admin|research|analyst|operations|ops)\b|$)/i, tag: 'dn_affiliation_pipe' },
  // "Role @ Company" or "Role at Company"
  { pattern: /(?:@|at)\s+([A-Z][A-Za-z0-9.]+(?:\s+[A-Z][A-Za-z0-9.]+){0,3})/i, tag: 'dn_affiliation_at' },
];
