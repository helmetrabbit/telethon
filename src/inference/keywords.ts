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
  // Fix v0.5.7: Bare "MM" removed — too ambiguous (catches "MM DOOM", initials, etc.)
  //             Now requires: org context ("CompanyName MM") or longform "market maker/making".
  { pattern: /(?<=[A-Z][a-z]+\s)MM\b/, orgType: 'market_maker', tag: 'org_mm' },
  { pattern: /\b(market\s*mak(?:er|ing)|liquidity\s*provid)\b/i, orgType: 'market_maker', tag: 'org_mm_name' },
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
  // Security/audit firms (fix v0.5.4 #1) — route to vendor_agency, not builder
  { pattern: /\b(security\s+(?:company|firm|audit)|audit(?:ing)?\s+(?:firm|company|services?))\b/i, label: 'vendor_agency', weight: 2.5, tag: 'security_vendor_bio' },

  // Media / KOL — individual only (agencies route to vendor_agency)
  { pattern: /\b(KOL|influencer|ambassador|content\s*creator)\b/i, label: 'media_kol', weight: 2.5, tag: 'kol_title' },
  { pattern: /\b(journalist|editor|press|PR\s*manager)\b/i, label: 'media_kol', weight: 2.0, tag: 'media_title' },
  // NOTE: bare "media" removed — too ambiguous; "promotion|campaign|social media" moved to vendor detection below

  // NOTE: market_maker removed from role keywords — it's org-type only (has_org_type).
  // People AT market-maker firms get function roles (bd, builder, etc.) + has_org_type=market_maker.
];

// ── Message → Role signals (aggregate patterns) ────────

export const MSG_ROLE_KEYWORDS: KeywordSignal<Role>[] = [
  // Builder — hard technical signals only (fix v0.5.4 #2: tightened)
  // Action verbs require first-person subject — "I/we shipped" not "they pushed"
  // Fix v0.5.7: Added first-person constraint to avoid 3rd-party statements.
  { pattern: /\b(?:I|we)\s+(?:shipped|deployed|merged|refactored|committed|pushed)\b/i, label: 'builder', weight: 1.0, tag: 'builder_action' },
  // Tech keywords: require HARD engineering context, not sales-pitch "API integration"
  // Removed: API, SDK (too generic — salespeople discuss these)
  // Kept: low-level tech signals that indicate actual engineering work
  // Fix v0.5.7: "PR" now requires "#" (PR #123) or explicit "pull request";
  //             "rust" requires word boundary (\brust\b) to avoid matching "trust".
  //             Bare "PR" removed — too ambiguous (public relations vs pull request).
  { pattern: /\b(smart\s*contract|solidity|\brust\b|typescript|RPC|repo|github|PR\s*#\d+|pull\s+request|commit|branch|bug\s*fix|stack\s*trace|error\s*log)\b/i, label: 'builder', weight: 1.0, tag: 'builder_tech' },
  // NOTE: removed "built", "launched" (too generic), "defi"/"tvl"/"protocol" (topic not role),
  //       "mainnet"/"testnet" (everyone discusses these), "API"/"SDK" (sales pitch language)

  // BD (fix v0.5.4: added partnerships plural, business development, listing/integration BD patterns)
  { pattern: /\b(partnerships?|collab|intro\s+to|warm\s+intro|deal)\b/i, label: 'bd', weight: 1.0, tag: 'bd_action' },
  { pattern: /\b(business\s+develop|biz\s*dev)\b/i, label: 'bd', weight: 1.5, tag: 'bd_role_msg' },
  // Fix v0.5.8: Self-ID BD patterns — "BD for Crust", "I'm in BD at…", "Head of Growth at…"
  // Require first-person or self-declare context to avoid 3rd-party references.
  { pattern: /\b(?:I(?:'m|\s+am)\s+(?:in\s+)?BD|(?:I|we)\s+(?:do|handle|run|lead)\s+BD)\b/i, label: 'bd', weight: 1.5, tag: 'bd_self_id_msg' },
  { pattern: /\bBD\s+(?:for|at|with)\s+[A-Z]/i, label: 'bd', weight: 1.5, tag: 'bd_for_org_msg' },
  { pattern: /\b(?:head|director|vp)\s+of\s+(?:growth|partnerships?|BD|business\s+dev)/i, label: 'bd', weight: 1.5, tag: 'bd_title_msg' },
  { pattern: /\b(?:growth\s+(?:&|and)\s+partnerships?|partnerships?\s+(?:&|and)\s+growth)\b/i, label: 'bd', weight: 1.5, tag: 'bd_growth_partnerships_msg' },
  // Token/chain listing patterns — BD signals for protocol/project reps (Aaron/HoudiniSwap)
  // Fix v0.5.5: Made more specific to CEX/DEX/chain context to avoid matching directory listings
  { pattern: /\b(token\s+listing|chain\s+listing|CEX\s+listing|DEX\s+listing)\b/i, label: 'bd', weight: 1.5, tag: 'bd_listing_msg' },
  { pattern: /\b(listing\s+(?:on|with)\s+(?:Binance|Coinbase|Kraken|OKX|Bybit|KuCoin|Gate|Uniswap|SushiSwap|PancakeSwap|[A-Z][a-z]+(?:swap|dex|exchange)))\b/i, label: 'bd', weight: 1.5, tag: 'bd_cex_listing_msg' },
  { pattern: /\b(ecosystem\s+partners?|integration\s+partners?|looking\s+to\s+(?:partner|integrate))\b/i, label: 'bd', weight: 1.5, tag: 'bd_ecosystem_msg' },
  { pattern: /\b(DMs?\s+(?:are\s+)?(?:\[)?open(?:\])?|(?:hmu|hit\s+me\s+up)\s+(?:in\s+)?DMs?)\b/i, label: 'bd', weight: 1.0, tag: 'bd_outreach_msg' },

  // Investor
  { pattern: /\b(series\s*[a-d]|raise|fundrais|invest|portfolio)\b/i, label: 'investor_analyst', weight: 1.0, tag: 'investor_topic' },
  { pattern: /\b(evaluating|due\s*diligence|thesis)\b/i, label: 'investor_analyst', weight: 1.0, tag: 'investor_action' },

  // Recruiter
  { pattern: /\b(hiring|recruit|talent|open\s*role|job\s*posting)\b/i, label: 'recruiter', weight: 1.0, tag: 'recruiter_action' },

  // NOTE: media_kol removed from msg role signals — discussing/selling KOL services doesn't
  //       make someone a KOL. Erhan (EAK agency) mentions "KOL" 7x but is a vendor, not a KOL.
  //       media_kol should only fire from display_name/bio self-identification.
  // Fix v0.5.8: Added first-person self-ID patterns for journalists/editors.
  //             These require "I'm a journalist" / "I work as a reporter" — not 3rd-party references.
  { pattern: /\b(?:I(?:'m|\s+am)\s+a\s+(?:journalist|reporter|editor|columnist|correspondent|content\s*creator))\b/i, label: 'media_kol', weight: 1.5, tag: 'media_kol_self_id_msg' },
  { pattern: /\b(?:I\s+(?:write|cover|report)\s+(?:for|on|about))\b/i, label: 'media_kol', weight: 1.2, tag: 'media_kol_activity_msg' },
  { pattern: /\b(?:editor[- ]in[- ]chief|managing\s+editor)\b/i, label: 'media_kol', weight: 1.5, tag: 'media_kol_title_msg' },

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

  // Security/audit vendor signals (fix v0.5.4 #1) — Drishti pattern
  // "web3 security company", "auditing services", "reach out for security requirements"
  { pattern: /\b(security\s+(?:company|firm|provider|services?)|audit(?:ing)?\s+(?:services?|firm|company)|smart\s*contract\s+audit)\b/i, label: 'vendor_agency', weight: 2.0, tag: 'vendor_security_msg' },
  { pattern: /\b(reach\s+out\s+(?:for|if)|(?:your|any)\s+(?:security|audit)\s+(?:requirements?|needs?))\b/i, label: 'vendor_agency', weight: 1.5, tag: 'vendor_security_cta_msg' },

  // Directory/marketplace vendor signals (fix v0.5.5: Marco/Semoto case)
  // "listed on/with [directory]", "in our ecosystem", "law firms in [directory]'s ecosystem"
  { pattern: /\b(listed\s+(?:on|with)\s+(?:us|our|the)\b|(?:in|on)\s+(?:our|the)\s+(?:directory|ecosystem|platform|marketplace))\b/i, label: 'vendor_agency', weight: 2.0, tag: 'vendor_directory_msg' },
  { pattern: /\b((?:law\s+)?firms?\s+in\s+(?:our|the)\s+ecosystem|providers?\s+(?:on|in)\s+(?:our|the)\s+(?:platform|directory))\b/i, label: 'vendor_agency', weight: 2.0, tag: 'vendor_marketplace_msg' },
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
  // Fix v0.5.7: Removed "we're looking" (too broad — fires on BD/networking).
  //             Added "looking for" + role noun, and CV/resume patterns.
  // Fix v0.5.8: Expanded role noun list after "looking for" to include modifiers
  //             (e.g., "looking for a frontend developer", "looking for senior engineers").
  //             Also added "we need a <role>" pattern.
  { pattern: /\b(hiring|recruit|job\s*(?:posting|opening)|open\s*roles?|vacancy|send\s+(?:your\s+)?(?:CV|resume)|looking\s+for\s+(?:a\s+)?(?:(?:senior|junior|lead|staff|principal|frontend|backend|full[- ]?stack|solidity|smart\s*contract|web3|defi|blockchain|remote)\s+)*(?:developer|engineer|designer|analyst|manager|lead|intern|candidate|dev\b|devs\b)|we\s+need\s+(?:a\s+)?(?:(?:senior|junior|lead|frontend|backend)\s+)*(?:developer|engineer|designer|dev\b))\b/i, label: 'hiring', weight: 0.8, tag: 'hiring_msg' },

  // Support
  // Fix v0.5.8: Split "help" by direction. "I can help" / "happy to help" → support_giving.
  //             "help me" / "need help" / bare "help" in question context → support_seeking.
  //             Removed bare "help" from support_seeking to avoid mis-direction.
  { pattern: /\b(stuck|issue|bug|problem|how\s*do\s*i|need\s+help|help\s+me|can\s+you\s+help|please\s+help)\b/i, label: 'support_seeking', weight: 0.8, tag: 'support_seeking_msg' },
  { pattern: /\b(try\s*this|here.s\s*how|you\s*can|solution|fix|I\s+can\s+help|happy\s+to\s+help|let\s+me\s+help|glad\s+to\s+help|I(?:'ll|\s+will)\s+help)\b/i, label: 'support_giving', weight: 0.8, tag: 'support_giving_msg' },

  // Broadcasting — strengthened (fix #5)
  // Fix v0.5.7: Removed bare "update" (too broad — any progress message).
  //             Kept announce/release/congrat. "update" now requires link or explicit context.
  { pattern: /\b(announce|release|congrat)\b/i, label: 'broadcasting', weight: 0.8, tag: 'broadcasting_msg' },
  { pattern: /\bupdate\b.*(?:https?:\/\/|\bcheck\s+(?:out|it))/i, label: 'broadcasting', weight: 0.8, tag: 'broadcasting_update_link_msg' },
  { pattern: /\b(webinar|live\s+session|ama\b|spaces\b|twitter\s+spaces)\b/i, label: 'broadcasting', weight: 1.0, tag: 'broadcasting_event_msg' },
  { pattern: /\b(we\s+(?:launched|shipped|released|just\s+dropped)|check\s+(?:out|it\s+out))\b/i, label: 'broadcasting', weight: 1.0, tag: 'broadcasting_launch_msg' },
  { pattern: /(?:luma\.com|lu\.ma|eventbrite|meetup\.com)\b/i, label: 'broadcasting', weight: 1.2, tag: 'broadcasting_link_msg' },
  { pattern: /\b(reminder|don'?t\s+miss|register\s+(?:now|here|today)|sign\s+up\s+(?:here|now))\b/i, label: 'broadcasting', weight: 1.0, tag: 'broadcasting_reminder_msg' },

  // Evaluating
  // Fix v0.5.8: Tightened — bare schedule/call/calendly/meeting alone is too noisy.
  //             Require explicit investment context: "schedule a call to discuss investment", etc.
  //             Pure scheduling chatter ("go back to calendly") no longer triggers evaluating.
  { pattern: /\b(schedule|calendar|call|meeting|calendly)\b.*\b(invest|fund|evaluat|portfolio|due\s*diligence|series\s*[a-d]|raise)/i, label: 'evaluating', weight: 0.6, tag: 'evaluating_schedule' },
  { pattern: /\b(invest|fund|evaluat|portfolio|due\s*diligence|series\s*[a-d]|raise)\b.*\b(schedule|calendar|call|meeting|calendly)/i, label: 'evaluating', weight: 0.6, tag: 'evaluating_schedule' },
  // Fix v0.5.8: Removed bare "back" — too ambiguous ("go back to calendly", "I'll be back").
  //             Replaced with bounded investment phrases: "backed by", "backing", "backers".
  { pattern: /\b(investment|fund|backed\s+by|backing|backers|series)\b/i, label: 'evaluating', weight: 0.6, tag: 'evaluating_investment' },
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
 * Detect self-declared affiliations in messages.
 * STRICT first-person self-declarations only — NOT third-person intros or queries.
 * Capture stops at punctuation: . , ; : ! ? ( )
 */
export const MSG_AFFILIATION_PATTERNS: AffiliationSignal[] = [
  // "we at X", "I'm from X", "I work at X" — strict first-person
  { pattern: /\b(?:we\s+at|I(?:'m|\s+am)\s+(?:from|at|with)|I\s+work\s+(?:at|for)|representing)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})(?=[.,;:!?()\s]|$)/i, tag: 'msg_affiliation_at' },
  // "our team/company at X" or "our X team/company" — capture the org after "at" or before "team"
  { pattern: /\b(?:our\s+(?:company|team|project|protocol)\s+at|our\s+(?:company|team|project|protocol)\s+is)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Za-z0-9]+){0,2})(?=[.,;:!?()\s]|$)/i, tag: 'msg_affiliation_our' },
  // "on behalf of X"
  { pattern: /\bon\s+behalf\s+of\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Za-z0-9]+){0,2})(?=[.,;:!?()\s]|$)/i, tag: 'msg_affiliation_behalf' },
  // "[Name] here from [Org]" — skip intermediate "core team at" by matching the final org
  // Pattern: "Name here from [the] [core team at] Org"
  { pattern: /\b[A-Z][a-z]+\s+here\s+from\s+(?:the\s+)?(?:(?:core\s+)?team\s+at\s+)?([A-Z][A-Za-z0-9]+(?:\s+[A-Za-z0-9]+){0,2})(?=[.,;:!?()\s]|$)/i, tag: 'msg_affiliation_intro' },
];

/**
 * Reject patterns for message affiliation — third-person intros, queries, and non-self-declarations.
 * If ANY of these match the message, skip affiliation extraction entirely.
 */
export const MSG_AFFILIATION_REJECT_PATTERNS: RegExp[] = [
  // Third-person inquiries
  /\b(?:anyone|anybody|someone|somebody|who(?:'s|\s+is)?)\s+(?:here\s+)?from\b/i,
  /\bis\s+(?:there\s+)?anyone\s+from\b/i,
  /\blooking\s+for\s+(?:someone|folks?|people|BD'?s?)\s+(?:from|in|at)\b/i,
  // Question intent patterns
  /\bdo\s+(?:we|you)\s+have\b/i,
  /\bin\s+here\s+from\b/i,
  /\bconnect\s+me\s+with\b/i,
  /\bdo\s+you\s+know\b/i,
  // Third-person introductions ("Adding @handle here from X")
  /\badding\s+@/i,
  /\bwelcome\s+@/i,
  /\bintroducing\s+@/i,
];

/**
 * Stopwords that should NOT appear at the start of an org name.
 * If captured org starts with these, reject the extraction.
 */
export const ORG_CAPTURE_STOPWORDS = new Set([
  'any', 'some', 'someone', 'anyone', 'large', 'small', 'big',
  'the', 'a', 'an', 'this', 'that', 'these', 'those',
  'new', 'old', 'good', 'best', 'top', 'major', 'other',
  'here', 'there', 'where', 'which', 'what', 'who',
  'at', 'from', 'with', 'for', 'to', 'in', 'on', 'of',
  'core', 'team', 'our', 'my', 'their', 'your',
  // Clause fragment stopwords (fix v0.5.5: Andy "each protocol" case)
  'each', 'every', 'all', 'most', 'many', 'few', 'several',
]);

/**
 * Bare titles that should NEVER be treated as org names.
 * Catches display names like "JC | Trader" where the pipe segment is a title, not a company.
 * Applied by the org-candidate validator to ALL extraction paths.
 */
export const ORG_TITLE_REJECT_SET = new Set([
  'trader', 'developer', 'engineer', 'founder', 'ceo', 'cto', 'cfo', 'cmo',
  'bd', 'bizdev', 'marketing', 'sales', 'recruiter', 'manager', 'lead',
  'analyst', 'investor', 'vc', 'admin', 'mod', 'community',
  // Multi-word variants
  'business developer', 'business development', 'biz dev',
]);

/**
 * Trailing words to strip from org captures (clause bleed-through).
 * Extended in v0.5.5 to include time/adverb words and chain qualifiers.
 */
export const ORG_TRAILING_STRIP_PATTERN = /\s+(?:team|the\s+team|core\s+team|we|if|and|but|or|so|as|for|to|is|are|was|were|has|have|had|will|would|can|could|may|might|should|right|now|today|here|there|currently|recently|soon|yet|still|just|already|then|very|really|actually)\b.*$/i;

/**
 * Pattern to clamp "X on Y" to just "X" (chain/network qualifiers).
 * Handles: "Crust on Mantle", "Uniswap on Arbitrum", "Protocol on Base"
 * The network name after "on" is stripped, keeping only the protocol name.
 */
export const ORG_ON_CHAIN_CLAMP_PATTERN = /\s+on\s+(?:Ethereum|Mainnet|Arbitrum|Optimism|Base|Polygon|Avalanche|BSC|BNB|Solana|Mantle|Scroll|zkSync|Linea|Blast|Mode|Fantom|Gnosis|Celo|Moonbeam|[A-Z][a-z]+)(?:\s|$)/i;

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
  // Fix v0.5.7: Split VC into separate pattern requiring title context.
  //             "Bloccelerate VC" or "VC at Firm" matches, but "Projects and VC" does not.
  { pattern: /\b(investor|venture|capital|fund|analyst|portfolio)\b/i, label: 'investor_analyst', weight: 3.0, tag: 'dn_investor_title' },
  // VC requires: preceded by an org name (Uppercase word) OR followed by "at/fund/partner/capital"
  // Negative: "and VC" / "or VC" / "& VC" context rejected
  { pattern: /(?:(?<=[A-Z][a-z]+\s)|(?:^|\|\s*))VC\b(?!\s+(?:in|and|or|&))/i, label: 'investor_analyst', weight: 3.0, tag: 'dn_investor_vc' },

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
