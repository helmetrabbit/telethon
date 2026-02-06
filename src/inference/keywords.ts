/**
 * Keyword dictionaries for bio and message pattern matching.
 *
 * Each entry maps a regex pattern to a role or intent label + weight.
 * Patterns are case-insensitive and matched against plain text.
 *
 * These are intentionally conservative — we'd rather miss a signal
 * than hallucinate one. Weights are additive on top of priors.
 */

import type { Role, Intent } from '../config/taxonomies.js';

// ── Types ───────────────────────────────────────────────

export interface KeywordSignal<T extends string> {
  pattern: RegExp;
  label: T;
  weight: number;
  /** Human-readable tag for evidence_ref (e.g. "keyword:ceo") */
  tag: string;
}

// ── Bio → Role signals ─────────────────────────────────

export const BIO_ROLE_KEYWORDS: KeywordSignal<Role>[] = [
  // Founder / Executive
  { pattern: /\b(ceo|cto|coo|cfo|co-?founder|founder)\b/i, label: 'founder_exec', weight: 3.0, tag: 'founder_title' },
  { pattern: /\b(chief\s+(executive|technology|operating|financial))\b/i, label: 'founder_exec', weight: 3.0, tag: 'chief_title' },
  { pattern: /\bhead\s+of\b/i, label: 'founder_exec', weight: 1.5, tag: 'head_of' },

  // Builder / Developer
  { pattern: /\b(developer|engineer|dev|full[- ]?stack|backend|frontend|solidity)\b/i, label: 'builder', weight: 2.5, tag: 'dev_title' },
  { pattern: /\b(building|shipped|hacking|coding)\b/i, label: 'builder', weight: 1.5, tag: 'builder_verb' },
  { pattern: /\b(rust|typescript|python|golang|smart\s*contract)\b/i, label: 'builder', weight: 1.5, tag: 'tech_skill' },

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

  // Media / KOL
  { pattern: /\b(KOL|influencer|ambassador|content\s*creator)\b/i, label: 'media_kol', weight: 2.5, tag: 'kol_title' },
  { pattern: /\b(media|journalist|editor|press|PR\s*manager)\b/i, label: 'media_kol', weight: 2.0, tag: 'media_title' },
  { pattern: /\b(promotion|campaign|social\s*media)\b/i, label: 'media_kol', weight: 1.5, tag: 'media_activity' },

  // Market Maker
  { pattern: /\b(market\s*mak|MM\b|liquidity\s*provid|trading\s*desk)\b/i, label: 'market_maker', weight: 2.5, tag: 'mm_title' },
  { pattern: /\b(orderbook|spread|depth|OTC)\b/i, label: 'market_maker', weight: 1.5, tag: 'mm_signal' },
];

// ── Message → Role signals (aggregate patterns) ────────

export const MSG_ROLE_KEYWORDS: KeywordSignal<Role>[] = [
  { pattern: /\b(shipped|deployed|launched|built|refactored|merged)\b/i, label: 'builder', weight: 1.0, tag: 'builder_action' },
  { pattern: /\b(tvl|protocol|smart\s*contract|mainnet|testnet|defi)\b/i, label: 'builder', weight: 0.8, tag: 'builder_topic' },
  { pattern: /\b(partnership|collab|intro\s+to|warm\s+intro|deal)\b/i, label: 'bd', weight: 1.0, tag: 'bd_action' },
  { pattern: /\b(series\s*[a-d]|raise|fundrais|invest|portfolio)\b/i, label: 'investor_analyst', weight: 1.0, tag: 'investor_topic' },
  { pattern: /\b(evaluating|due\s*diligence|thesis)\b/i, label: 'investor_analyst', weight: 1.0, tag: 'investor_action' },
  { pattern: /\b(hiring|recruit|talent|open\s*role|job\s*posting)\b/i, label: 'recruiter', weight: 1.0, tag: 'recruiter_action' },

  // Media / KOL
  { pattern: /\b(KOL|influencer|ambassador|campaign|promotion)\b/i, label: 'media_kol', weight: 1.0, tag: 'kol_action' },
  { pattern: /\b(content|thread|tweet|post|article|PR\b|press)\b/i, label: 'media_kol', weight: 0.6, tag: 'media_topic' },

  // Market Maker
  { pattern: /\b(market\s*mak|liquidity|spread|orderbook|depth)\b/i, label: 'market_maker', weight: 1.0, tag: 'mm_action' },
  { pattern: /\b(CEX|DEX\s*listing|OTC|trading\s*pair)\b/i, label: 'market_maker', weight: 0.8, tag: 'mm_topic' },
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
  { pattern: /\b(connect|intro|meet up|let'?s\s*chat|dm\s*me)\b/i, label: 'networking', weight: 0.8, tag: 'networking_msg' },
  { pattern: /\b(evaluat|assess|compare|review|look\s*into)\b/i, label: 'evaluating', weight: 0.8, tag: 'evaluating_msg' },
  { pattern: /\b(sell|pitch|demo|offer|pricing|buy)\b/i, label: 'selling', weight: 0.8, tag: 'selling_msg' },
  { pattern: /\b(hiring|recruit|job|open\s*role|we.re\s*looking)\b/i, label: 'hiring', weight: 0.8, tag: 'hiring_msg' },
  { pattern: /\b(help|stuck|issue|bug|problem|how\s*do\s*i)\b/i, label: 'support_seeking', weight: 0.8, tag: 'support_seeking_msg' },
  { pattern: /\b(try\s*this|here.s\s*how|you\s*can|solution|fix)\b/i, label: 'support_giving', weight: 0.8, tag: 'support_giving_msg' },
  { pattern: /\b(announce|update|ship|launch|release|congrat)\b/i, label: 'broadcasting', weight: 0.8, tag: 'broadcasting_msg' },
  { pattern: /\b(schedule|calendar|call|meeting|calendly)\b/i, label: 'evaluating', weight: 0.6, tag: 'evaluating_schedule' },
  { pattern: /\b(investment|fund|back|series)\b/i, label: 'evaluating', weight: 0.6, tag: 'evaluating_investment' },
];

// ── Affiliation signals (bio only, self-declared) ──────

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

  // Builder / Developer
  { pattern: /\b(developer|engineer|dev\b|full[- ]?stack|backend|frontend|solidity)\b/i, label: 'builder', weight: 3.0, tag: 'dn_dev_title' },
  { pattern: /\b(builder|building|hacker)\b/i, label: 'builder', weight: 2.0, tag: 'dn_builder' },

  // Investor / Analyst
  { pattern: /\b(investor|vc\b|venture|capital|fund|analyst|portfolio)\b/i, label: 'investor_analyst', weight: 3.0, tag: 'dn_investor_title' },

  // Recruiter
  { pattern: /\b(recruiter|recruiting|talent|headhunter|staffing|hiring)\b/i, label: 'recruiter', weight: 3.0, tag: 'dn_recruiter_title' },

  // Vendor / Agency
  { pattern: /\b(agency|consulting|consultant)\b/i, label: 'vendor_agency', weight: 2.5, tag: 'dn_agency' },

  // Media / KOL
  { pattern: /\bKOL\b/i, label: 'media_kol', weight: 3.0, tag: 'dn_kol' },
  { pattern: /\b(influencer|ambassador|content\s*creat)\b/i, label: 'media_kol', weight: 3.0, tag: 'dn_media_title' },
  { pattern: /\b(marketing|PR\b|press)\b/i, label: 'media_kol', weight: 2.0, tag: 'dn_marketing' },

  // Market Maker
  { pattern: /\bMM\b/, label: 'market_maker', weight: 3.0, tag: 'dn_mm' },  // case-sensitive — "MM" only
  { pattern: /\b(market\s*mak|liquidity)\b/i, label: 'market_maker', weight: 3.0, tag: 'dn_mm_title' },

  // Community
  { pattern: /\b(community|moderator|mod\b|admin)\b/i, label: 'community', weight: 2.5, tag: 'dn_community' },

  // Security / Audit → mapped to builder (closest existing role)
  { pattern: /\b(security|audit|auditor)\b/i, label: 'builder', weight: 2.0, tag: 'dn_security' },

  // Research → mapped to investor_analyst
  { pattern: /\b(research|researcher)\b/i, label: 'investor_analyst', weight: 2.0, tag: 'dn_researcher' },
];

// ── Display Name → Affiliation signals ─────────────────
// Parse "Name | Company" and "Name | Company Role" pipe patterns.

export const DISPLAY_NAME_AFFILIATION_PATTERNS: AffiliationSignal[] = [
  // "Alice | Acme Labs" or "Alice | Acme Labs BD Lead"
  // Captures the company segment between the pipe and either end-of-string or a known role word
  { pattern: /\|\s*([A-Z][A-Za-z0-9.]+(?:\s+[A-Z][A-Za-z0-9.]+){0,4}?)(?:\s+(?:CEO|CTO|COO|CFO|CMO|co-?founder|founder|BD|head|director|VP|lead|manager|engineer|dev|developer|builder|investor|vc|recruiter|growth|sales|marketing|KOL|ambassador|MM|community|mod|admin|research|analyst|operations|ops)\b|$)/i, tag: 'dn_affiliation_pipe' },
  // "Role @ Company" or "Role at Company"
  { pattern: /(?:@|at)\s+([A-Z][A-Za-z0-9.]+(?:\s+[A-Z][A-Za-z0-9.]+){0,3})/i, tag: 'dn_affiliation_at' },
];
