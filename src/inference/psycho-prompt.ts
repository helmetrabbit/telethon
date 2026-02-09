/**
 * Build a prompt for psychographic/communication-style profiling.
 * Captures personality, communication style, location, and key topics.
 */

import type { UserBriefing } from './llm-prompt.js';

// Larger budget — style detection needs more text
// High Context: Increased to 150000 to allow full history analysis with DeepSeek V3
const MAX_MESSAGE_CHARS = 150000;

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

  // Omni-Profile Extensions
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
  scam_risk_score?: number; // 0-100
  confidence_score?: number; // 0.0-1.0
  career_stage?: string;
  tribe_affiliations?: string[];
  reputation_score?: number; // 0-100
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
}

// ── Prompt builder ──────────────────────────────────────

export function buildPsychoPrompt(briefing: UserBriefing): string {
  const groupList = briefing.groups
    .map((g) => `  - ${g.title} (${g.kind}${g.description ? ` — ${g.description}` : ''}, ${g.msgCount} msgs)`)
    .join('\n');

  let msgBlock = '';
  let charBudget = MAX_MESSAGE_CHARS;
  for (const m of briefing.sampleMessages) {
    const line = `[${m.sent_at}] ${m.groupTitle ? `[in ${m.groupTitle}] ` : ''}${m.text}\n`;
    if (charBudget - line.length < 0) break;
    msgBlock += line;
    charBudget -= line.length;
  }

  const claimList = briefing.existingClaims.length > 0
    ? briefing.existingClaims
        .map((c) => `  - ${c.predicate}: ${c.value} (confidence=${c.confidence})`)
        .join('\n')
    : '  (none)';

  return `You are a cynical, high-precision communication analyst. Your job is to extract highly specific behavioral data.

### 🚫 STRICT PROHIBITIONS (ANTI-SLOP)
- DO NOT say "Uses emojis" → Say WHICH emoji ("Overuses 🔥", "Ends sentences with 🗿").
- DO NOT say "Friendly tone" → Say "Overly apologetic" or "Fake enthusiastic".
- DO NOT say "Uses slang" → Quote the EXACT slang ("Uses 'gm' unironically", "Says 'based'").
- DO NOT give generic advice like "Be polite" → Give a SPECIFIC opening line.
- DO NOT guess location from event attendance (BDs travel constantly). Only set 'based_in' if they explicitly say "I live in X" or "My office in Y".
- DO NOT dump lists of tokens or tickers (BTC, ETH, SOL) into 'tribe_affiliations' or 'driving_values'. Only tag the *concept* (e.g. 'Bitcoin Maxi' instead of 'BTC').
- If the user is generic/boring, return empty arrays. Do not hallucinate interesting traits.

## User Profile
- Handle: ${briefing.handle || '(none)'}
- Display Name: ${briefing.displayName || '(none)'}
- Bio: ${briefing.bio || '(none)'}
- Total Messages: ${briefing.totalMessages}
${briefing.viralStats ? `- Viral Stats: ${briefing.viralStats.totalViews} views, ${briefing.viralStats.totalReactions} reactions, ${briefing.viralStats.repliesReceived} replies received` : ''}
- Groups:
${groupList || '  (none)'}

## Known Classification
${claimList}

## Message Samples (chronological)
${msgBlock || '(no messages)'}

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
  
  "generated_bio_professional": "A sharp, 1-sentence LinkedIn headline summarizing their professional identity (e.g. 'Senior Solidity Engineer specializing in ZK rollups').",
  "generated_bio_personal": "A casual, 1-sentence Twitter bio capturing their vibe (e.g. 'DeFi degen hunting shitcoins and complaining about gas').",
  "primary_role": "Specific Job Title (e.g. 'Head of Growth', 'Solidity Dev', 'Founder'). Return null if unclear.",
  "primary_company": "Main Company/Project they work for (e.g. 'Consensys', 'Solana Foundation'). Return null if unclear.",
  "deep_skills": ["Specific technical or domain skills inferred from jargon (e.g. 'Rust', 'Tokenomics', 'Community Management'). Max 8 items."],
  "technical_specifics": ["Liberal collection of specific tools, protocols, or frameworks mentioned (e.g. 'Halo2', 'Foundry', 'EigenLayer', 'Wasm', 'React', 'Solidity'). Capture actual tech stack usage. Max 15 items."],
  "business_focus": ["High-level Sector Focus (e.g. 'DeFi', 'Infra', 'Consumer', 'DePIN', 'AI Agents'). Max 5 items."],
  "affiliations": ["ALL associated projects/DAOs. Max 10 items."],
  "tribe_affiliations": ["Cultural tribes/subcultures (e.g. 'Solana Maxi', 'E-Acc', 'Regenerative Finance', 'NFT Flipper', 'Degen'). **DO NOT** use job titles like 'BD' or 'Founder'. **DO NOT** list tokens. Max 5 items."],
  "driving_values": ["Core motivations (e.g. 'Profit', 'Technology', 'Privacy', 'Clout'). **DO NOT** list tokens (e.g. BTC, ETH) or assets. Abstract concepts only. Max 5 items."],
  "languages": ["Detected languages (e.g. 'English', 'Chinese', 'Russian')"],
  "social_platforms": ["Platform names ONLY where user has a presence (e.g. 'Twitter', 'LinkedIn', 'Farcaster', 'GitHub', 'Telegram', 'YouTube'). DO NOT include URLs or handles here. Max 10 items."],
  "social_urls": ["Full social profile URLs or handles extracted from bio/messages (e.g. 'twitter.com/vitalikbuterin', 'linkedin.com/in/johndoe', 'github.com/user', 'x.com/user', 'warpcast.xyz/user'). Only include URLs/handles you can actually see in the evidence. Max 10 items."],
  "buying_power": "Estimate: 'High', 'Medium', 'Low', 'Unknown' (Based on transaction talk, investment size)",
  "career_stage": "one of: Junior, Mid-Level, Senior, Executive, Founder, Retired, Student",
  "scam_risk_score": 0 to 100 (Integer. 0=Safe, 100=Obvious Scammer. Flag vague promises, urgency, DM-sliding),
  "confidence_score": 0.0 to 1.0 (Float. How confident are you in this analysis?),

  "quirks": [ { "value": "Deep behavioral/intellectual quirk (e.g. 'Uses rhetorical questions', 'Quotes philosophy')", "quote": "context", "date": "YYYY-MM-DD" } ],
  "notable_topics": [ { "value": "Specific Interest (not just 'Crypto')", "quote": "context", "date": "YYYY-MM-DD" } ],
  "pain_points": [ { "value": "Specific complaint/problem they face (e.g. 'Hates high gas fees', 'Can\'t find reliable dev', 'Complain about VC dumping')", "quote": "context", "date": "YYYY-MM-DD" } ],
  "crypto_values": [ { "value": "Value system signal (e.g. 'Privacy-maximalist', 'Reg-compliant', 'On-chain purist', 'Profit-maxi'). DO NOT list tokens.", "quote": "context", "date": "YYYY-MM-DD" } ],
  "connection_requests": [ { "value": "Person/Company they are trying to reach (e.g. 'Looking for intro to Paradigm', 'Anyone know the Consensys BD team?')", "quote": "context", "date": "YYYY-MM-DD" } ],
  "fingerprint_tags": [ "Strict list of keywords: 'ETH Denver', 'Vitalik', 'Berlin', 'ZK-Rollups', 'Safe', 'Uniswap' (Liberally tag events, people, places, topics, companies). Max 15 items." ],
  "based_in": "Format: 'City, Country (Source: [Evidence])'. Evidence MUST be a quote or specific observation. e.g. 'London, UK (Source: Mentioned \"taking the tube\" on 2023-05-12)'. Return null if uncertain.",
  "attended_events": ["Specific conferences"],
  "preferred_contact_style": "Psychological analysis of how THEY interact. (e.g. 'Lurks for weeks then drops a link', 'Writes paragraphs of analysis', 'Only replies to high-status users', 'Acts like a confused beginner').",
  "reasoning": "Brief justification"
}

IMPORTANT:
- For 'quirks' and 'notable_topics', you MUST extract the 'quote' and the 'date' from the message logs provided.
- If a message line starts with "[2023-10-27T10:00:00.000Z]", use "2023-10-27" as the date.
- Do not hallucinate quotes or dates. If evidence is missing, do not include the item.
- 'quirks' should be DEEP stylistic or behavioral observations.
  - GOOD: "Uses intense Socratic questioning", "Formal tone but drops intense slang", "Refers to obscure economic theories", "Uses military analogies", "Passive-aggressive ellipses".
  - AVOID: Just listing emojis unless they are rare/defining. Look for intellectual or personality quirks.
`;
}
