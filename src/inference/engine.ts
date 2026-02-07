/**
 * Deterministic inference engine.
 *
 * Pipeline per user:
 *   1. Load priors based on group_kind memberships
 *   2. Scan bio for keyword signals → role/intent evidence
 *   3. Scan messages for keyword signals → role/intent evidence
 *   4. Read aggregated features → feature-based evidence
 *   5. Combine prior + evidence weights per label
 *   6. Softmax → probabilities
 *   7. Evidence gating: refuse to emit if insufficient non-membership evidence
 *   8. Emit top-1 role claim + top-1 intent claim (if gating passes)
 *
 * All logic is explainable: every claim points to specific evidence rows.
 */

import pg from 'pg';
import type { Role, Intent, EvidenceType, Predicate, OrgType } from '../config/taxonomies.js';
import { ROLES, INTENTS } from '../config/taxonomies.js';
import type { GroupKind } from '../config/taxonomies.js';
import type { InferenceConfig } from '../config/inference-config.js';
import {
  BIO_ROLE_KEYWORDS,
  BIO_INTENT_KEYWORDS,
  MSG_ROLE_KEYWORDS,
  MSG_INTENT_KEYWORDS,
  BIO_AFFILIATION_PATTERNS,
  MSG_AFFILIATION_PATTERNS,
  MSG_AFFILIATION_REJECT_PATTERNS,
  DISPLAY_NAME_ROLE_KEYWORDS,
  DISPLAY_NAME_AFFILIATION_PATTERNS,
  AFFILIATION_REJECT_SET,
  AFFILIATION_REJECT_PATTERNS,
  ORG_TYPE_SIGNALS,
} from './keywords.js';
import { validateOrgCandidate } from './org-validator.js';

// ── Org name normalization ───────────────────────────────
// Normalize org names for dedup: lowercase, strip punctuation,
// collapse whitespace, remove common suffixes.

export function normalizeOrgName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[''""]/g, '')                           // smart quotes
    .replace(/[^a-z0-9\s.]/g, '')                     // strip non-alphanumeric except dots
    .replace(/\.(exchange|io|xyz|com|org|net|gg|fi)$/i, '')  // strip TLD suffixes
    .replace(/\b(inc|ltd|corp|labs?|protocol|network|finance|exchange)\b/g, '')  // strip entity suffixes
    .replace(/\s+/g, ' ')                              // collapse whitespace
    .trim();
}

// ── Types ───────────────────────────────────────────────

export interface EvidenceRow {
  evidence_type: EvidenceType;
  evidence_ref: string;
  weight: number;
}

export interface ScoredLabel<T extends string> {
  label: T;
  score: number;
  probability: number;
  evidence: EvidenceRow[];
}

export interface UserInferenceInput {
  userId: number;
  displayName: string | null;
  bio: string | null;
  /** group_kinds the user is a member of */
  memberGroupKinds: GroupKind[];
  /** All message texts (plain) */
  messageTexts: string[];
  /** Aggregated features */
  totalMsgCount: number;
  totalReplyCount: number;
  totalMentionCount: number;
  avgMsgLen: number;
  bdGroupMsgShare: number;
  groupsActiveCount: number;
}

export interface AffiliationResult {
  name: string;
  source: EvidenceType;
  tag: string;
}

export interface OrgTypeResult {
  orgType: OrgType;
  source: EvidenceType;
  tag: string;
}

export interface UserInferenceResult {
  userId: number;
  roleClaim: ScoredLabel<Role> | null;
  intentClaim: ScoredLabel<Intent> | null;
  affiliations: AffiliationResult[];
  orgTypes: OrgTypeResult[];
  /** Notes about gating decisions */
  gatingNotes: string[];
}

// ── Softmax ─────────────────────────────────────────────

function softmax(scores: Map<string, number>): Map<string, number> {
  const values = [...scores.values()];
  const maxVal = Math.max(...values);
  let sumExp = 0;
  const exps = new Map<string, number>();

  for (const [label, score] of scores) {
    const e = Math.exp(score - maxVal); // subtract max for numerical stability
    exps.set(label, e);
    sumExp += e;
  }

  const probs = new Map<string, number>();
  for (const [label, e] of exps) {
    probs.set(label, e / sumExp);
  }
  return probs;
}

// ── Score a user ────────────────────────────────────────

export function scoreUser(input: UserInferenceInput, config: InferenceConfig): UserInferenceResult {
  const result: UserInferenceResult = {
    userId: input.userId,
    roleClaim: null,
    intentClaim: null,
    affiliations: [],
    orgTypes: [],
    gatingNotes: [],
  };

  // ── 1. Initialize scores with priors ────────────────

  const roleScores = new Map<Role, number>();
  const roleEvidence = new Map<Role, EvidenceRow[]>();
  const intentScores = new Map<Intent, number>();
  const intentEvidence = new Map<Intent, EvidenceRow[]>();

  for (const r of ROLES) {
    roleScores.set(r, 0);
    roleEvidence.set(r, []);
  }
  for (const i of INTENTS) {
    intentScores.set(i, 0);
    intentEvidence.set(i, []);
  }

  // Apply priors from each group_kind membership
  for (const gk of input.memberGroupKinds) {
    const rolePrior = config.rolePriors[gk] ?? {};
    for (const [role, weight] of Object.entries(rolePrior)) {
      const r = role as Role;
      roleScores.set(r, (roleScores.get(r) ?? 0) + weight);
      roleEvidence.get(r)!.push({
        evidence_type: 'membership',
        evidence_ref: `membership:group_kind:${gk}`,
        weight,
      });
    }

    const intentPrior = config.intentPriors[gk] ?? {};
    for (const [intent, weight] of Object.entries(intentPrior)) {
      const i = intent as Intent;
      intentScores.set(i, (intentScores.get(i) ?? 0) + weight);
      intentEvidence.get(i)!.push({
        evidence_type: 'membership',
        evidence_ref: `membership:group_kind:${gk}`,
        weight,
      });
    }
  }

  // ── 2. Bio keyword signals ──────────────────────────

  if (input.bio && input.bio.trim().length > 0) {
    for (const kw of BIO_ROLE_KEYWORDS) {
      if (kw.pattern.test(input.bio)) {
        roleScores.set(kw.label, (roleScores.get(kw.label) ?? 0) + kw.weight);
        roleEvidence.get(kw.label)!.push({
          evidence_type: 'bio',
          evidence_ref: `bio:${kw.tag}`,
          weight: kw.weight,
        });
      }
    }

    for (const kw of BIO_INTENT_KEYWORDS) {
      if (kw.pattern.test(input.bio)) {
        intentScores.set(kw.label, (intentScores.get(kw.label) ?? 0) + kw.weight);
        intentEvidence.get(kw.label)!.push({
          evidence_type: 'bio',
          evidence_ref: `bio:${kw.tag}`,
          weight: kw.weight,
        });
      }
    }

    // Affiliation detection (self-declared only)
    for (const aff of BIO_AFFILIATION_PATTERNS) {
      const match = input.bio.match(aff.pattern);
      if (match && match[1]) {
        const bioOrg = validateOrgCandidate(match[1]);
        if (bioOrg) {
          result.affiliations.push({ name: bioOrg, source: 'bio', tag: aff.tag });
        }
      }
    }
  }

  // ── 2b. Display name signals ────────────────────────
  // Parse "Name | Company Role" patterns for role + affiliation evidence.
  // Display names are high-signal — users self-declare identity here.

  if (input.displayName && input.displayName.trim().length > 0) {
    const dn = input.displayName;

    for (const kw of DISPLAY_NAME_ROLE_KEYWORDS) {
      if (kw.pattern.test(dn)) {
        roleScores.set(kw.label, (roleScores.get(kw.label) ?? 0) + kw.weight);
        roleEvidence.get(kw.label)!.push({
          evidence_type: 'display_name',
          evidence_ref: `display_name:${kw.tag}`,
          weight: kw.weight,
        });
      }
    }

    // Fix v0.5.6 B: "Business Developer" / "Business Development" / "BizDev" in display name
    // must route to BD, NOT builder.  The word "Developer" inside this compound phrase
    // should NOT fire the dn_dev_title builder signal.
    if (/\b(business\s+developer|business\s+development|bizdev)\b/i.test(dn)) {
      const bdOverrideW = 4.0; // strong enough to beat dn_dev_title's 3.0
      roleScores.set('bd', (roleScores.get('bd') ?? 0) + bdOverrideW);
      roleEvidence.get('bd')!.push({
        evidence_type: 'display_name',
        evidence_ref: 'display_name:dn_business_developer_override',
        weight: bdOverrideW,
      });
      // Neutralise the dn_dev_title builder signal that matched "Developer"
      // by subtracting the same weight that dn_dev_title would have added (3.0)
      const builderPenalty = -3.0;
      roleScores.set('builder', (roleScores.get('builder') ?? 0) + builderPenalty);
      roleEvidence.get('builder')!.push({
        evidence_type: 'display_name',
        evidence_ref: 'display_name:dn_business_developer_builder_block',
        weight: builderPenalty,
      });
    }

    // Agency / vendor detection override (fix #4):
    // If the display name contains selling language (discount, %, pricing, services, packages),
    // this is a commercial entity, not an individual KOL. Boost vendor_agency.
    if (/\b(discount|%\s*off|\d+%|pricing|packages?|services?|solutions?|agency)\b/i.test(dn)) {
      const w = 3.5;
      roleScores.set('vendor_agency', (roleScores.get('vendor_agency') ?? 0) + w);
      roleEvidence.get('vendor_agency')!.push({
        evidence_type: 'display_name',
        evidence_ref: 'display_name:dn_selling_language',
        weight: w,
      });
    }

    // Display name affiliation detection — with reject-list filtering
    for (const aff of DISPLAY_NAME_AFFILIATION_PATTERNS) {
      const match = dn.match(aff.pattern);
      if (match && match[1]) {
        let rawCompany = match[1].trim();

        // For pipe-based extraction: skip if the pipe segment contains @ (already handled by at-pattern)
        // or contains role/title words that indicate it's not a company name but a title segment
        if (aff.tag === 'dn_affiliation_pipe') {
          const fullSegment = match[0];
          if (/@/.test(fullSegment)) continue;
          // If segment is purely role/title language, skip it
          if (/^\|\s*(?:Head|CEO|CTO|COO|CFO|CMO|VP|Director|Lead|Manager|BD)\s+(?:of|at|for)\b/i.test(fullSegment)) continue;
        }

        // Strip leading title prefixes: "CEO nReach" → "nReach"
        rawCompany = rawCompany.replace(
          /^(?:CEO|CTO|COO|CFO|CMO|VP|Director|Head|Lead|Manager)\s+/i,
          '',
        ).trim();

        // Run through centralised org-candidate validator
        const company = validateOrgCandidate(rawCompany);
        if (!company) continue;

        // Normalize for dedup: lowercase, strip punctuation, collapse whitespace
        const normKey = normalizeOrgName(company);

        // Avoid duplicates (by normalized form)
        if (!result.affiliations.some((a) => normalizeOrgName(a.name) === normKey)) {
          result.affiliations.push({ name: company, source: 'display_name', tag: aff.tag });
        }
      }
    }

    // Org-type detection from display name (fix #2)
    // If the display name or its company segment matches an org-type pattern,
    // emit a has_org_type claim. This separates "person is BD" from "firm is a market maker".
    for (const ots of ORG_TYPE_SIGNALS) {
      if (ots.pattern.test(dn)) {
        if (!result.orgTypes.some((o) => o.orgType === ots.orgType)) {
          result.orgTypes.push({ orgType: ots.orgType, source: 'display_name', tag: ots.tag });
        }
      }
    }
  }

  // ── 3. Message keyword signals ──────────────────────
  // Scan a sample of messages (up to 200) for patterns.

  const messageSample = input.messageTexts.slice(0, 200);
  const msgRoleHits = new Map<string, number>(); // tag → count
  const msgIntentHits = new Map<string, number>();

  for (const text of messageSample) {
    for (const kw of MSG_ROLE_KEYWORDS) {
      if (kw.pattern.test(text)) {
        const key = `${kw.label}:${kw.tag}`;
        msgRoleHits.set(key, (msgRoleHits.get(key) ?? 0) + 1);
      }
    }
    for (const kw of MSG_INTENT_KEYWORDS) {
      if (kw.pattern.test(text)) {
        const key = `${kw.label}:${kw.tag}`;
        msgIntentHits.set(key, (msgIntentHits.get(key) ?? 0) + 1);
      }
    }

    // Message-based affiliation extraction
    // Only scan first 50 messages to avoid noise from casual mentions.
    if (messageSample.indexOf(text) < 50) {
      // Skip messages that match reject patterns (third-person inquiries, questions, @handle intros)
      const isInquiry = MSG_AFFILIATION_REJECT_PATTERNS.some((rp) => rp.test(text));
      if (!isInquiry) {
        for (const aff of MSG_AFFILIATION_PATTERNS) {
          const match = text.match(aff.pattern);
          if (match && match[1]) {
            // Check for @handle before the match — indicates third-person intro
            const matchIndex = text.indexOf(match[0]);
            const beforeMatch = text.slice(0, matchIndex);
            if (beforeMatch.includes('@')) continue; // Third-person: "Adding @user here from X"

            // Validate through centralised org-candidate validator
            const company = validateOrgCandidate(match[1]);
            if (!company) continue;

            const normKey = normalizeOrgName(company);
            // Message self-declare overrides display_name: if a normalized match
            // already exists from display_name, replace it with the message source
            const existingIdx = result.affiliations.findIndex(
              (a) => normalizeOrgName(a.name) === normKey,
            );
            if (existingIdx >= 0) {
              // Keep message source as higher priority
              result.affiliations[existingIdx] = { name: company, source: 'message', tag: aff.tag };
            } else {
              result.affiliations.push({ name: company, source: 'message', tag: aff.tag });
            }
          }
        }
      }
    }
  }

  // Convert hit counts to evidence (log-scaled to avoid one prolific pattern dominating)
  for (const [key, count] of msgRoleHits) {
    const [label, tag] = key.split(':') as [Role, string];
    const kw = MSG_ROLE_KEYWORDS.find((k) => k.label === label && k.tag === tag);
    if (!kw) continue;
    const scaledWeight = kw.weight * Math.log2(1 + count);
    roleScores.set(label, (roleScores.get(label) ?? 0) + scaledWeight);
    roleEvidence.get(label)!.push({
      evidence_type: 'message',
      evidence_ref: `msg:${tag}:count=${count}`,
      weight: parseFloat(scaledWeight.toFixed(3)),
    });
  }

  // Fix v0.5.4 #1: Message affiliation + vendor evidence → boost vendor_agency
  // When someone self-identifies from a company in messages AND shows vendor/service selling
  // language, they're actively representing that company as a vendor, not just wearing an "old hat"
  // from their display name. This handles Drishti (SolidityScan security vendor) being mis-labeled
  // as BD because her display name says "Gate.io South Asia BD".
  const hasMessageAffiliation = result.affiliations.some((a) => a.source === 'message');
  const hasVendorMsgEvidence = [...msgRoleHits.keys()].some((k) => k.startsWith('vendor_agency:'));
  if (hasMessageAffiliation && hasVendorMsgEvidence) {
    const boostW = 4.0; // Strong boost to override display_name role signals
    roleScores.set('vendor_agency', (roleScores.get('vendor_agency') ?? 0) + boostW);
    roleEvidence.get('vendor_agency')!.push({
      evidence_type: 'message',
      evidence_ref: 'msg:vendor_affiliation_boost:message_self_declare+vendor_evidence',
      weight: boostW,
    });
  }

  // Fix v0.5.5 #2: Directory/marketplace/broker override → force vendor_agency, suppress bd
  // If message evidence includes vendor_directory_msg or vendor_marketplace_msg, the user
  // operates a directory/marketplace (e.g. Semoto), NOT doing BD.  Clamp bd score to 0
  // and apply a strong vendor_agency boost to guarantee the role wins.
  const hasDirectoryEvidence = [...msgRoleHits.keys()].some((k) =>
    k === 'vendor_agency:vendor_directory_msg' || k === 'vendor_agency:vendor_marketplace_msg',
  );
  if (hasDirectoryEvidence) {
    // Suppress bd entirely
    roleScores.set('bd', 0);
    roleEvidence.set('bd', []);  // wipe evidence — it was a false positive
    // Strong boost to vendor_agency
    const dirBoost = 6.0;
    roleScores.set('vendor_agency', (roleScores.get('vendor_agency') ?? 0) + dirBoost);
    roleEvidence.get('vendor_agency')!.push({
      evidence_type: 'message',
      evidence_ref: 'msg:directory_override:vendor_directory+marketplace',
      weight: dirBoost,
    });
  }

  for (const [key, count] of msgIntentHits) {
    const [label, tag] = key.split(':') as [Intent, string];
    const kw = MSG_INTENT_KEYWORDS.find((k) => k.label === label && k.tag === tag);
    if (!kw) continue;
    const scaledWeight = kw.weight * Math.log2(1 + count);
    intentScores.set(label, (intentScores.get(label) ?? 0) + scaledWeight);
    intentEvidence.get(label)!.push({
      evidence_type: 'message',
      evidence_ref: `msg:${tag}:count=${count}`,
      weight: parseFloat(scaledWeight.toFixed(3)),
    });
  }

  // ── 4. Feature-based signals ────────────────────────

  // High reply ratio → support_giving or community
  // Requires totalMsgCount>=8 to avoid false positives on low-volume users
  if (input.totalMsgCount >= 8) {
    const replyRatio = input.totalReplyCount / input.totalMsgCount;
    if (replyRatio > 0.4) {
      const w = 0.7;  // Reduced from 1.5 — feature-only evidence should not dominate
      intentScores.set('support_giving', (intentScores.get('support_giving') ?? 0) + w);
      intentEvidence.get('support_giving')!.push({
        evidence_type: 'feature',
        evidence_ref: `feature:reply_ratio=${replyRatio.toFixed(2)}`,
        weight: w,
      });
    }
  }

  // NOTE: bd_share feature signal removed (fix #6) — contradicts evidence-only approach.
  // Group membership share should not influence role classification.

  // High mention count → networking signal ONLY (fix v0.5.4 #4)
  // NOTE: Removed community ROLE boost from mention_count. Being mentioned doesn't make
  // someone a community manager — Sukesh/Dexter were mis-scored as community from mentions.
  // Community ROLE should require message evidence (welcome messages, mod actions, rule enforcement).
  // Mention count still boosts networking INTENT (reasonable signal for connector behavior).
  if (input.totalMentionCount >= 3) {
    const w = 1.0;
    // Networking intent only — NOT community role
    intentScores.set('networking', (intentScores.get('networking') ?? 0) + w);
    intentEvidence.get('networking')!.push({
      evidence_type: 'feature',
      evidence_ref: `feature:mention_count=${input.totalMentionCount}`,
      weight: w,
    });
  }

  // Multi-group activity → networking signal
  if (input.groupsActiveCount >= 3) {
    const w = 1.0;
    intentScores.set('networking', (intentScores.get('networking') ?? 0) + w);
    intentEvidence.get('networking')!.push({
      evidence_type: 'feature',
      evidence_ref: `feature:groups_active=${input.groupsActiveCount}`,
      weight: w,
    });
  }

  // ── 5. Softmax → probabilities ──────────────────────

  const roleProbs = softmax(roleScores as Map<string, number>);
  const intentProbs = softmax(intentScores as Map<string, number>);

  // ── 6. Build ranked lists ───────────────────────────

  const rankedRoles: ScoredLabel<Role>[] = ROLES
    .filter((r) => r !== 'unknown')
    .map((r) => ({
      label: r,
      score: roleScores.get(r) ?? 0,
      probability: roleProbs.get(r) ?? 0,
      evidence: roleEvidence.get(r) ?? [],
    }))
    .sort((a, b) => b.probability - a.probability);

  const rankedIntents: ScoredLabel<Intent>[] = INTENTS
    .filter((i) => i !== 'unknown')
    .map((i) => ({
      label: i,
      score: intentScores.get(i) ?? 0,
      probability: intentProbs.get(i) ?? 0,
      evidence: intentEvidence.get(i) ?? [],
    }))
    .sort((a, b) => b.probability - a.probability);

  // ── 7. Evidence gating ──────────────────────────────

  const topRole = rankedRoles[0];
  if (topRole) {
    const nonMembershipEvidence = topRole.evidence.filter(
      (e) => e.evidence_type !== 'membership',
    );
    if (nonMembershipEvidence.length < config.gating.minNonMembershipEvidence) {
      result.gatingNotes.push(
        `role:${topRole.label} GATED — only ${nonMembershipEvidence.length} non-membership evidence (need ≥${config.gating.minNonMembershipEvidence})`,
      );
    } else if (topRole.probability < config.gating.minClaimConfidence) {
      result.gatingNotes.push(
        `role:${topRole.label} GATED — confidence ${topRole.probability.toFixed(3)} < threshold ${config.gating.minClaimConfidence}`,
      );
    } else {
      result.roleClaim = topRole;
    }
  }

  const topIntent = rankedIntents[0];
  if (topIntent) {
    const nonMembershipEvidence = topIntent.evidence.filter(
      (e) => e.evidence_type !== 'membership',
    );
    if (nonMembershipEvidence.length < config.gating.minNonMembershipEvidence) {
      result.gatingNotes.push(
        `intent:${topIntent.label} GATED — only ${nonMembershipEvidence.length} non-membership evidence (need ≥${config.gating.minNonMembershipEvidence})`,
      );
    } else if (topIntent.probability < config.gating.minClaimConfidence) {
      result.gatingNotes.push(
        `intent:${topIntent.label} GATED — confidence ${topIntent.probability.toFixed(3)} < threshold ${config.gating.minClaimConfidence}`,
      );
    } else {
      result.intentClaim = topIntent;
    }
  }

  return result;
}

// ── Write claims to DB ──────────────────────────────────

export async function writeClaimWithEvidence(
  client: pg.PoolClient,
  userId: number,
  predicate: Predicate,
  objectValue: string,
  confidence: number,
  status: 'tentative' | 'supported',
  evidence: EvidenceRow[],
  modelVersion: string,
  notes?: string,
): Promise<number> {
  // Deduplicate evidence by (evidence_type, evidence_ref)
  const deduped = new Map<string, EvidenceRow>();
  for (const e of evidence) {
    const key = `${e.evidence_type}::${e.evidence_ref}`;
    const existing = deduped.get(key);
    if (!existing || e.weight > existing.weight) {
      deduped.set(key, e);
    }
  }

  // Upsert claim: ON CONFLICT updates confidence/status/timestamp
  const claimRes = await client.query(
    `INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version, notes, generated_at)
     VALUES ($1, $2::predicate_label, $3, $4, $5::claim_status, $6, $7, now())
     ON CONFLICT (subject_user_id, predicate, object_value, model_version)
     DO UPDATE SET confidence = EXCLUDED.confidence,
                  status = EXCLUDED.status,
                  notes = EXCLUDED.notes,
                  generated_at = now()
     RETURNING id`,
    [userId, predicate, objectValue, confidence, status, modelVersion, notes ?? null],
  );
  const claimId: number = claimRes.rows[0].id;

  // Clear old evidence for this claim (idempotent replace)
  await client.query('DELETE FROM claim_evidence WHERE claim_id = $1', [claimId]);

  for (const e of deduped.values()) {
    await client.query(
      `INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
       VALUES ($1, $2::evidence_type, $3, $4)
       ON CONFLICT DO NOTHING`,
      [claimId, e.evidence_type, e.evidence_ref, e.weight],
    );
  }

  return claimId;
}

// ── Write abstention log entry ──────────────────────────

export async function writeAbstention(
  client: pg.PoolClient,
  userId: number,
  predicate: Predicate,
  reasonCode: string,
  details: string,
  modelVersion: string,
): Promise<void> {
  await client.query(
    `INSERT INTO abstention_log (subject_user_id, predicate, reason_code, details, model_version)
     VALUES ($1, $2::predicate_label, $3, $4, $5)`,
    [userId, predicate, reasonCode, details, modelVersion],
  );
}
