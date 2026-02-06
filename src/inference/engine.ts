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
import type { Role, Intent, EvidenceType, Predicate } from '../config/taxonomies.js';
import { ROLES, INTENTS } from '../config/taxonomies.js';
import { ROLE_PRIORS, INTENT_PRIORS, PRIORS_VERSION } from '../config/priors.js';
import type { GroupKind } from '../config/taxonomies.js';
import { DEFAULT_CONFIG } from '../config/app-config.js';
import {
  BIO_ROLE_KEYWORDS,
  BIO_INTENT_KEYWORDS,
  MSG_ROLE_KEYWORDS,
  MSG_INTENT_KEYWORDS,
  BIO_AFFILIATION_PATTERNS,
} from './keywords.js';

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

export interface UserInferenceResult {
  userId: number;
  roleClaim: ScoredLabel<Role> | null;
  intentClaim: ScoredLabel<Intent> | null;
  affiliations: string[];
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

export function scoreUser(input: UserInferenceInput): UserInferenceResult {
  const result: UserInferenceResult = {
    userId: input.userId,
    roleClaim: null,
    intentClaim: null,
    affiliations: [],
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
    const rolePrior = ROLE_PRIORS[gk] ?? {};
    for (const [role, weight] of Object.entries(rolePrior)) {
      const r = role as Role;
      roleScores.set(r, (roleScores.get(r) ?? 0) + weight);
      roleEvidence.get(r)!.push({
        evidence_type: 'membership',
        evidence_ref: `membership:group_kind:${gk}`,
        weight,
      });
    }

    const intentPrior = INTENT_PRIORS[gk] ?? {};
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
        result.affiliations.push(match[1].trim());
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
  if (input.totalMsgCount > 0) {
    const replyRatio = input.totalReplyCount / input.totalMsgCount;
    if (replyRatio > 0.4) {
      const w = 1.5;
      intentScores.set('support_giving', (intentScores.get('support_giving') ?? 0) + w);
      intentEvidence.get('support_giving')!.push({
        evidence_type: 'feature',
        evidence_ref: `feature:reply_ratio=${replyRatio.toFixed(2)}`,
        weight: w,
      });
    }
  }

  // High BD group share → bd role signal
  if (input.bdGroupMsgShare > 0.5 && input.totalMsgCount >= 3) {
    const w = 1.0;
    roleScores.set('bd', (roleScores.get('bd') ?? 0) + w);
    roleEvidence.get('bd')!.push({
      evidence_type: 'feature',
      evidence_ref: `feature:bd_share=${input.bdGroupMsgShare.toFixed(2)}`,
      weight: w,
    });
  }

  // High mention count → community / networking signal
  if (input.totalMentionCount >= 3) {
    const w = 1.0;
    roleScores.set('community', (roleScores.get('community') ?? 0) + w);
    roleEvidence.get('community')!.push({
      evidence_type: 'feature',
      evidence_ref: `feature:mention_count=${input.totalMentionCount}`,
      weight: w,
    });
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
    if (nonMembershipEvidence.length < DEFAULT_CONFIG.minNonMembershipEvidence) {
      result.gatingNotes.push(
        `role:${topRole.label} GATED — only ${nonMembershipEvidence.length} non-membership evidence (need ≥${DEFAULT_CONFIG.minNonMembershipEvidence})`,
      );
    } else if (topRole.probability < DEFAULT_CONFIG.minClaimConfidence) {
      result.gatingNotes.push(
        `role:${topRole.label} GATED — confidence ${topRole.probability.toFixed(3)} < threshold ${DEFAULT_CONFIG.minClaimConfidence}`,
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
    if (nonMembershipEvidence.length < DEFAULT_CONFIG.minNonMembershipEvidence) {
      result.gatingNotes.push(
        `intent:${topIntent.label} GATED — only ${nonMembershipEvidence.length} non-membership evidence (need ≥${DEFAULT_CONFIG.minNonMembershipEvidence})`,
      );
    } else if (topIntent.probability < DEFAULT_CONFIG.minClaimConfidence) {
      result.gatingNotes.push(
        `intent:${topIntent.label} GATED — confidence ${topIntent.probability.toFixed(3)} < threshold ${DEFAULT_CONFIG.minClaimConfidence}`,
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

  const claimRes = await client.query(
    `INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version, notes)
     VALUES ($1, $2::predicate_label, $3, $4, $5::claim_status, $6, $7)
     RETURNING id`,
    [userId, predicate, objectValue, confidence, status, PRIORS_VERSION, notes ?? null],
  );
  const claimId: number = claimRes.rows[0].id;

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
