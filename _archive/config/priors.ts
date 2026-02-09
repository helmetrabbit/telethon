/**
 * Inference priors — base probabilities by group_kind.
 *
 * These are NOT claims. They are starting weights that the inference
 * engine uses. A prior alone never produces a claim (evidence gating
 * requires at least one non-membership evidence type).
 *
 * Version-stamped so we can track which prior set generated a claim.
 */

import type { GroupKind, Role, Intent } from './taxonomies.js';

export const PRIORS_VERSION = 'v0.1.0';

/**
 * Prior weight per role, keyed by group_kind.
 * Weights are un-normalized (the inference engine will softmax them).
 */
export const ROLE_PRIORS: Record<GroupKind, Partial<Record<Role, number>>> = {
  bd: {
    bd: 3.0,
    founder_exec: 1.5,
    investor_analyst: 1.0,
    vendor_agency: 0.8,
    builder: 0.5,
    community: 0.3,
    recruiter: 0.3,
    unknown: 0.1,
  },
  work: {
    builder: 3.0,
    founder_exec: 1.5,
    bd: 0.5,
    community: 0.5,
    investor_analyst: 0.3,
    vendor_agency: 0.3,
    recruiter: 0.3,
    unknown: 0.1,
  },
  general_chat: {
    community: 3.0,
    builder: 1.0,
    bd: 0.5,
    founder_exec: 0.5,
    investor_analyst: 0.3,
    vendor_agency: 0.3,
    recruiter: 0.3,
    unknown: 0.2,
  },
  unknown: {
    unknown: 1.0,
    community: 0.5,
    builder: 0.5,
    bd: 0.3,
    founder_exec: 0.3,
    investor_analyst: 0.2,
    vendor_agency: 0.2,
    recruiter: 0.2,
  },
};

/**
 * Prior weight per intent, keyed by group_kind.
 */
export const INTENT_PRIORS: Record<GroupKind, Partial<Record<Intent, number>>> = {
  bd: {
    networking: 3.0,
    evaluating: 2.0,
    selling: 1.5,
    hiring: 0.5,
    broadcasting: 0.5,
    support_seeking: 0.3,
    support_giving: 0.3,
    unknown: 0.1,
  },
  work: {
    support_seeking: 2.0,
    support_giving: 2.0,
    broadcasting: 1.0,
    networking: 0.5,
    evaluating: 0.5,
    selling: 0.3,
    hiring: 0.3,
    unknown: 0.1,
  },
  general_chat: {
    networking: 2.0,
    broadcasting: 1.5,
    support_seeking: 1.0,
    support_giving: 1.0,
    evaluating: 0.5,
    selling: 0.3,
    hiring: 0.3,
    unknown: 0.2,
  },
  unknown: {
    unknown: 1.0,
    networking: 0.5,
    broadcasting: 0.5,
    support_seeking: 0.3,
    support_giving: 0.3,
    evaluating: 0.3,
    selling: 0.2,
    hiring: 0.2,
  },
};
