/**
 * Taxonomy definitions — controlled vocabularies for the ontology.
 * These mirror the Postgres ENUMs defined in the migration.
 */

export const GROUP_KINDS = ['bd', 'work', 'general_chat', 'unknown'] as const;
export type GroupKind = (typeof GROUP_KINDS)[number];

export const ROLES = [
  'bd',
  'builder',
  'founder_exec',
  'investor_analyst',
  'recruiter',
  'vendor_agency',
  'community',
  'media_kol',
  'market_maker',
  'unknown',
] as const;
export type Role = (typeof ROLES)[number];

export const INTENTS = [
  'networking',
  'evaluating',
  'selling',
  'hiring',
  'support_seeking',
  'support_giving',
  'broadcasting',
  'unknown',
] as const;
export type Intent = (typeof INTENTS)[number];

export const EVIDENCE_TYPES = ['bio', 'message', 'feature', 'membership', 'display_name'] as const;
export type EvidenceType = (typeof EVIDENCE_TYPES)[number];

export const CLAIM_STATUSES = ['tentative', 'supported'] as const;
export type ClaimStatus = (typeof CLAIM_STATUSES)[number];

export const PREDICATES = [
  'has_role',
  'has_intent',
  'has_topic_affinity',
  'affiliated_with',
] as const;
export type Predicate = (typeof PREDICATES)[number];
