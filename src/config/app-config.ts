/**
 * Feature flags and runtime settings.
 */

export interface AppConfig {
  /** If true, drop raw message text after feature extraction */
  dropRawTextAfterFeatures: boolean;

  /** If true, pseudonymize external user IDs before storage */
  pseudonymizeExternalIds: boolean;

  /** Minimum non-membership evidence rows required for role/intent claims */
  minNonMembershipEvidence: number;

  /** Minimum confidence threshold to emit a claim */
  minClaimConfidence: number;
}

export const DEFAULT_CONFIG: AppConfig = {
  dropRawTextAfterFeatures: false,
  pseudonymizeExternalIds: false,
  minNonMembershipEvidence: 1,
  minClaimConfidence: 0.15,
};
