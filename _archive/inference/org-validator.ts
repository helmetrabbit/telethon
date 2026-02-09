/**
 * Org-candidate validator — quality gate for affiliation extraction.
 *
 * Centralises all org-name validation logic so that every extraction
 * path (message, display-name, bio) runs the same checks.
 *
 * Returns `null` if the candidate is rejected, or the cleaned name
 * if it passes validation.
 */

import {
  ORG_CAPTURE_STOPWORDS,
  ORG_TRAILING_STRIP_PATTERN,
  ORG_ON_CHAIN_CLAMP_PATTERN,
  AFFILIATION_REJECT_SET,
  AFFILIATION_REJECT_PATTERNS,
  ORG_TITLE_REJECT_SET,
} from './keywords.js';

// ── Additional shape checks ────────────────────────────

/**
 * Reject candidates whose first character is lowercase — real org names
 * always start capitalised (e.g. "Crust", "SolidityScan", NOT "each protocol").
 */
const STARTS_LOWERCASE = /^[a-z]/;

/**
 * Reject generic multi-word phrases that slip through stopword checks.
 * These look like sentence fragments, not org names.
 */
const GENERIC_PHRASE_PATTERN =
  /^(?:all\s|any\s|some\s|each\s|every\s|most\s|many\s|few\s|several\s|different\s|various\s|other\s|certain\s|specific\s|particular\s)/i;

/**
 * Pattern to clamp definitional clauses: "X is a …" → "X".
 * Handles: "Awesome Capital is a VC fund" → "Awesome Capital"
 */
const IS_A_CLAMP = /\s+is\s+an?\s+.*/i;

/**
 * Validate and clean an org-name candidate.
 *
 * Pipeline:
 *   1. Strip trailing clause words (right, now, today …)
 *   2. Clamp "X on Y" chain qualifiers
 *   3. Clamp "X is a …" definitional clauses
 *   4. Truncate at first lowercase-starting word (org names are capitalised)
 *   5. Length ≥ 3
 *   6. Reject if first word is a stopword
 *   7. Reject if starts lowercase (after cleaning)
 *   8. Reject if in ORG_TITLE_REJECT_SET (bare titles)
 *   9. Reject if in AFFILIATION_REJECT_SET
 *   10. Reject if matches AFFILIATION_REJECT_PATTERNS
 *   11. Reject if matches GENERIC_PHRASE_PATTERN
 *
 * @returns Cleaned org name or `null` if rejected.
 */
export function validateOrgCandidate(raw: string): string | null {
  let name = raw.trim();

  // Step 1: strip trailing clause bleed-through
  name = name.replace(ORG_TRAILING_STRIP_PATTERN, '').trim();

  // Step 2: clamp "X on Y" → "X"
  name = name.replace(ORG_ON_CHAIN_CLAMP_PATTERN, '').trim();

  // Step 3: clamp "X is a …" → "X" (definitional clauses in pipe segments)
  name = name.replace(IS_A_CLAMP, '').trim();

  // Step 4: truncate at the first lowercase-starting word
  // Org names are capitalised (SolidityScan, Gate.io, HoudiniSwap).
  // If the capture bled into a lowercase word ("specialize", "in"), chop it.
  const words = name.split(/\s+/);
  let truncIdx = words.length;
  for (let i = 1; i < words.length; i++) {
    if (/^[a-z]/.test(words[i])) {
      truncIdx = i;
      break;
    }
  }
  name = words.slice(0, truncIdx).join(' ');

  // Step 5: minimum length
  if (name.length < 3) return null;

  // Step 6: first-word stopword
  const firstWord = name.split(/\s+/)[0].toLowerCase();
  if (ORG_CAPTURE_STOPWORDS.has(firstWord)) return null;

  // Step 7: must start with uppercase (real org names are capitalised)
  if (STARTS_LOWERCASE.test(name)) return null;

  // Step 8: bare title check — "Trader", "Developer", "BD" etc. are NOT orgs
  if (ORG_TITLE_REJECT_SET.has(name.toLowerCase())) return null;

  // Step 9: reject-set (locations, industries, verticals, etc.)
  if (AFFILIATION_REJECT_SET.has(name.toLowerCase())) return null;

  // Step 10: reject patterns (events, conferences, pure numbers)
  if (AFFILIATION_REJECT_PATTERNS.some((rp) => rp.test(name))) return null;

  // Step 11: generic multi-word phrase
  if (GENERIC_PHRASE_PATTERN.test(name)) return null;

  return name;
}
