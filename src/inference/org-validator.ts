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
 * Validate and clean an org-name candidate.
 *
 * Pipeline:
 *   1. Strip trailing clause words (right, now, today …)
 *   2. Clamp "X on Y" chain qualifiers
 *   3. Truncate at first lowercase-starting word (org names are capitalised)
 *   4. Length ≥ 3
 *   5. Reject if first word is a stopword
 *   6. Reject if starts lowercase (after cleaning)
 *   7. Reject if in AFFILIATION_REJECT_SET
 *   8. Reject if matches AFFILIATION_REJECT_PATTERNS
 *   9. Reject if matches GENERIC_PHRASE_PATTERN
 *
 * @returns Cleaned org name or `null` if rejected.
 */
export function validateOrgCandidate(raw: string): string | null {
  let name = raw.trim();

  // Step 1: strip trailing clause bleed-through
  name = name.replace(ORG_TRAILING_STRIP_PATTERN, '').trim();

  // Step 2: clamp "X on Y" → "X"
  name = name.replace(ORG_ON_CHAIN_CLAMP_PATTERN, '').trim();

  // Step 3: truncate at the first lowercase-starting word
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

  // Step 4: minimum length
  if (name.length < 3) return null;

  // Step 5: first-word stopword
  const firstWord = name.split(/\s+/)[0].toLowerCase();
  if (ORG_CAPTURE_STOPWORDS.has(firstWord)) return null;

  // Step 6: must start with uppercase (real org names are capitalised)
  if (STARTS_LOWERCASE.test(name)) return null;

  // Step 7: reject-set (locations, titles, verticals, etc.)
  if (AFFILIATION_REJECT_SET.has(name.toLowerCase())) return null;

  // Step 8: reject patterns (events, conferences, pure numbers)
  if (AFFILIATION_REJECT_PATTERNS.some((rp) => rp.test(name))) return null;

  // Step 9: generic multi-word phrase
  if (GENERIC_PHRASE_PATTERN.test(name)) return null;

  return name;
}
