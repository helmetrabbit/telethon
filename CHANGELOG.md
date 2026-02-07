# Changelog

## v0.5.7 — Regex tightening + golden cohort lock (2026-02-07)

### Fixes (6 regex patterns)

- **A) `builder_tech`**: Bare `PR` removed — too ambiguous (public relations vs pull request).
  Now requires `PR #<digits>` or explicit `pull request`. `rust` now requires `\b` word boundary
  to avoid matching inside "trust".
- **B) `builder_action`**: Added first-person constraint (`I/we shipped|deployed|…`).
  Prevents 3rd-party statements ("they pushed") from firing as builder evidence.
- **C) `hiring_msg`**: Removed overly broad `we're looking` (fires on BD/networking context).
  Now requires explicit hiring language: `hiring`, `recruit`, `job posting/opening`,
  `open role`, `vacancy`, `send CV/resume`, or `looking for a <role noun>`.
- **D) `dn_investor_title`**: Split `VC` into separate context-gated pattern.
  Requires: preceded by an org name (`Bloccelerate VC`) OR at start of pipe segment.
  Rejects: `and VC`, `or VC`, `& VC` context (e.g., "Connecting Projects and VC").
- **E) `org_mm`**: Bare `MM` removed — too ambiguous (catches "MM DOOM", initials).
  Now requires org context: `CompanyName MM` (lookbehind for uppercase word) or
  longform `market maker/making/liquidity provider`.
- **F) `broadcasting_msg`**: Bare `update` removed — too broad (any progress message).
  `update` now requires a link or explicit `check out/it` context.
  `announce`, `release`, `congrat` remain as-is.

### New

- **Golden cohort test**: 33 real users from page-2 export locked in
  `tests/fixtures/taxonomy_golden_cohort_2026-02-07.json`.
  Runner: `npm run golden` (exercises full DB → inference pipeline).
- Config: `config/inference.v0.5.7.json`

### Results

- 24/24 regression tests pass
- 33/33 golden cohort tests pass
- Key fix validations:
  - UC (BTCWire PR): builder → vendor_agency ✅
  - Jay Wong ("Projects and VC"): investor_analyst → NONE ✅
  - MM DOOM: market_maker org-type → NONE ✅
  - Nick | AngeLabs MM: BD retained, MM org-type retained ✅
  - Kate | Bloccelerate VC: investor_analyst retained ✅

## v0.5.6 — Final patch + freeze (2026-02-06)

- ORG_TITLE_REJECT_SET: bare titles ("Trader", "Developer") rejected as org names
- IS_A_CLAMP: "X is a Y" → "X" in display name pipe segments
- Business Developer → bd override (not builder)
- 24 regression test cases, all pass
- Trace mode: full explain/debug with per-message hit capture
