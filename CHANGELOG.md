# Changelog

## v0.5.8 — Taxonomy recall + precision fixes (2026-02-07)

### Fixes (8 pattern/logic changes)

- **A) `evaluating_investment`**: Bare `back` removed — too ambiguous ("go back to calendly").
  Replaced with bounded investment phrases: `backed by`, `backing`, `backers`.
- **B) `evaluating_schedule`**: Bare schedule/call/calendly/meeting alone no longer triggers
  evaluating. Now requires co-occurrence with investment language in the same message
  (e.g., "schedule a call to discuss investment").
- **C) BD message recall**: Added 4 new message-level BD patterns:
  - `bd_self_id_msg`: "I'm in BD", "I do BD", "we handle BD"
  - `bd_for_org_msg`: "BD for Crust", "BD at Protocol"
  - `bd_title_msg`: "Head of Growth", "Director of Partnerships", "VP of BD"
  - `bd_growth_partnerships_msg`: "Growth & Partnerships"
- **D) `media_kol` message recall**: Added first-person self-ID patterns for journalists/editors.
  "I'm a journalist", "I write for CoinDesk", "editor-in-chief". Still gated by first-person
  context to avoid vendor KOL agency false positives (Erhan case preserved).
- **E) `builder_tech` co-occurrence gate**: `smart contract` alone (without `builder_action`
  or bio/display_name dev identity) now gets a 50% weight discount. Prevents salespeople
  discussing smart contracts from being classified as builders.
- **F) Support intent direction**: Split "help" matching by direction.
  "I can help you" / "happy to help" → `support_giving`.
  "need help" / "help me" / "stuck" → `support_seeking`.
  Bare "help" removed from `support_seeking` to prevent mis-direction.
- **G) Feature-only intent gating**: Intent claims now require at least one non-feature,
  non-membership evidence source (bio, message, or display_name). Feature-only evidence
  (reply_ratio, mention_count, groups_active) can no longer originate intent claims alone.
- **H) `hiring_msg` recall expansion**: "looking for a frontend developer", "we need a backend dev",
  and modifier-qualified role nouns (senior, junior, lead, staff, solidity, web3, etc.) now match.

### Golden cohort updates

- **pandu-3056**: NONE → `bd` (detected via `bd_for_org_msg` "BD for Crust")
- **paulo-3146**: NONE → `bd` (detected via `bd_title_msg` "Head of Growth")

### New regression test cases (11 added, 35 total)

- `pandu-back-no-evaluating`: "go back to calendly" must NOT trigger evaluating
- `schedule-alone-no-evaluating`: bare schedule words without investment context
- `schedule-with-investment-evaluating`: schedule + investment context SHOULD evaluate
- `bd-self-id-msg`: "BD for Crust" → BD role
- `head-of-growth-bd`: "Head of Growth" → BD role
- `journalist-media-kol`: "I'm a journalist" → media_kol
- `support-giving-direction`: "I can help you" → support_giving (not seeking)
- `support-seeking-still-works`: "need help" → support_seeking preserved
- `feature-only-intent-gated`: feature-only evidence blocked from intent claims
- `hiring-frontend-developer`: "looking for a frontend developer" → hiring
- `builder-tech-alone-weak`: "smart contract" alone → builder NOT supported

### Results

- 35/35 regression tests pass
- 33/33 golden cohort tests pass
- Config: `config/inference.v0.5.8.json`

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
