# tg-profile-engine

**Local-only Telegram chat analysis with an ontology-lite knowledge graph, evidence-gated inference, and anti-hallucination constraints.**

This system ingests [Telegram Desktop JSON exports](https://telegram.org/blog/export-and-more), stores them in a normalised PostgreSQL schema, computes per-user behavioural features, runs a deterministic inference engine that assigns role/intent labels, and exports self-contained profile documents where **every claim is backed by traceable evidence**.

> ⚠️ **Privacy first** — all data stays on your machine. No external APIs, no cloud services, no telemetry. The database runs in a local Docker container and nothing ever leaves `localhost`.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Telethon Collector](#telethon-collector)
5. [Ingesting Telethon Output](#ingesting-telethon-output)
6. [Diagnostics Export](#diagnostics-export)
7. [Pipeline Steps](#pipeline-steps)
8. [Schema Overview](#schema-overview)
9. [What "Ontology-Lite" and "Claims" Mean](#what-ontology-lite-and-claims-mean)
10. [Inference Engine](#inference-engine)
11. [Evidence Gating & Anti-Hallucination](#evidence-gating--anti-hallucination)
12. [Output Format](#output-format)
13. [Interpreting Profiles & Unknowns](#interpreting-profiles--unknowns)
14. [DBeaver / SQL Verification](#dbeaver--sql-verification)
15. [Constraint Verification](#constraint-verification)
16. [Negative Test Runbook](#negative-test-runbook)
17. [Configuration](#configuration)
18. [Project Structure](#project-structure)
19. [Ethics & Privacy](#ethics--privacy)

---

## Architecture

```
Telegram JSON export
        │
        ▼
┌─────────────────┐     ┌──────────────────┐     ┌───────────────────┐
│  1. ingest       │────▶│  2. compute-     │────▶│  3. infer-claims  │
│  (raw + norm)    │     │     features     │     │  (scoring engine) │
└─────────────────┘     └──────────────────┘     └───────────────────┘
        │                        │                        │
        ▼                        ▼                        ▼
┌───────────────────────────────────────────────────────────────────┐
│                     PostgreSQL 16 (Docker)                        │
│  Layer 1: raw_imports, raw_import_rows          (traceability)   │
│  Layer 2: users, groups, memberships, messages  (normalised)     │
│  Layer 3: user_features_daily, claims, claim_evidence (derived)  │
└───────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────┐
│  4. export-      │──▶  data/output/*.json
│     profiles     │
└─────────────────┘
```

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| **Node.js** | ≥ 18 | Runtime for TypeScript CLI tools |
| **npm** | (bundled) | Dependency management |
| **Docker** | ≥ 20 | Runs PostgreSQL 16 + dbmate |
| **Docker Compose** | v2 (bundled with Docker Desktop) | Orchestrates services |
| **Make** | (usually pre-installed on macOS/Linux) | Convenience commands |
| **Python** | ≥ 3.9 | Required only for the Telethon collector (`tools/telethon_collector/`) |

Optional but recommended:
- **DBeaver** (or any SQL client) — for manual data inspection

---

## Quick Start

### 1. Clone and install

```bash
cd telethon
npm install
```

### 2. Start the database

```bash
make db-up        # starts Postgres 16 in Docker
make db-migrate   # runs schema migrations via dbmate
```

### 3. Place your Telegram export

Export a chat from **Telegram Desktop** → `Export chat history` → format **JSON** → save the file into:

```
data/exports/your_chat.json
```

A sample file (`sample_bd_chat.json`) is included for testing.

### 4. Run the full pipeline

```bash
# One command does it all:
make pipeline

# Or step by step:
make ingest              # → raw + normalised data
make compute-features    # → per-user daily metrics
make infer-claims        # → evidence-backed claims
make export-profiles     # → JSON profiles in data/output/
```

### 5. Inspect results

```bash
ls data/output/
cat data/output/profiles.json | head -100
```

Or connect DBeaver to the local database (see [DBeaver section](#dbeaver--sql-verification)).

---

## Telethon Collector

The `tools/telethon_collector/` directory contains a Python-based Telethon client that exports messages and participant lists directly from the Telegram API. This is an alternative to Telegram Desktop's manual JSON export and captures data that Desktop exports miss (e.g., full participant lists with usernames).

### 1. Set up credentials

1. Go to <https://my.telegram.org> → **API development tools** → create an app.
2. Copy the template and fill in your values:

```bash
cp tools/telethon_collector/.env.example tools/telethon_collector/.env
# Edit .env with your TG_API_ID, TG_API_HASH, and TG_PHONE
```

### 2. Create the Python virtualenv

```bash
make tg:venv
```

This creates a venv at `tools/telethon_collector/.venv/` and installs `telethon` + `python-dotenv`.

### 3. Find your target group

```bash
make tg:list-dialogs
```

This prints all your Telegram dialogs (groups, channels, DMs) with their title, ID, and username. Find the group you want to collect.

### 4. Collect messages + participants

```bash
# Collect from a group by title (default: 5000 messages)
make tg:collect GROUP="BD in Web3"

# Custom output path + message limit
make tg:collect GROUP="BD in Web3" OUT=data/exports/bd_web3.json LIMIT=10000

# Only messages after a date
make tg:collect GROUP="BD in Web3" SINCE=2024-01-01
```

The collector outputs a JSON file compatible with the ingestion pipeline, including a `participants[]` array (when the API permits) and a `participants_status` field indicating success or fallback.

> See `tools/telethon_collector/README.md` for full details on arguments and troubleshooting.

---

## Ingesting Telethon Output

Telethon exports are ingested with the same `ingest` command used for Desktop exports:

```bash
npm run ingest -- --file data/exports/bd_web3.json --kind bd
```

The ingestion pipeline automatically detects the Telethon format and:

- Imports all messages (with `ON CONFLICT DO NOTHING` for re-runs)
- Processes the `participants[]` array — creates user records and memberships for every non-bot, non-deleted participant
- Logs participant ingestion stats to the console

After ingestion, run the rest of the pipeline:

```bash
make compute-features
make infer-claims
make export-profiles
```

Or run everything in one shot:

```bash
make pipeline
```

---

## Diagnostics Export

The diagnostics exporter produces a **share-safe** JSON summary of the database — no message text, no usernames, no PII.

```bash
DIAGNOSTICS_SALT=any-random-string make export-diagnostics
```

Or via npm:

```bash
DIAGNOSTICS_SALT=any-random-string npm run export-diagnostics
```

The output file (default `share/diagnostics.json`) contains:

| Section | Contents |
|---------|----------|
| `meta` | Timestamp, model version, salt indicator |
| `counts` | Groups, users, messages, date range |
| `membership` | Distribution by group kind, top 20 users by message count (pseudonymized via SHA-256) |
| `inference_summary` | Claims by predicate, evidence type distribution, abstention breakdown, coverage percentages |

All user identifiers are replaced with `sha256(salt + user_id)` truncated to 12 hex characters. The salt is never stored in the output.

Optional arguments:

```bash
# Filter to a specific group
DIAGNOSTICS_SALT=s npm run export-diagnostics -- --group-external-id 12345 --out-file share/bd_diag.json
```

---

## Pipeline Steps

### Step 1 — `ingest`

```bash
node dist/cli/ingest.js [--file path/to/export.json] [--kind bd|work|general_chat]
```

- Reads a Telegram Desktop JSON export
- Computes SHA-256 of the file for **idempotency** (re-running with the same file is a no-op)
- Writes raw traceability rows (`raw_imports` + `raw_import_rows`)
- Normalises into `users`, `groups`, `memberships`, `messages`, `message_mentions`
- Skips service messages (joins, leaves, etc.)
- Handles both plain-string and rich-text-array Telegram text formats

### Step 2 — `compute-features`

```bash
node dist/cli/compute-features.js
```

Computes per-user per-day aggregate features via a single CTE-based SQL upsert:

| Feature | Description |
|---------|-------------|
| `msg_count` | Messages sent that day |
| `reply_count` | Messages that are replies |
| `mention_count` | Total @-mentions made |
| `avg_msg_len` | Average character length |
| `groups_active_count` | Distinct groups messaged in |
| `bd_group_msg_share` | Fraction of messages in BD-type groups |

### Step 3 — `infer-claims`

```bash
node dist/cli/infer-claims.js
```

Runs the deterministic inference engine for every user. Produces `claims` + `claim_evidence` rows. See [Inference Engine](#inference-engine) for details.

### Step 4 — `export-profiles`

```bash
node dist/cli/export-profiles.js
```

Generates self-contained JSON profiles in `data/output/`:
- One file per user (`<id>_<handle>.json`)
- A combined `profiles.json` with all users

---

## Schema Overview

The database uses three layers, each building on the previous:

### Layer 1 — Raw Traceability

| Table | Purpose |
|-------|---------|
| `raw_imports` | One row per ingested file (path, SHA-256, timestamp) |
| `raw_import_rows` | One row per JSON item (preserves original payload) |

### Layer 2 — Normalised Entities

| Table | Purpose |
|-------|---------|
| `users` | Deduplicated users (external_id, display_name, handle, bio) |
| `groups` | Chat groups (external_id, title, kind) |
| `memberships` | User ↔ Group many-to-many (first/last seen) |
| `messages` | Normalised messages (text, length, flags, timestamps) |
| `message_mentions` | @-mentions extracted from messages |

### Layer 3 — Derived / Claims

| Table | Purpose |
|-------|---------|
| `user_features_daily` | Aggregated behavioural metrics per user per day |
| `claims` | Inferred labels: `has_role`, `has_intent`, `affiliated_with` |
| `claim_evidence` | Evidence rows backing each claim (type, ref, weight) |
| `abstention_log` | Records when the engine **chose not** to emit a claim (predicate, reason, model version) |

### ENUMs (Controlled Vocabularies)

| ENUM | Values |
|------|--------|
| `group_kind` | bd, work, general_chat, unknown |
| `role_label` | bd, builder, founder_exec, investor_analyst, recruiter, vendor_agency, community, unknown |
| `intent_label` | networking, evaluating, selling, hiring, support_seeking, support_giving, broadcasting, unknown |
| `evidence_type` | bio, message, feature, membership |
| `claim_status` | tentative, supported |
| `predicate_label` | has_role, has_intent, has_topic_affinity, affiliated_with |

---

## What "Ontology-Lite" and "Claims" Mean

### Ontology-Lite

A full ontology (like OWL/RDF) defines classes, properties, and logical axioms over a domain. This project uses an **ontology-lite** approach — a small, fixed vocabulary of entity types (users, groups), relationship predicates (`has_role`, `has_intent`, `has_topic_affinity`, `affiliated_with`), and controlled ENUM values (8 roles, 8 intents, 4 group kinds). There are no class hierarchies, no transitive inference, and no open-world assumptions. The vocabulary is enforced by PostgreSQL ENUM types and constraint triggers — the system literally cannot store a label outside the defined taxonomy.

### Claims ≠ Facts

A **claim** is a structured assertion about a user, paired with a **confidence score** and an **evidence array**. Claims are explicitly labelled `tentative` or `supported` and should always be read as:

> "Based on evidence E₁, E₂, … Eₙ, the engine estimates with probability P that user U has role R."

Claims are **not ground truth**. They are deterministic outputs of a keyword-matching + prior-weighting system. They can be wrong (a user whose bio says "investor" might be joking). The evidence array exists so you can **audit** the reasoning and decide for yourself.

### Abstentions

When the engine **cannot** produce a claim that meets the evidence threshold, it logs an **abstention** — a record that says "I looked at user U for predicate P and chose not to make a claim, because: insufficient evidence." Abstentions are stored in the `abstention_log` table and are just as important as claims: they prove the system is **defaulting to silence** rather than guessing.

---

## Inference Engine

The engine is **fully deterministic** — no ML models, no LLMs, no randomness. Given the same data it will always produce the same output.

### Pipeline per user

1. **Load priors** — base probabilities derived from group_kind memberships (e.g., being in a BD group slightly increases the `bd` role prior)
2. **Scan bio** — regex-based keyword matching against curated dictionaries (e.g., "investor" → `investor_analyst`, weight 3.0)
3. **Scan messages** — same keyword matching, log-scaled so 100 matching messages aren't 100× one match
4. **Read features** — behavioural signals (high reply ratio → `support_giving`, high BD share → `bd`)
5. **Combine** — sum prior + all evidence weights per label
6. **Softmax** — convert raw scores to probabilities
7. **Evidence gating** — refuse to emit if insufficient evidence (see below)
8. **Emit** — write top-1 role claim + top-1 intent claim + any affiliation claims

### Scoring Formula

For each label $l$ in a category:

$$\text{score}(l) = \text{prior}(l) + \sum_{e \in \text{evidence}} w_e \cdot \mathbb{1}[\text{e matches } l]$$

$$P(l) = \frac{e^{\text{score}(l)}}{\sum_{l'} e^{\text{score}(l')}}$$

A claim is emitted only if $P(l) \geq 0.15$ (configurable) **and** evidence gating passes.

---

## Evidence Gating & Anti-Hallucination

The system enforces a **"default to unknown"** philosophy at two levels:

### Application-Level Gating

Before writing a claim, the engine checks:
1. **Minimum confidence**: `P(label) ≥ 0.15` (configurable via `gating.minClaimConfidence` in the inference config)
2. **Minimum non-membership evidence**: role and intent claims require at least 1 evidence row that is **not** just group membership (configurable via `gating.minNonMembershipEvidence`)

If either check fails, the claim is **not written** — an abstention is logged instead, recording the predicate and reason code.

### Database-Level Triggers (Safety Net)

Four `DEFERRABLE INITIALLY DEFERRED` constraint triggers act as a **structural** safety net. Even if the application code has a bug, the database will reject invalid data at `COMMIT`:

| # | Trigger | Fires on | Enforces |
|---|---------|----------|----------|
| 1 | `claim_must_have_evidence` | `INSERT OR UPDATE` on `claims` | Every claim must have ≥1 `claim_evidence` row |
| 2 | `claim_needs_real_evidence` | `INSERT OR UPDATE` on `claims` | `has_role`, `has_intent`, and `has_topic_affinity` claims must have ≥1 evidence row whose type is **not** `membership` |
| 3 | `claim_validate_object_value` | `INSERT OR UPDATE` on `claims` | `has_role` / `has_intent` object_value must be a valid ENUM member; free-text predicates must be non-empty |
| 4 | `evidence_change_revalidate_claim` | `UPDATE OR DELETE` on `claim_evidence` | After evidence is removed or changed, re-checks that the parent claim still satisfies triggers 1 & 2 |

All four triggers are `DEFERRABLE INITIALLY DEFERRED`, meaning they evaluate at `COMMIT` time. This allows the application to insert a claim and its evidence in any order within a single transaction — the constraint is only checked once, atomically, when the transaction commits.

### What this means in practice

- If a user only appears in messages but has no distinctive behaviour → **no role/intent claim** emitted
- If a user is in a BD group but has no bio keywords or message patterns → **no role claim** beyond what evidence supports
- The system **never fabricates** information — every field in the output is either directly observed or backed by a chain of evidence

---

## Output Format

Each profile JSON has this structure:

```json
{
  "_meta": {
    "generated_at": "2026-02-06T...",
    "engine_version": "0.1.0",
    "priors_version": "v0.1.0"
  },
  "observed": {
    "user_id": 2,
    "display_name": "Alice Chen",
    "handle": "@alicechen",
    "bio": "CEO at TechStartup",
    "memberships": [
      { "group_title": "BD Chat Export", "group_kind": "bd", "first_seen": "...", "last_seen": "..." }
    ],
    "message_stats": {
      "total_messages": 3,
      "date_range": { "first": "2025-11-01", "last": "2025-11-03" }
    }
  },
  "derived": {
    "daily": [
      { "date": "2025-11-01", "msg_count": 2, "reply_count": 0, "mention_count": 2, ... }
    ],
    "aggregate": { "total_days_active": 2, "total_messages": 3, "avg_daily_messages": 1.5, ... }
  },
  "claims": [
    {
      "predicate": "has_role",
      "label": "founder_exec",
      "confidence": 0.72,
      "status": "supported",
      "evidence": [
        { "evidence_type": "bio", "evidence_ref": "bio_keyword:founder_ceo", "weight": 3 },
        { "evidence_type": "membership", "evidence_ref": "member_of:bd", "weight": 0.3 }
      ]
    }
  ]
}
```

**Key guarantees:**
- `observed` contains only directly extracted data — no inference
- `derived` contains computed aggregates — deterministic from `observed`
- `claims` always include the full `evidence` array — you can audit exactly why each label was assigned
- If no claim was emitted for a category, it is **absent** (not set to "unknown")

---

## Interpreting Profiles & Unknowns

### The three sections

| Section | What it contains | Source |
|---------|-----------------|--------|
| `observed` | Directly extracted data: display name, handle, bio text, group memberships, message counts | Telegram JSON export — zero inference |
| `derived` | Computed aggregates: daily feature vectors, totals, averages | Deterministic SQL over `observed` data |
| `claims` | Inferred labels with confidence and evidence | Inference engine output — always auditable |

### Reading a claim

Every claim entry has:
- **`predicate`**: what kind of assertion (`has_role`, `has_intent`, `has_topic_affinity`, `affiliated_with`)
- **`label`**: the asserted value (e.g., `founder_exec`, `networking`)
- **`confidence`**: softmax probability (0–1). Higher = more evidence supporting this label vs. alternatives
- **`status`**: `tentative` (meets threshold but weak) or `supported` (strong evidence)
- **`evidence`**: array of `{ evidence_type, evidence_ref, weight }` — the full audit trail

### What absence means

If a user has **no `has_role` claim**, it means one of:
1. The engine found no keyword matches in bio or messages for any role
2. The confidence for the top role was below the threshold (`minClaimConfidence`)
3. All evidence was membership-only (no bio/message/feature evidence)

In all three cases, an **abstention** was logged in `abstention_log`. To see why a claim was not emitted:

```sql
SELECT * FROM abstention_log
WHERE subject_user_id = <user_id>
ORDER BY generated_at;
```

### Confidence is relative, not absolute

A confidence of 0.72 means "72% of the softmax probability mass for this category landed on this label." It does **not** mean "72% chance this is correct." A user with only one weak keyword match might get a high confidence simply because no other label had any evidence at all. Always check the `evidence` array.

---

## DBeaver / SQL Verification

### Connection Settings

| Field | Value |
|-------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `tgprofile` |
| Username | `tgprofile` |
| Password | `localdev` |

### Useful Queries

**1. How many messages per user?**
```sql
SELECT u.display_name, COUNT(*) AS msg_count
FROM messages m
JOIN users u ON u.id = m.user_id
GROUP BY u.display_name
ORDER BY msg_count DESC;
```

**2. All claims with their evidence:**
```sql
SELECT
  u.display_name,
  c.predicate,
  c.label,
  ROUND(c.confidence::numeric, 3) AS confidence,
  c.status,
  ce.evidence_type,
  ce.evidence_ref,
  ce.weight
FROM claims c
JOIN users u ON u.id = c.user_id
JOIN claim_evidence ce ON ce.claim_id = c.id
ORDER BY u.display_name, c.predicate, ce.weight DESC;
```

**3. User features aggregated across all days:**
```sql
SELECT
  u.display_name,
  SUM(f.msg_count) AS total_msgs,
  SUM(f.reply_count) AS total_replies,
  SUM(f.mention_count) AS total_mentions,
  ROUND(AVG(f.avg_msg_len)::numeric, 1) AS avg_msg_len,
  MAX(f.groups_active_count) AS max_groups,
  ROUND(AVG(f.bd_group_msg_share)::numeric, 3) AS avg_bd_share
FROM user_features_daily f
JOIN users u ON u.id = f.user_id
GROUP BY u.display_name
ORDER BY total_msgs DESC;
```

**4. Memberships overview:**
```sql
SELECT
  u.display_name,
  g.title AS group_title,
  g.kind,
  m.first_seen_at,
  m.last_seen_at
FROM memberships m
JOIN users u ON u.id = m.user_id
JOIN groups g ON g.id = m.group_id
ORDER BY u.display_name;
```

**5. Raw import traceability:**
```sql
SELECT
  ri.source_path,
  ri.sha256,
  ri.imported_at,
  COUNT(rir.id) AS raw_rows
FROM raw_imports ri
JOIN raw_import_rows rir ON rir.raw_import_id = ri.id
GROUP BY ri.id
ORDER BY ri.imported_at;
```

**6. Gated claims — users who have NO role or intent claim:**
```sql
SELECT u.display_name, u.handle
FROM users u
WHERE NOT EXISTS (
  SELECT 1 FROM claims c
  WHERE c.user_id = u.id
    AND c.predicate IN ('has_role', 'has_intent')
);
```

**7. Abstention log — why claims were NOT emitted:**
```sql
SELECT
  u.display_name,
  a.predicate,
  a.reason_code,
  a.details,
  a.model_version,
  a.generated_at
FROM abstention_log a
JOIN users u ON u.id = a.subject_user_id
ORDER BY u.display_name, a.predicate;
```

**8. Claims with model version (idempotency check):**
```sql
SELECT
  u.display_name,
  c.predicate,
  c.label,
  c.model_version,
  COUNT(ce.id) AS evidence_count
FROM claims c
JOIN users u ON u.id = c.user_id
LEFT JOIN claim_evidence ce ON ce.claim_id = c.id
GROUP BY u.display_name, c.predicate, c.label, c.model_version
ORDER BY u.display_name;
```

---

## Constraint Verification

These queries let you **prove** the database-level anti-hallucination constraints are active. Run them in DBeaver (or any SQL client) against `localhost:5432/tgprofile`.

### List all constraint triggers

```sql
SELECT
  tg.tgname       AS trigger_name,
  cl.relname      AS table_name,
  CASE tg.tgtype & 66
    WHEN 2  THEN 'BEFORE'
    WHEN 66 THEN 'INSTEAD OF'
    ELSE 'AFTER'
  END              AS timing,
  CASE
    WHEN tg.tgtype & 4  > 0 AND tg.tgtype & 8  > 0 AND tg.tgtype & 16 > 0 THEN 'INSERT OR UPDATE OR DELETE'
    WHEN tg.tgtype & 4  > 0 AND tg.tgtype & 8  > 0 THEN 'INSERT OR UPDATE'
    WHEN tg.tgtype & 4  > 0 AND tg.tgtype & 16 > 0 THEN 'INSERT OR DELETE'
    WHEN tg.tgtype & 8  > 0 AND tg.tgtype & 16 > 0 THEN 'UPDATE OR DELETE'
    WHEN tg.tgtype & 4  > 0 THEN 'INSERT'
    WHEN tg.tgtype & 8  > 0 THEN 'UPDATE'
    WHEN tg.tgtype & 16 > 0 THEN 'DELETE'
  END              AS events,
  tg.tgdeferrable  AS deferrable,
  tg.tginitdeferred AS initially_deferred,
  p.proname        AS function_name
FROM pg_trigger tg
JOIN pg_class cl ON cl.oid = tg.tgrelid
JOIN pg_proc  p  ON p.oid = tg.tgfoid
WHERE cl.relname IN ('claims', 'claim_evidence')
  AND NOT tg.tgisinternal
ORDER BY cl.relname, tg.tgname;
```

**Expected output: 4 rows**, all with `deferrable = true` and `initially_deferred = true`:

| trigger_name | table_name | timing | events | deferrable | initially_deferred |
|---|---|---|---|---|---|
| `claim_must_have_evidence` | claims | AFTER | INSERT OR UPDATE | true | true |
| `claim_needs_real_evidence` | claims | AFTER | INSERT OR UPDATE | true | true |
| `claim_validate_object_value` | claims | AFTER | INSERT OR UPDATE | true | true |
| `evidence_change_revalidate_claim` | claim_evidence | AFTER | UPDATE OR DELETE | true | true |

### Inspect trigger function source

To read the body of any trigger function:

```sql
SELECT prosrc
FROM pg_proc
WHERE proname = 'trg_claim_must_have_evidence';
-- Replace with: trg_claim_needs_real_evidence, trg_claim_validate_object_value,
--               trg_evidence_change_revalidate_claim
```

### Verify the unique index (idempotent upserts)

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'claims'
  AND indexname = 'idx_claims_unique_per_version';
```

**Expected:** one row with `CREATE UNIQUE INDEX idx_claims_unique_per_version ON public.claims USING btree (subject_user_id, predicate, object_value, model_version)`.

---

## Negative Test Runbook

These SQL scripts prove the constraint triggers **actually reject** invalid data. Run each one — every test should fail with a `RAISE EXCEPTION` error at `COMMIT`.

> ⚠️ Each test runs in its own transaction and is designed to **fail**. Your SQL client will show an error — that is the expected outcome.

### Test 1 — Bare claim (no evidence)

A claim with zero evidence rows must be rejected by `claim_must_have_evidence`.

```sql
BEGIN;
  INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version)
  SELECT id, 'has_role', 'builder', 0.5, 'tentative', 'test'
  FROM users LIMIT 1;
COMMIT;  -- ❌ ERROR: Claim <id> has no evidence rows
```

### Test 2 — `has_topic_affinity` with membership-only evidence

`has_topic_affinity` requires non-membership evidence (enforced since Phase B).

```sql
BEGIN;
  WITH ins AS (
    INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version)
    SELECT id, 'has_topic_affinity', 'DeFi', 0.5, 'tentative', 'test'
    FROM users LIMIT 1
    RETURNING id
  )
  INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
  SELECT ins.id, 'membership', 'member_of:bd', 0.3 FROM ins;
COMMIT;  -- ❌ ERROR: Claim <id> (has_topic_affinity) has no non-membership evidence
```

### Test 3 — `has_role` with membership-only evidence

```sql
BEGIN;
  WITH ins AS (
    INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version)
    SELECT id, 'has_role', 'builder', 0.5, 'tentative', 'test'
    FROM users LIMIT 1
    RETURNING id
  )
  INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
  SELECT ins.id, 'membership', 'member_of:bd', 0.3 FROM ins;
COMMIT;  -- ❌ ERROR: Claim <id> (has_role) has no non-membership evidence
```

### Test 4a — `has_role` with invalid ENUM value

`object_value` must be a valid `role_label` ENUM member.

```sql
BEGIN;
  WITH ins AS (
    INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version)
    SELECT id, 'has_role', 'buildre', 0.5, 'tentative', 'test'  -- typo!
    FROM users LIMIT 1
    RETURNING id
  )
  INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
  SELECT ins.id, 'bio', 'bio_keyword:builder', 3.0 FROM ins;
COMMIT;  -- ❌ ERROR: invalid input value for enum role_label: "buildre"
```

### Test 4b — `has_intent` with invalid ENUM value

```sql
BEGIN;
  WITH ins AS (
    INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version)
    SELECT id, 'has_intent', 'vibing', 0.5, 'tentative', 'test'  -- not a real intent
    FROM users LIMIT 1
    RETURNING id
  )
  INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
  SELECT ins.id, 'bio', 'bio_keyword:networking', 3.0 FROM ins;
COMMIT;  -- ❌ ERROR: invalid input value for enum intent_label: "vibing"
```

### Test 4c — `affiliated_with` empty value

Free-text predicates must have a non-empty `object_value`.

```sql
BEGIN;
  WITH ins AS (
    INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version)
    SELECT id, 'affiliated_with', '', 0.5, 'tentative', 'test'  -- empty!
    FROM users LIMIT 1
    RETURNING id
  )
  INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
  SELECT ins.id, 'bio', 'bio_keyword:org', 3.0 FROM ins;
COMMIT;  -- ❌ ERROR: Claim <id> (affiliated_with) has empty object_value
```

### Test 5 — UPDATE predicate to bypass trigger

Updating an existing claim's predicate must be re-validated.

```sql
BEGIN;
  UPDATE claims
  SET predicate = 'has_topic_affinity'
  WHERE predicate = 'affiliated_with'
    AND id = (SELECT id FROM claims WHERE predicate = 'affiliated_with' LIMIT 1);
COMMIT;  -- ❌ ERROR: Claim <id> (has_topic_affinity) has no non-membership evidence
-- (Fails because the existing evidence was all membership-type, or the claim doesn't exist)
```

> **Note:** Test 5 only applies if there is an `affiliated_with` claim in the database. If not, the UPDATE is a no-op and the test is vacuously safe.

### Positive test — valid claim passes

This test should **succeed** (no error):

```sql
BEGIN;
  WITH ins AS (
    INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version)
    SELECT id, 'has_role', 'builder', 0.65, 'supported', 'test-positive'
    FROM users LIMIT 1
    RETURNING id
  )
  INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
  VALUES
    ((SELECT id FROM ins), 'bio', 'bio_keyword:builder', 3.0),
    ((SELECT id FROM ins), 'membership', 'member_of:bd', 0.3);
COMMIT;  -- ✅ SUCCESS

-- Cleanup:
DELETE FROM claims WHERE model_version = 'test-positive';
```

---

## Configuration

### Environment Variables (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `tgprofile` | Database username |
| `POSTGRES_PASSWORD` | `localdev` | Database password |
| `POSTGRES_DB` | `tgprofile` | Database name |
| `POSTGRES_PORT` | `5432` | Host port for Postgres |
| `INFERENCE_CONFIG` | `config/inference.v0.2.0.json` | Path to versioned inference config file |

### Inference Config (`config/inference.v0.2.0.json`)

The inference engine is configured via a **versioned JSON file**. This is the single source of truth for priors, gating thresholds, and model version:

```json
{
  "version": "v0.2.0",
  "gating": {
    "minNonMembershipEvidence": 1,
    "minClaimConfidence": 0.15
  },
  "rolePriors": {
    "bd": { "bd": 0.5, "builder": 0.1, ... },
    ...
  },
  "intentPriors": { ... }
}
```

| Field | Purpose |
|-------|---------|
| `version` | Stamped on every claim and abstention → ties output to config |
| `gating.minNonMembershipEvidence` | Minimum non-membership evidence rows required for role/intent/topic claims |
| `gating.minClaimConfidence` | Minimum softmax probability to emit a claim |
| `rolePriors[groupKind][roleLabel]` | Additive prior weight for each role given group membership |
| `intentPriors[groupKind][intentLabel]` | Additive prior weight for each intent given group membership |

To create a new config version: copy the file, change `version`, adjust weights, and set `INFERENCE_CONFIG` to point to the new file. Old claims (tagged with the old version) remain untouched.

### Application Config (`src/config/app-config.ts`)

| Setting | Default | Description |
|---------|---------|-------------|
| `dropRawTextAfterFeatures` | `false` | If true, scrub raw text after feature computation |
| `pseudonymizeExternalIds` | `false` | If true, hash external IDs before storage |

---

## Project Structure

```
telethon/
├── config/
│   └── inference.v0.2.0.json     # Versioned inference config (priors + gating)
├── data/
│   ├── exports/          # Place Telegram JSON exports here (gitignored)
│   └── output/           # Generated profiles appear here (gitignored)
├── db/
│   └── migrations/
│       ├── 20260206120000_create_schema.sql
│       ├── 20260206120100_claim_evidence_triggers.sql
│       ├── 20260206130000_harden_claim_constraints.sql
│       ├── 20260206140000_abstention_log_and_claims_unique.sql
│       └── 20260206150000_unique_messages_per_group.sql
├── share/                # Diagnostics output (gitignored)
├── src/
│   ├── cli/
│   │   ├── ingest.ts
│   │   ├── compute-features.ts
│   │   ├── infer-claims.ts       # Loads config, writes claims + abstentions
│   │   ├── export-profiles.ts
│   │   └── export-diagnostics.ts # Share-safe diagnostics (no PII)
│   ├── config/
│   │   ├── taxonomies.ts         # ENUMs & type definitions
│   │   ├── priors.ts             # Legacy priors (no longer imported by engine)
│   │   ├── app-config.ts         # Feature flags
│   │   └── inference-config.ts   # Loads versioned JSON config
│   ├── db/
│   │   └── index.ts              # Postgres connection pool
│   ├── inference/
│   │   ├── engine.ts             # Deterministic scoring + claim/abstention writes
│   │   └── keywords.ts           # Bio/message keyword dictionaries
│   ├── parsers/
│   │   └── telegram.ts           # Zod schemas for Telegram + Telethon exports
│   └── utils.ts                  # SHA-256, arg parsing, helpers
├── tools/
│   └── telethon_collector/
│       ├── collect_group_export.py  # Telethon API collector
│       ├── list_dialogs.py          # List all Telegram dialogs
│       ├── requirements.txt         # Python deps (telethon, python-dotenv)
│       ├── .env.example             # Credential template
│       └── README.md                # Collector-specific docs
├── .env                          # Local database credentials (gitignored)
├── .gitignore
├── docker-compose.yml
├── Makefile
├── package.json
├── tsconfig.json
└── README.md                     # ← You are here
```

---

## Ethics & Privacy

This tool is designed for **legitimate, personal research** on chat data you have the right to access.

### What this tool does NOT do:
- ❌ Contact any external API or service
- ❌ Send data anywhere outside your machine
- ❌ Use machine learning or LLMs for inference
- ❌ Guess or fabricate information about users
- ❌ Store credentials or authentication tokens

### What this tool DOES do:
- ✅ Process only files you explicitly provide
- ✅ Store all data in a local Docker container
- ✅ Back every inference with auditable evidence
- ✅ Default to "unknown" when evidence is insufficient
- ✅ Provide full traceability from claim → evidence → raw data

### Responsible use guidelines:
1. **Only analyse chats you have legitimate access to** — either your own groups or groups where you have explicit permission
2. **Do not share output profiles** without consent of the individuals described
3. **Review claims critically** — even with evidence gating, the keyword-based system can produce incorrect labels. Always verify before acting on any claim.
4. **Use `dropRawTextAfterFeatures`** if you want to minimise raw text retention after analysis
5. **Use `pseudonymizeExternalIds`** if you want to decouple profiles from real Telegram IDs

---

## Reset & Cleanup

```bash
# Destroy everything (database + volumes) and start fresh
make db-reset

# Just stop the database (data persists)
make db-down

# Remove generated output
rm -rf data/output/*.json
```

---

## License

Private / personal use. Not distributed.
