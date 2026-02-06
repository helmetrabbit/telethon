# tg-profile-engine

**Local-only Telegram chat analysis with an ontology-lite knowledge graph, evidence-gated inference, and anti-hallucination constraints.**

This system ingests [Telegram Desktop JSON exports](https://telegram.org/blog/export-and-more), stores them in a normalised PostgreSQL schema, computes per-user behavioural features, runs a deterministic inference engine that assigns role/intent labels, and exports self-contained profile documents where **every claim is backed by traceable evidence**.

> ⚠️ **Privacy first** — all data stays on your machine. No external APIs, no cloud services, no telemetry. The database runs in a local Docker container and nothing ever leaves `localhost`.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Pipeline Steps](#pipeline-steps)
5. [Schema Overview](#schema-overview)
6. [Inference Engine](#inference-engine)
7. [Evidence Gating & Anti-Hallucination](#evidence-gating--anti-hallucination)
8. [Output Format](#output-format)
9. [DBeaver / SQL Verification](#dbeaver--sql-verification)
10. [Configuration](#configuration)
11. [Project Structure](#project-structure)
12. [Ethics & Privacy](#ethics--privacy)

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
1. **Minimum confidence**: `P(label) ≥ 0.15` (configurable via `minClaimConfidence`)
2. **Minimum non-membership evidence**: role and intent claims require at least 1 evidence row that is **not** just group membership (configurable via `minNonMembershipEvidence`)

If either check fails, the claim is **silently dropped** — the user's role/intent simply stays unassigned rather than guessing.

### Database-Level Triggers

Two `DEFERRABLE INITIALLY DEFERRED` constraint triggers act as a safety net:

1. **`claim_must_have_evidence`** — every claim must have ≥1 `claim_evidence` row. A bare claim with no evidence is rejected at `COMMIT`.
2. **`claim_role_intent_needs_real_evidence`** — `has_role` / `has_intent` claims must have at least one evidence row whose type is **not** `membership`. This prevents the system from labelling someone purely because they appeared in a certain group.

Both triggers fire at transaction commit time, ensuring atomicity.

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

---

## Configuration

### Environment Variables (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `tgprofile` | Database username |
| `POSTGRES_PASSWORD` | `localdev` | Database password |
| `POSTGRES_DB` | `tgprofile` | Database name |
| `POSTGRES_PORT` | `5432` | Host port for Postgres |

### Application Config (`src/config/app-config.ts`)

| Setting | Default | Description |
|---------|---------|-------------|
| `dropRawTextAfterFeatures` | `false` | If true, scrub raw text after feature computation |
| `pseudonymizeExternalIds` | `false` | If true, hash external IDs before storage |
| `minNonMembershipEvidence` | `1` | Minimum non-membership evidence for role/intent claims |
| `minClaimConfidence` | `0.15` | Minimum softmax probability to emit a claim |

### Priors (`src/config/priors.ts`)

Role and intent priors are configured per `group_kind`. These are small additive weights (0.1–0.5) that nudge the scoring based on what kind of group a user appears in. They are **not** sufficient on their own to generate a claim — additional evidence is always required.

---

## Project Structure

```
telethon/
├── data/
│   ├── exports/          # Place Telegram JSON exports here (gitignored)
│   └── output/           # Generated profiles appear here (gitignored)
├── db/
│   └── migrations/
│       ├── 20260206120000_create_schema.sql
│       └── 20260206120100_claim_evidence_triggers.sql
├── src/
│   ├── cli/
│   │   ├── ingest.ts
│   │   ├── compute-features.ts
│   │   ├── infer-claims.ts
│   │   └── export-profiles.ts
│   ├── config/
│   │   ├── taxonomies.ts       # ENUMs & type definitions
│   │   ├── priors.ts           # Role/intent priors per group_kind
│   │   └── app-config.ts       # Feature flags & thresholds
│   ├── db/
│   │   └── index.ts            # Postgres connection pool
│   ├── inference/
│   │   ├── engine.ts           # Deterministic scoring engine
│   │   └── keywords.ts         # Bio/message keyword dictionaries
│   ├── parsers/
│   │   └── telegram.ts         # Zod schemas for Telegram export JSON
│   └── utils.ts                # SHA-256, arg parsing, helpers
├── .env                        # Local database credentials (gitignored)
├── .gitignore
├── docker-compose.yml
├── Makefile
├── package.json
├── tsconfig.json
└── README.md                   # ← You are here
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
