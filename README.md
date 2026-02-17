# tg-profile-engine (code-first)

Local Telegram chat analysis pipeline. This README is derived from the **current code**, not historical docs.

## What It Does (Actual Pipeline)

1. **Collect** Telegram data (JSON export)
2. **Bulk ingest** into Postgres
3. **Compute daily features**
4. **Optionally scrape bios** from public `t.me/<handle>`
5. **LLM psychographic enrichment** via OpenRouter
6. **Export a static viewer** (`viewer/index.html`)

## Quick Start

### 1. Install

```bash
npm install
```

### 2. Configure DB (remote by default)

This repo connects to Postgres via `DATABASE_URL` in `.env`.

Remote (OpenClaw server over Tailscale):

```bash
cp -n .env.example .env
npm run env:remote
npm run db:smoke
```

If MagicDNS is flaky, use the Tailnet IP instead:

```bash
npm run env:remote:ip
npm run db:smoke
```

Local Postgres (optional, via Docker):

```bash
npm run env:local
make db-up
make db-migrate
```


### 3. Data collection strategy (important)

- **Groups:** manual export is the reliable path (historical coverage, no group-access surprises).
- **DMs:** can use live collection safely, private chats only.

Manual group export:

```bash
source tools/telethon_collector/.venv/bin/activate
python tools/telethon_collector/collect_group_export.py --group "<group name or @username>" --out data/exports/<group>.json
```

Live DM capture:

```bash
make tg-listen-dm
# Writes to: data/exports/telethon_dms_live.jsonl
```

Ingest live DM stream into Postgres (separate from group tables):

```bash
make tg-ingest-dm-jsonl file=data/exports/telethon_dms_live.jsonl
# or: npm run ingest-dm-jsonl -- --file data/exports/telethon_dms_live.jsonl
```

Optional (recommended) profile fact extraction via OpenRouter during DM ingest:
- `OPENROUTER_API_KEY=<key>` (set it in `.env`, which is gitignored)
- `DM_PROFILE_LLM_EXTRACTION=auto` (default behavior: auto-enable when key exists)
- `DM_PROFILE_LLM_MODEL=deepseek/deepseek-chat`

Repo runtime note:
- `openclaw.env` is loaded by the live pipeline scripts and Node bootstrap. Keep **non-secret** defaults there (models, thresholds). Put secrets like `OPENROUTER_API_KEY` in `.env` (gitignored).

The live ingest supports resumable checkpoints so repeated runs only process new rows:

```bash
# explicit checkpoint path (optional; defaults to <jsonl>.checkpoint.json)
npm run ingest-dm-jsonl -- --file data/exports/telethon_dms_live.jsonl --state-file data/.state/dm-live.state.json
```

Start fully automatic live DM pipeline (listener + periodic ingest + reconciler):

```bash
make tg-live-start
# optional overrides:
#   FILE=data/exports/telethon_dms_live.jsonl
#   INTERVAL=10
#   STATE_FILE=data/.state/dm-live.state.json
#   DM_RESPONSE_MODE=conversational   # conversational|template
#   DM_PERSONA_NAME="Lobster Llama"
```

For ingest-only automation (no profile reconciliation):

```bash
make tg-live-start-ingest
```

For automatic *DM profile correction reconciliation* (company/role corrections from inbound chat):

```bash
make tg-listen-ingest-dm-profile FILE=data/exports/telethon_dms_live.jsonl INTERVAL=10
# or one-off reconcile run:
make tg-reconcile-dm-psych
# optionally: make tg-reconcile-dm-psych userIds=1,2011 limit=5
```

Health and lifecycle:

```bash
make tg-live-status
make tg-live-stop
```

If you need a clean replay (reprocess full file), reset checkpoints:

```bash
make tg-live-state-reset FILE=data/exports/telethon_dms_live.jsonl STATE_FILE=data/.state/dm-live.state.json
```

Check and stop:

```bash
make tg-live-status
make tg-live-stop
```

If this is your first DM ingest, run migrations first:

```bash
make db-migrate
```

### 4. Put a Telegram JSON export in place

```
data/exports/your_chat.json
```

### 5. Run the pipeline

```bash
# One-shot pipeline
npm run pipeline

# Or step by step:
npm run build
npm run bulk-ingest -- --file data/exports/your_chat.json
npm run backfill-message-names # high-precision backfill from export sender names
npm run backfill-telethon-names # optional; fills missing names (best for no-handle users)
npm run compute-features
npm run scrape-bios           # optional
npm run enrich-psycho
npm run export-viewer
```

### 5. Open the viewer

Open `viewer/index.html` in a browser.

## CLI Commands (from `package.json`)

```bash
npm run bulk-ingest      # Ingest a single JSON export into Postgres
npm run backfill-message-names # Name backfill from Telegram export sender strings
npm run backfill-telethon-names # Telethon-based display-name backfill
npm run compute-features # Per-user daily aggregates
npm run scrape-bios      # Scrape public bios from t.me
npm run ingest-dm-jsonl # Ingest DM JSONL from live/private collector
npm run reconcile-dm-psych # Reconcile profile updates from DM correction events
npm run enrich-psycho    # LLM psychographic profiling
npm run export-viewer    # Export viewer/data.js
```

## Ingestion (Code Behavior)

`src/cli/bulk-ingest.ts`:
- Reads a single Telegram JSON export.
- Upserts `groups` and `users`.
- COPY loads `messages` for speed.
- Extracts @mentions into `message_mentions`.
- Backfills `memberships` from messages.

**Notes**:
- Raw traceability tables (`raw_imports`, `raw_import_rows`) are not populated by this ingest path.
- There is no dedup/upsert on messages beyond DB-level unique constraints.

## Feature Computation

`src/cli/compute-features.ts` writes `user_features_daily` using a CTE:
- `msg_count`, `reply_count`, `mention_count`
- `avg_msg_len`, `groups_active_count`
- `bd_group_msg_share`

Mentions come from `message_mentions` populated during bulk ingest.

## Bio Scraping

`src/cli/scrape-bios.ts`:
- Fetches `https://t.me/<handle>` for users missing a bio/name.
- Extracts both bio and display name from HTML (with retries/backoff).
- Updates `users.bio` and missing `users.display_name` when scrapeable.

## Psychographic Enrichment (LLM)

`src/cli/enrich-psycho.ts`:
- Builds a message sample per user (weighted by group importance).
- Includes existing `claims` **if present**.
- Calls OpenRouter (`src/inference/llm-client.ts`).
- Validates outputs against strict enums (`src/inference/psycho-prompt.ts`).
- Writes to `user_psychographics`.

**Keys**:
- Requires `process.env.OPENROUTER_API_KEY` for LLM calls (set it in `.env`).

## Viewer Export

`src/cli/export-viewer.ts`:
- Exports `claims` (by model version), `user_psychographics`, and activity heatmaps.
- Writes a single JS file: `viewer/data.js`.
- `viewer/index.html` is a static Tailwind UI that consumes it.

## Claims and Deterministic Inference

The schema supports `claims`, `claim_evidence`, and `abstention_log`, but **this repo does not currently include a deterministic inference engine that writes them**. If you already have `claims` in the DB (from an external process), they will be included in the viewer and in LLM prompts.

## Telethon Collector (Python)

`tools/telethon_collector/` uses Telethon to export:
- Messages
- Participants (when allowed)

Output JSON is compatible with `bulk-ingest.ts`.

## Database Overview (Current Schema)

Core tables used by code:
- `users`, `groups`, `memberships`, `messages`, `message_mentions`
- `user_features_daily`
- `user_psychographics`
- `claims` (read-only in this codebase)

## Known Gaps (Code Reality)

- Raw import traceability tables are unused.
- No in-repo job writes `claims`/`claim_evidence`.
- `app-config.ts` exists but is unused.

If you want these filled in, ask and I’ll implement them.

## Automated DM Pipeline (recommended)

For 24/7 unattended enrichment, use the long-running supervisor:

```bash
# Start once (runs listener, resumable ingestion, and optional reconcile)
# Includes a preflight check for venv/session/DB wiring before launch.
make tg-live-start

# Check health
make tg-live-status

# Stop cleanly
make tg-live-stop
```

Supervisor semantics:
- keeps a single listener process alive and restarts it if it exits
- ingests new DM lines with a checkpoint file (`.checkpoint.json` or `STATE_FILE` override)
- runs reconciliation in the same cycle when started with `tg-live-start`
- uses exponential-ish backoff after failures

Important behavior:
- This pipeline handles capture, ingest, profile reconciliation, and **automated pending-response handling**.
- Inbound messages are queued as `pending` when first ingested.
- Outbound workers can send a lightweight response template and mark messages as `responded`.
- First-contact DM users are auto-onboarded with persisted state in `dm_profile_state`
  (`onboarding_status`, required/missing fields, last prompted field, started/completed timestamps).
- `DM_AUTO_ACK` defaults to off (`0`) and is optional; enable it with `DM_AUTO_ACK=1` only when you want an immediate acknowledgement on each inbound DM.
- If you stop getting listener output and see `EOFError: EOF when reading a line` or repeated
  "Please enter your phone", the Telegram session needs interactive re-auth once.

Optional one-shot responder:

```bash
make tg-respond-dm [limit=20] [max_retries=3]
```

Responder variables:
- `DM_RESPONSE_TEMPLATE`: custom response message template.
- `DM_MAX_RETRIES`: max retry count per inbound message.
- `DM_RESPONSE_DRY_RUN=1`: dry-run mode without sending.
- `DM_RESPONSE_LLM_ENABLED=1`: enable OpenRouter conversational replies (fallbacks to deterministic replies on error).
- `DM_RESPONSE_MODEL=deepseek/deepseek-chat`
- `DM_RESPONSE_MAX_TOKENS=420`
- `DM_RESPONSE_TEMPERATURE=0.15`
- `DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD=0.8`: auto-apply communication-style updates at/above this confidence.
- `DM_CONTACT_STYLE_CONFIRM_THRESHOLD=0.55`: below auto-apply threshold, style updates become pending and require a `yes/no` confirmation.
- `DM_CONTACT_STYLE_TTL_DAYS=45`: days before style preference is considered stale and re-confirmed.
- `DM_CONTACT_STYLE_RECONFIRM_COOLDOWN_DAYS=14`: minimum spacing between stale-style reconfirm prompts.
- Onboarding is deterministic and LLM-independent for new/collecting users until core profile fields are captured.

Operational tuning runbook: `runbook.md` (includes copy/paste metric queries and threshold adjustment workflow).

To run under systemd, call `make tg-live-start` from a service that stays running; logs are written to `data/logs/` and state is persisted in `data/.state/`.

Example systemd unit bootstrap (adjust paths/user):

```bash
sudo cp tools/telethon_collector/systemd/tg-dm-live.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tg-dm-live
```
