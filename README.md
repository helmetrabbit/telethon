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

For continuous context, run ingest in a loop while you keep the listener on in another terminal:

```bash
make tg-listen-ingest-dm file=data/exports/telethon_dms_live.jsonl interval=30
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
