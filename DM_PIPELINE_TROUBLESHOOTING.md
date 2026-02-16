# Telegram DM Pipeline Troubleshooting & Ideal Solution

This document summarizes the issues hit during the DM live pipeline stabilization work and the intended final behavior.

## Current Intended Behavior
- Listener captures inbound DMs to `data/exports/telethon_dms_live.jsonl`.
- Ingest job reads JSONL into `dm_messages`.
- Responder worker handles rows in `dm_messages` with `response_status = 'pending'` / `'failed'`.
- Sent replies are marked as `response_status='responded'` and linked via `response_message_external_id`.
- Duplicate responses are suppressed via SQL and in-loop checks.

## Troubles observed
1. **Resurrected lock/process overlap**
   - Multiple runs and background supervisor/Makefile interactions left stale lock/PID artifacts.
   - Listener occasionally saw missing/invalid `data/logs/*` paths and exited, causing pipeline to stop processing without clear action.

2. **Responder contention with listener session**
   - Concurrent Telethon session usage produced `sqlite3.OperationalError: database is locked`.
   - Early fix via file lock worked but skipped responder too aggressively.
   - Reworked to stop listener briefly during response cycle in live loop to avoid lock contention.

3. **Duplicate replies per inbound message**
   - Real duplicate outbound sends happened when duplicate pending rows were present in one responder batch.
   - Existing SQL-level `NOT EXISTS` and exact-text checks helped, but duplicate batch signatures still slipped through.
   - Added in-batch signature suppression.

4. **“No response” complaints while DB showed responded**
   - In user-facing cases, the outbound text was generic capture acknowledgment while user expected a data-collection question.
   - Ensured dataset-focused prompt is the default and aligned between `run-dm-response.sh` and `run-dm-live.sh`.

5. **Intermittent “nothing happens” reports**
   - Often due to listener being offline after restart churn, resulting in empty JSONL and zero newly ingested rows.
   - `tg-live-stop` + restart + clean lock/state handling required.

## Ideal architecture/operations to make this robust
- **Single source of truth for templates**
  - Keep defaults centralized in one place and avoid duplicated fallback strings across scripts.

- **Deterministic process lifecycle**
  - On startup, validate and recreate expected directories/files (`data/.pids`, `data/logs`, `data/.state`, exports path).
  - If stale pids/locks exist, clear after validation and log explicit cleanup.
  - Add readiness checks so listener must be healthy before assuming response loop correctness.

- **Explicit duplicate prevention policy**
  - Keep DB uniqueness semantics (preferably one response per `(conversation_id, inbound_message_window)`), not just per-row.
  - Continue both in-band SQL guard (`NOT EXISTS` outbound at/after inbound) and in-batch guard.

- **Backpressure-safe locking**
  - Prefer short, scoped serializing behavior around listener↔responder session access instead of long-lived skips.
  - Avoid silent no-op lock skips unless explicit rationale.

- **Clear observability**
  - Persist cycle logs with one line per phase: capture, ingest, reconcile, claim, send/skips, final stats.
  - Emit explicit alerts on “file path missing”, “no new rows”, and “listener not running” events.

- **Operational runbook**
  1. `make tg-live-stop`
  2. remove stale `data/.pids/*` and ensure paths exist
  3. `make tg-live-start FILE=... INTERVAL=10 RESPONSE_ENABLED=1`
  4. send a real test DM
  5. verify: inbound row exists + responded outbound exists within one cycle.

## Commit-level changes made
- `f46ab68` — responder locking and duplicate handling hardening.
- `13d0021` — dedupe and retry hardening.
- `0b51be0` — pre-send idempotence guards.
- `0cbdab9` — session lock serialization (listener/responder).
- `a6445fa` — live loop pauses listener during response cycle.
- `c90d659` — default response template changed to dataset-collection wording.
- `27675b0` — align live-loop default template with responder default.
- `c34f845` — in-batch duplicate suppression in responder.

## Current recommendation
- Keep template-driven replies and continue with a single-run supervisor pattern.
- Add a small health job that checks listener up + newest export size > 0 + most recent inbound row age before claiming response pipeline health.
