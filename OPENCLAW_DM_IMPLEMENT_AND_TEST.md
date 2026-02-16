# OpenClaw DM Pipeline: Implement + Test Runbook

This runbook is designed for OpenClaw to execute end-to-end so the DM pipeline is working without manual debugging loops.

## Goal

Achieve a **working** DM system where:

1. inbound DMs are ingested,
2. `Lobster Llama` responds in conversational mode,
3. profile clarifications update `user_psychographics`,
4. checks are validated with hard pass/fail criteria.

---

## Definition of Done

All of the following are true:

1. build and migrations succeed.
2. deterministic fixture ingest creates DM rows and profile events.
3. reconcile writes updated `primary_role`, `primary_company`, and at least one of `preferred_contact_style` or `notable_topics`.
4. responder run (dry-run) marks at least 2 fixture inbound rows as `response_status='responded'`.
5. no command in this runbook fails.

---

## 0. Environment Setup

Run from repo root:

```bash
cd /Users/prestonsmith/telethon
set -euo pipefail
set -a
[ -f .env ] && source .env
[ -f tools/telethon_collector/.env ] && source tools/telethon_collector/.env
set +a
```

Sanity checks:

```bash
test -n "${DATABASE_URL:-${PG_DSN:-}}" || (echo "DATABASE_URL/PG_DSN is missing" && exit 1)
test -n "${TG_API_ID:-}" || (echo "TG_API_ID is missing" && exit 1)
test -n "${TG_API_HASH:-}" || (echo "TG_API_HASH is missing" && exit 1)
test -f "tools/telethon_collector/.venv/bin/python" || (echo "Missing tools/telethon_collector/.venv" && exit 1)
```

---

## 1. Build + Migrate

```bash
npm run build
make db-migrate
```

---

## 2. Implementation Guard Checks

These checks ensure required code paths exist before testing:

```bash
rg -n "DM_RESPONSE_MODE|DM_PERSONA_NAME|conversational" tools/telethon_collector/run-dm-live.sh tools/telethon_collector/run-dm-response.sh tools/telethon_collector/respond-dm-pending.py
rg -n "render_conversational_reply|fetch_latest_profile|--persona-name|--mode" tools/telethon_collector/respond-dm-pending.py
rg -n "preferred_contact_style|notable_topics|primary_role" src/cli/ingest-dm-jsonl.ts src/cli/reconcile-dm-psychometry.ts
```

If any check fails, implement missing logic before proceeding.

---

## 3. Deterministic Fixture Test (No Telegram Sending Required)

Use isolated fixture/state paths:

```bash
TEST_JSONL="data/exports/telethon_dms_openclaw_test.jsonl"
TEST_STATE="data/.state/dm-openclaw-test.state.json"
mkdir -p data/exports data/.state data/logs
rm -f "$TEST_JSONL" "$TEST_STATE"
```

Create fixture events:

```bash
cat > "$TEST_JSONL" <<'JSONL'
{"message_id":910001,"chat_id":"9001","account_id":"user999999","direction":"inbound","sender_id":"user111111","sender_name":"Casey Builder","sender_username":"caseyb","peer_id":"user999999","peer_name":"Lobster Llama","peer_username":"lobsterllama","text":"I'm a Product Manager at OpenClaw Labs.","text_len":40,"date":"2026-02-16T20:00:00+00:00","reply_to_message_id":null,"views":0,"forwards":0,"has_links":false,"has_mentions":false}
{"message_id":910002,"chat_id":"9001","account_id":"user999999","direction":"inbound","sender_id":"user111111","sender_name":"Casey Builder","sender_username":"caseyb","peer_id":"user999999","peer_name":"Lobster Llama","peer_username":"lobsterllama","text":"My priorities are partnerships, growth, and founder intros.","text_len":60,"date":"2026-02-16T20:01:00+00:00","reply_to_message_id":null,"views":0,"forwards":0,"has_links":false,"has_mentions":false}
{"message_id":910003,"chat_id":"9001","account_id":"user999999","direction":"inbound","sender_id":"user111111","sender_name":"Casey Builder","sender_username":"caseyb","peer_id":"user999999","peer_name":"Lobster Llama","peer_username":"lobsterllama","text":"Best way to reach me is concise Telegram DMs.","text_len":45,"date":"2026-02-16T20:02:00+00:00","reply_to_message_id":null,"views":0,"forwards":0,"has_links":false,"has_mentions":false}
{"message_id":920001,"chat_id":"9002","account_id":"user999999","direction":"inbound","sender_id":"user222222","sender_name":"Morgan Ops","sender_username":"morganops","peer_id":"user999999","peer_name":"Lobster Llama","peer_username":"lobsterllama","text":"I work as a Founder at Seastar Network. My priorities are hiring and BD.","text_len":74,"date":"2026-02-16T20:03:00+00:00","reply_to_message_id":null,"views":0,"forwards":0,"has_links":false,"has_mentions":false}
JSONL
```

Run ingest + reconcile:

```bash
npm run ingest-dm-jsonl -- --file "$TEST_JSONL" --state-file "$TEST_STATE"
npm run reconcile-dm-psych
```

Run responder in dry-run conversational mode:

```bash
DM_RESPONSE_DRY_RUN=1 \
DM_RESPONSE_MODE=conversational \
DM_PERSONA_NAME="Lobster Llama" \
DM_RESPONSE_LIMIT=20 \
DM_MAX_RETRIES=3 \
bash tools/telethon_collector/run-dm-response.sh
```

---

## 4. Pass/Fail Validation Queries

Run validations:

```bash
DB_URL="${DATABASE_URL:-${PG_DSN:-}}"

psql "$DB_URL" -v ON_ERROR_STOP=1 -c "
SELECT response_status, count(*) AS cnt
FROM dm_messages
WHERE external_message_id IN ('910001','910002','910003','920001')
GROUP BY 1
ORDER BY 1;
"

psql "$DB_URL" -v ON_ERROR_STOP=1 -c "
SELECT u.external_id, p.primary_role, p.primary_company, p.preferred_contact_style, p.notable_topics, p.created_at
FROM users u
JOIN LATERAL (
  SELECT primary_role, primary_company, preferred_contact_style, notable_topics, created_at
  FROM user_psychographics
  WHERE user_id = u.id
  ORDER BY created_at DESC, id DESC
  LIMIT 1
) p ON TRUE
WHERE u.external_id IN ('user111111','user222222')
ORDER BY u.external_id;
"

psql "$DB_URL" -v ON_ERROR_STOP=1 -c "
SELECT event_type, processed, count(*) AS cnt
FROM dm_profile_update_events
WHERE source_external_message_id IN ('910001','910002','910003','920001')
GROUP BY 1,2
ORDER BY 1,2;
"
```

Hard checks:

```bash
RESPONDED_COUNT=$(psql "$DB_URL" -Atc "
SELECT count(*)
FROM dm_messages
WHERE external_message_id IN ('910001','920001')
  AND response_status='responded';
")

ROLE_COMPANY_COUNT=$(psql "$DB_URL" -Atc "
SELECT count(*)
FROM users u
JOIN LATERAL (
  SELECT primary_role, primary_company
  FROM user_psychographics
  WHERE user_id = u.id
  ORDER BY created_at DESC, id DESC
  LIMIT 1
) p ON TRUE
WHERE u.external_id IN ('user111111','user222222')
  AND p.primary_role IS NOT NULL
  AND p.primary_company IS NOT NULL;
")

STYLE_OR_TOPICS_COUNT=$(psql "$DB_URL" -Atc "
SELECT count(*)
FROM users u
JOIN LATERAL (
  SELECT preferred_contact_style, notable_topics
  FROM user_psychographics
  WHERE user_id = u.id
  ORDER BY created_at DESC, id DESC
  LIMIT 1
) p ON TRUE
WHERE u.external_id IN ('user111111','user222222')
  AND (
    p.preferred_contact_style IS NOT NULL
    OR jsonb_array_length(COALESCE(p.notable_topics, '[]'::jsonb)) > 0
  );
")

echo "RESPONDED_COUNT=$RESPONDED_COUNT"
echo "ROLE_COMPANY_COUNT=$ROLE_COMPANY_COUNT"
echo "STYLE_OR_TOPICS_COUNT=$STYLE_OR_TOPICS_COUNT"

test "${RESPONDED_COUNT}" -ge 2
test "${ROLE_COMPANY_COUNT}" -ge 1
test "${STYLE_OR_TOPICS_COUNT}" -ge 1
```

If all three tests pass, deterministic pipeline behavior is validated.

---

## 5. Optional Live Telegram Smoke Test

Run pipeline:

```bash
make tg-live-stop || true
make tg-live-start INTERVAL=10 RESPONSE_ENABLED=1 DM_RESPONSE_MODE=conversational DM_PERSONA_NAME="Lobster Llama"
```

Then send a real DM to the account and validate:

```bash
make tg-live-status
tail -n 80 data/logs/dm-listener.log
tail -n 80 data/logs/dm-ingest.log
tail -n 80 data/logs/dm-respond.log
```

SQL checks:

```bash
psql "$DB_URL" -v ON_ERROR_STOP=1 -c "
SELECT id, conversation_id, direction, response_status, response_last_error, response_message_external_id, sent_at, text
FROM dm_messages
ORDER BY id DESC
LIMIT 25;
"
```

Stop:

```bash
make tg-live-stop
```

---

## 6. Failure Triage (Fast)

1. `response_status column missing` in logs:
   run `make db-migrate`.
2. `Session file not found`:
   set `DM_SESSION_PATH`/`TG_SESSION_PATH` to authenticated session file.
3. `database is locked`:
   ensure only one live supervisor/listener is running, then `make tg-live-stop` and restart.
4. no new rows ingested:
   verify listener is running and JSONL file is growing.
5. responses not sent but rows are pending:
   run responder directly:
   `DM_RESPONSE_MODE=conversational DM_PERSONA_NAME="Lobster Llama" bash tools/telethon_collector/run-dm-response.sh`

---

## 7. Output Required from OpenClaw

At the end, OpenClaw must provide:

1. command transcript showing each section executed,
2. output of the three hard checks in Section 4,
3. final statement: `WORKING` or `NOT WORKING`,
4. if `NOT WORKING`, first failing command and exact error text.
