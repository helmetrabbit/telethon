# DM Contact-Style Threshold Tuning Runbook

This runbook is for tuning:
- `DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD`
- `DM_CONTACT_STYLE_CONFIRM_THRESHOLD`

It is aligned with current pipeline behavior in `tools/telethon_collector/respond-dm-pending.py` and `src/cli/reconcile-dm-psychometry.ts`.

## 0) Prereqs

```bash
# Run from repo root
cd /home/node/.openclaw/workspace/telethon

# Must be set
export DATABASE_URL="postgres://..."

# Optional: override current runtime defaults
export DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD="${DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD:-0.80}"
export DM_CONTACT_STYLE_CONFIRM_THRESHOLD="${DM_CONTACT_STYLE_CONFIRM_THRESHOLD:-0.55}"
```

## 1) Current Log Queries (exact current format)

Current responder summaries are line-based in `data/logs/dm-respond.log`.

```bash
LOG=data/logs/dm-respond.log

# Latest responder outcomes
rg -n "dm responder: responded=|No pending DM responses to send|failed to respond to inbound dm id=|response cycle failed|Response worker failed" "$LOG" | tail -n 200

# Fast counters (whole file)
printf "cycles=%s\n" "$(rg -c 'dm responder: responded=' "$LOG" 2>/dev/null || echo 0)"
printf "no_pending=%s\n" "$(rg -c 'No pending DM responses to send' "$LOG" 2>/dev/null || echo 0)"
printf "message_failures=%s\n" "$(rg -c 'failed to respond to inbound dm id=' "$LOG" 2>/dev/null || echo 0)"
printf "cycle_failures=%s\n" "$(rg -c 'response cycle failed' "$LOG" 2>/dev/null || echo 0)"
```

## 2) Last 7 Days Metrics (copy/paste)

This is the primary tuning query block.

```bash
AUTO_APPLY_THRESHOLD="${DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD:-0.80}"

psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -v auto_apply="$AUTO_APPLY_THRESHOLD" <<'SQL'
WITH style_events_raw AS (
  SELECT
    e.id AS event_id,
    e.user_id,
    e.source_message_id,
    e.created_at,
    lower(btrim(f->>'new_value')) AS style_value,
    COALESCE(NULLIF(f->>'confidence', '')::double precision, e.confidence::double precision, 0.0) AS confidence
  FROM dm_profile_update_events e
  JOIN LATERAL jsonb_array_elements(e.extracted_facts) f ON true
  WHERE e.created_at >= now() - interval '7 days'
    AND f->>'field' = 'preferred_contact_style'
    AND COALESCE(btrim(f->>'new_value'), '') <> ''
),
style_events AS (
  SELECT DISTINCT ON (event_id)
    event_id, user_id, source_message_id, created_at, style_value, confidence
  FROM style_events_raw
  ORDER BY event_id, confidence DESC
),
auto_applied AS (
  SELECT *
  FROM style_events
  WHERE confidence >= :auto_apply
    AND source_message_id IS NOT NULL
),
next_three_inbound AS (
  SELECT
    a.event_id,
    a.style_value,
    m2.id AS inbound_message_id,
    row_number() OVER (PARTITION BY a.event_id ORDER BY m2.sent_at, m2.id) AS rn
  FROM auto_applied a
  JOIN dm_messages src ON src.id = a.source_message_id
  JOIN dm_messages m2
    ON m2.sender_id = a.user_id
   AND m2.direction = 'inbound'
   AND (m2.sent_at, m2.id) > (src.sent_at, src.id)
),
auto_apply_regrets AS (
  SELECT DISTINCT n.event_id
  FROM next_three_inbound n
  JOIN dm_profile_update_events e2 ON e2.source_message_id = n.inbound_message_id
  JOIN LATERAL jsonb_array_elements(e2.extracted_facts) f2 ON true
  WHERE n.rn <= 3
    AND f2->>'field' = 'preferred_contact_style'
    AND COALESCE(btrim(f2->>'new_value'), '') <> ''
    AND lower(btrim(f2->>'new_value')) <> n.style_value
),
confirmation_messages AS (
  SELECT
    count(*) FILTER (
      WHERE direction = 'outbound'
        AND sent_at >= now() - interval '7 days'
        AND text ILIKE '%Want me to switch to that style? Reply yes or no.%'
    ) AS confirmation_prompted,
    count(*) FILTER (
      WHERE direction = 'outbound'
        AND sent_at >= now() - interval '7 days'
        AND text ILIKE 'Perfect %switched.%'
    ) AS confirmed_yes,
    count(*) FILTER (
      WHERE direction = 'outbound'
        AND sent_at >= now() - interval '7 days'
        AND (
          text ILIKE 'Got it %keeping your current style:%'
          OR text ILIKE 'Got it %change style yet.%'
        )
    ) AS confirmed_no
  FROM dm_messages
),
counts AS (
  SELECT
    (SELECT count(*) FROM style_events) AS style_detected,
    (SELECT count(*) FROM auto_applied) AS auto_applied_count,
    (SELECT count(*) FROM auto_apply_regrets) AS auto_apply_regrets,
    (SELECT confirmation_prompted FROM confirmation_messages) AS confirmation_prompted,
    (SELECT confirmed_yes FROM confirmation_messages) AS confirmed_yes,
    (SELECT confirmed_no FROM confirmation_messages) AS confirmed_no
)
SELECT
  style_detected,
  confirmation_prompted,
  confirmed_yes,
  confirmed_no,
  auto_applied_count,
  auto_apply_regrets,
  round((confirmation_prompted::numeric / NULLIF(style_detected, 0)) * 100, 2) AS prompt_rate_pct,
  round((confirmed_yes::numeric / NULLIF(confirmed_yes + confirmed_no, 0)) * 100, 2) AS confirm_yes_rate_pct,
  round((auto_apply_regrets::numeric / NULLIF(auto_applied_count, 0)) * 100, 2) AS auto_apply_regret_rate_pct
FROM counts;
SQL
```

Targets:
- `prompt_rate_pct`: `25` to `45`
- `confirm_yes_rate_pct`: `>= 65`
- `auto_apply_regret_rate_pct`: `<= 3`

## 3) Rolling 200 Style Events Metrics (copy/paste)

Use this when traffic is bursty and 7-day windows lag reality.

```bash
AUTO_APPLY_THRESHOLD="${DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD:-0.80}"

psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -v auto_apply="$AUTO_APPLY_THRESHOLD" <<'SQL'
WITH style_events_raw AS (
  SELECT
    e.id AS event_id,
    e.user_id,
    e.source_message_id,
    e.created_at,
    lower(btrim(f->>'new_value')) AS style_value,
    COALESCE(NULLIF(f->>'confidence', '')::double precision, e.confidence::double precision, 0.0) AS confidence
  FROM dm_profile_update_events e
  JOIN LATERAL jsonb_array_elements(e.extracted_facts) f ON true
  WHERE f->>'field' = 'preferred_contact_style'
    AND COALESCE(btrim(f->>'new_value'), '') <> ''
),
style_events AS (
  SELECT DISTINCT ON (event_id)
    event_id, user_id, source_message_id, created_at, style_value, confidence
  FROM style_events_raw
  ORDER BY event_id, confidence DESC
),
scoped_style_events AS (
  SELECT *
  FROM style_events
  ORDER BY created_at DESC, event_id DESC
  LIMIT 200
),
bounds AS (
  SELECT min(created_at) AS since_ts, max(created_at) AS until_ts
  FROM scoped_style_events
),
auto_applied AS (
  SELECT *
  FROM scoped_style_events
  WHERE confidence >= :auto_apply
    AND source_message_id IS NOT NULL
),
next_three_inbound AS (
  SELECT
    a.event_id,
    a.style_value,
    m2.id AS inbound_message_id,
    row_number() OVER (PARTITION BY a.event_id ORDER BY m2.sent_at, m2.id) AS rn
  FROM auto_applied a
  JOIN dm_messages src ON src.id = a.source_message_id
  JOIN dm_messages m2
    ON m2.sender_id = a.user_id
   AND m2.direction = 'inbound'
   AND (m2.sent_at, m2.id) > (src.sent_at, src.id)
),
auto_apply_regrets AS (
  SELECT DISTINCT n.event_id
  FROM next_three_inbound n
  JOIN dm_profile_update_events e2 ON e2.source_message_id = n.inbound_message_id
  JOIN LATERAL jsonb_array_elements(e2.extracted_facts) f2 ON true
  WHERE n.rn <= 3
    AND f2->>'field' = 'preferred_contact_style'
    AND COALESCE(btrim(f2->>'new_value'), '') <> ''
    AND lower(btrim(f2->>'new_value')) <> n.style_value
),
confirmation_messages AS (
  SELECT
    count(*) FILTER (
      WHERE m.direction = 'outbound'
        AND m.text ILIKE '%Want me to switch to that style? Reply yes or no.%'
    ) AS confirmation_prompted,
    count(*) FILTER (
      WHERE m.direction = 'outbound'
        AND m.text ILIKE 'Perfect %switched.%'
    ) AS confirmed_yes,
    count(*) FILTER (
      WHERE m.direction = 'outbound'
        AND (
          m.text ILIKE 'Got it %keeping your current style:%'
          OR m.text ILIKE 'Got it %change style yet.%'
        )
    ) AS confirmed_no
  FROM dm_messages m
  JOIN bounds b ON b.since_ts IS NOT NULL
  WHERE m.sent_at >= b.since_ts
    AND m.sent_at <= b.until_ts + interval '30 minutes'
),
counts AS (
  SELECT
    (SELECT count(*) FROM scoped_style_events) AS style_detected,
    (SELECT count(*) FROM auto_applied) AS auto_applied_count,
    (SELECT count(*) FROM auto_apply_regrets) AS auto_apply_regrets,
    COALESCE((SELECT confirmation_prompted FROM confirmation_messages), 0) AS confirmation_prompted,
    COALESCE((SELECT confirmed_yes FROM confirmation_messages), 0) AS confirmed_yes,
    COALESCE((SELECT confirmed_no FROM confirmation_messages), 0) AS confirmed_no
)
SELECT
  style_detected,
  confirmation_prompted,
  confirmed_yes,
  confirmed_no,
  auto_applied_count,
  auto_apply_regrets,
  round((confirmation_prompted::numeric / NULLIF(style_detected, 0)) * 100, 2) AS prompt_rate_pct,
  round((confirmed_yes::numeric / NULLIF(confirmed_yes + confirmed_no, 0)) * 100, 2) AS confirm_yes_rate_pct,
  round((auto_apply_regrets::numeric / NULLIF(auto_applied_count, 0)) * 100, 2) AS auto_apply_regret_rate_pct
FROM counts;
SQL
```

## 4) Tiny Decision Output (keep/raise/lower + delta)

This prints a direct recommendation from last-7-day metrics.

```bash
AUTO="${DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD:-0.80}"
CONF="${DM_CONTACT_STYLE_CONFIRM_THRESHOLD:-0.55}"

read -r PROMPT_RATE CONFIRM_YES_RATE AUTO_REGRET <<<"$(
  psql "$DATABASE_URL" -X -A -t -F $'\t' -v ON_ERROR_STOP=1 -v auto_apply="$AUTO" <<'SQL'
WITH style_events_raw AS (
  SELECT
    e.id AS event_id,
    e.user_id,
    e.source_message_id,
    lower(btrim(f->>'new_value')) AS style_value,
    COALESCE(NULLIF(f->>'confidence', '')::double precision, e.confidence::double precision, 0.0) AS confidence,
    e.created_at
  FROM dm_profile_update_events e
  JOIN LATERAL jsonb_array_elements(e.extracted_facts) f ON true
  WHERE e.created_at >= now() - interval '7 days'
    AND f->>'field' = 'preferred_contact_style'
    AND COALESCE(btrim(f->>'new_value'), '') <> ''
),
style_events AS (
  SELECT DISTINCT ON (event_id)
    event_id, user_id, source_message_id, style_value, confidence
  FROM style_events_raw
  ORDER BY event_id, confidence DESC
),
auto_applied AS (
  SELECT *
  FROM style_events
  WHERE confidence >= :auto_apply
    AND source_message_id IS NOT NULL
),
next_three_inbound AS (
  SELECT
    a.event_id,
    a.style_value,
    m2.id AS inbound_message_id,
    row_number() OVER (PARTITION BY a.event_id ORDER BY m2.sent_at, m2.id) AS rn
  FROM auto_applied a
  JOIN dm_messages src ON src.id = a.source_message_id
  JOIN dm_messages m2
    ON m2.sender_id = a.user_id
   AND m2.direction = 'inbound'
   AND (m2.sent_at, m2.id) > (src.sent_at, src.id)
),
auto_apply_regrets AS (
  SELECT DISTINCT n.event_id
  FROM next_three_inbound n
  JOIN dm_profile_update_events e2 ON e2.source_message_id = n.inbound_message_id
  JOIN LATERAL jsonb_array_elements(e2.extracted_facts) f2 ON true
  WHERE n.rn <= 3
    AND f2->>'field' = 'preferred_contact_style'
    AND COALESCE(btrim(f2->>'new_value'), '') <> ''
    AND lower(btrim(f2->>'new_value')) <> n.style_value
),
confirmation_messages AS (
  SELECT
    count(*) FILTER (
      WHERE direction = 'outbound'
        AND sent_at >= now() - interval '7 days'
        AND text ILIKE '%Want me to switch to that style? Reply yes or no.%'
    ) AS confirmation_prompted,
    count(*) FILTER (
      WHERE direction = 'outbound'
        AND sent_at >= now() - interval '7 days'
        AND text ILIKE 'Perfect %switched.%'
    ) AS confirmed_yes,
    count(*) FILTER (
      WHERE direction = 'outbound'
        AND sent_at >= now() - interval '7 days'
        AND (
          text ILIKE 'Got it %keeping your current style:%'
          OR text ILIKE 'Got it %change style yet.%'
        )
    ) AS confirmed_no
  FROM dm_messages
),
counts AS (
  SELECT
    (SELECT count(*) FROM style_events) AS style_detected,
    (SELECT count(*) FROM auto_applied) AS auto_applied_count,
    (SELECT count(*) FROM auto_apply_regrets) AS auto_apply_regrets,
    (SELECT confirmation_prompted FROM confirmation_messages) AS confirmation_prompted,
    (SELECT confirmed_yes FROM confirmation_messages) AS confirmed_yes,
    (SELECT confirmed_no FROM confirmation_messages) AS confirmed_no
)
SELECT
  COALESCE(confirmation_prompted::double precision / NULLIF(style_detected, 0), 0.0) AS prompt_rate,
  COALESCE(confirmed_yes::double precision / NULLIF(confirmed_yes + confirmed_no, 0), 0.0) AS confirm_yes_rate,
  COALESCE(auto_apply_regrets::double precision / NULLIF(auto_applied_count, 0), 0.0) AS auto_apply_regret_rate
FROM counts;
SQL
)"

awk -v pr="$PROMPT_RATE" -v cy="$CONFIRM_YES_RATE" -v ar="$AUTO_REGRET" -v auto="$AUTO" -v conf="$CONF" '
function clamp(v) { if (v < 0) return 0; if (v > 1) return 1; return v }
BEGIN {
  action="keep"; delta=0.00; reason="within target band";
  if (ar > 0.03) {
    action="raise"; delta=0.03; reason="auto_apply_regret_rate > 3%";
  } else if (pr > 0.45 && cy >= 0.75 && ar <= 0.03) {
    action="lower"; delta=0.03; reason="prompt_rate high + confirm_yes_rate high";
  } else if (cy < 0.50) {
    action="keep"; delta=0.00; reason="confirm_yes_rate < 50%; improve extraction before threshold edits";
  }

  new_auto=auto;
  if (action=="raise") new_auto=clamp(auto + delta);
  if (action=="lower") new_auto=clamp(auto - delta);

  sign = (delta >= 0 ? "+" : "");
  printf "AUTO_APPLY: %s by %s%.2f (%.2f -> %.2f) | reason=%s\n", action, sign, delta, auto, new_auto, reason;
}
'
```

## 5) Threshold Change Log Template

Paste this after every threshold edit.

```md
### Threshold Edit
- Time (UTC): YYYY-MM-DDTHH:MM:SSZ
- Operator: <name>
- Scope window used: last-7-days | rolling-200
- Old values:
  - DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD: <old>
  - DM_CONTACT_STYLE_CONFIRM_THRESHOLD: <old>
- New values:
  - DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD: <new>
  - DM_CONTACT_STYLE_CONFIRM_THRESHOLD: <new>
- Decision output:
  - AUTO_APPLY: keep|raise|lower by <delta>
- Reason:
  - <metric values + why this change>
- Expected effect (next 24h):
  - <e.g., lower prompt rate by ~8 points>
- Validation plan:
  - rerun last-7-days query at +24h
  - rerun rolling-200 query after next major traffic block
```

## 6) Guardrails

- Change one threshold at a time.
- Hold each change for at least 24h before the next change.
- If `confirm_yes_rate_pct < 50`, prioritize extractor quality before threshold loosening.
- Current implementation note: `DM_CONTACT_STYLE_CONFIRM_THRESHOLD` controls medium/low confidence wording band; confirmation gating itself is controlled by whether confidence is below `DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD`.
