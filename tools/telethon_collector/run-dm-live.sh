#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PID_DIR="$ROOT_DIR/data/.pids"
LISTENER_PID_FILE="$PID_DIR/tg-listen-dm.pid"
SUPERVISOR_PID_FILE="$PID_DIR/tg-live-supervisor.pid"
LOG_DIR="$ROOT_DIR/data/logs"
LOCK_FILE="$PID_DIR/tg-live-supervisor.lock"
JSONL_FILE="${1:-data/exports/telethon_dms_live.jsonl}"
INTERVAL="${2:-30}"
MODE="${3:-profile}"  # profile|ingest
RESPONSE_ENABLED="${RESPONSE_ENABLED:-1}"
STATE_FILE="${4:-$ROOT_DIR/data/.state/dm-live.state.json}"
SESSION_PATH="${5:-$ROOT_DIR/tools/telethon_collector/telethon_openclaw.session}"
SNAPSHOT_STATE_FILE="${SNAPSHOT_STATE_FILE:-$ROOT_DIR/data/.state/dm-live-catchup.state.json}"

if [ -f "$ROOT_DIR/.env" ]; then
  set -a
  . "$ROOT_DIR/.env"
  set +a
fi
if [ -f "$ROOT_DIR/openclaw.env" ]; then
  set -a
  . "$ROOT_DIR/openclaw.env"
  set +a
fi
if [ -f "$ROOT_DIR/tools/telethon_collector/.env" ]; then
  set -a
  . "$ROOT_DIR/tools/telethon_collector/.env"
  set +a
fi

DB_CONN="${DATABASE_URL:-${PG_DSN:-}}"

if [[ "$JSONL_FILE" = /* ]]; then
  JSONL_PATH="$JSONL_FILE"
else
  JSONL_PATH="$ROOT_DIR/$JSONL_FILE"
fi

mkdir -p "$PID_DIR" "$LOG_DIR" "$(dirname "$STATE_FILE")"
mkdir -p "$(dirname "$SESSION_PATH")"

# Runtime guardrails
MAX_LISTENER_RESTARTS=8
AUTH_RETRY_COOLDOWN=120
LISTENER_FAIL_DELAY=2

LISTENER_FAIL_STREAK=0
RESPONSE_STATUS_AVAILABLE=""

log() {
  echo "[$(date -Is)] $*"
}

log_err() {
  log "ERROR: $*" >&2
}

is_running() {
  local pid_file=$1
  [ -f "$pid_file" ] || return 1
  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  [ -n "$pid" ] || return 1
  kill -0 "$pid" 2>/dev/null
}

listener_is_running() {
  if is_running "$LISTENER_PID_FILE"; then
    return 0
  fi

  local pids
  pids=$(pgrep -f "listen-dms.py --out ${JSONL_PATH}" || true)
  [ -n "$pids" ]
}

check_requirements() {
  if ! command -v make >/dev/null 2>&1; then
    log_err "make is required but not installed"
    return 1
  fi
  if [ ! -x "$ROOT_DIR/tools/telethon_collector/.venv/bin/python" ]; then
    log_err "Telethon virtualenv missing: $ROOT_DIR/tools/telethon_collector/.venv"
    return 1
  fi
  if [ -z "$DB_CONN" ]; then
    log_err "DATABASE_URL/PG_DSN not configured"
    return 1
  fi

  if [ ! -f "$SESSION_PATH" ]; then
    log_err "Session path not found: $SESSION_PATH"
    log_err "Set SESSION_PATH/TG_SESSION_PATH to an authenticated Telethon session file."
    return 1
  fi

  return 0
}

cleanup_stale_listener() {
  local out_path="$1"
  # kill old listeners targeting this exact output file path (stops sqlite lock/dupes)
  local pids
  pids=$(pgrep -f "listen-dms.py --out ${out_path}" || true)
  if [ -n "$pids" ]; then
    log "Cleaning up old listener processes for $out_path: $pids"
    while IFS= read -r pid; do
      if [ -n "$pid" ] && [ "$pid" != "$$" ]; then
        kill "$pid" 2>/dev/null || true
      fi
    done <<< "$pids"
    sleep 1
  fi
}

cleanup_listener_pidfile() {
  local path=$1
  if [ -f "$path" ]; then
    local pid
    pid="$(cat "$path" 2>/dev/null || true)"
    if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$path"
    fi
  fi
}


run_snapshot_cycle() {
  if [ ! -x "$ROOT_DIR/tools/telethon_collector/.venv/bin/python" ]; then
    return 0
  fi

  (
    cd "$ROOT_DIR/tools/telethon_collector"
    . .venv/bin/activate
    python3 snapshot-dms.py \
      --out "$JSONL_PATH" \
      --state-file "$SNAPSHOT_STATE_FILE" \
      --session-path "$SESSION_PATH" \
      --limit "${CATCHUP_LIMIT:-40}"
  ) >> "$LOG_DIR/dm-ingest.log" 2>&1

  local status=$?
  if [ "$status" -ne 0 ]; then
    log_err "snapshot-catchup failed with status=$status"
    return 1
  fi
  return 0
}

has_response_status_column() {
  if [ -z "$DB_CONN" ]; then
    return 1
  fi

  # Cache only the positive check. If DB is temporarily unreachable or the
  # schema is mid-migration, re-check on the next cycle.
  if [ "$RESPONSE_STATUS_AVAILABLE" = "1" ]; then
    return 0
  fi

  local out
  if ! out=$(
    "$ROOT_DIR/tools/telethon_collector/.venv/bin/python" - "$DB_CONN" <<'PY'
import os
import sys
from psycopg import connect

conn_url = sys.argv[1]
with connect(conn_url) as conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'dm_messages'
              AND column_name = 'response_status'
            LIMIT 1
            """
        )
        row = cur.fetchone()
        print('1' if row else '0')
PY
  ); then
    log_err "Response-status schema check failed; responder will be skipped for this cycle"
    return 1
  fi

  if [ "$out" = "1" ]; then
    RESPONSE_STATUS_AVAILABLE="1"
    return 0
  fi
  return 1
}

start_listener() {
  cleanup_listener_pidfile "$LISTENER_PID_FILE"
  cleanup_stale_listener "$JSONL_PATH"

  mkdir -p "$(dirname "$JSONL_PATH")"
  : >> "$JSONL_PATH"

  if [ ! -f "$SESSION_PATH" ]; then
    log_err "Cannot start listener: session not found ($SESSION_PATH)"
    return 1
  fi

  (
    cd "$ROOT_DIR/tools/telethon_collector"
    . .venv/bin/activate
    DM_AUTO_ACK="${DM_AUTO_ACK:-0}" \
    DM_AUTO_ACK_TEXT="${DM_AUTO_ACK_TEXT:-Got it — I captured this message and will process it now.}" \
    TG_SESSION_PATH="$SESSION_PATH" \
    .venv/bin/python3 -u listen-dms.py --out "$JSONL_PATH"
  ) >> "$LOG_DIR/dm-listener.log" 2>&1 &
  listener_pid=$!
  echo "$listener_pid" > "$LISTENER_PID_FILE"

  sleep 2
  if ! kill -0 "$listener_pid" 2>/dev/null; then
    local listener_status=0
    if wait "$listener_pid"; then
      listener_status=0
    else
      listener_status=$?
    fi
    rm -f "$LISTENER_PID_FILE"
    local latest
    latest=$(tail -n 1 "$LOG_DIR/dm-listener.log" | sed -n '1,120p' || true)
    if echo "$latest" | grep -qi "Please enter your phone\|interactive login"; then
      log_err "Listener hit interactive-auth wall; waiting ${AUTH_RETRY_COOLDOWN}s before recheck."
      LISTENER_FAIL_STREAK=$((LISTENER_FAIL_STREAK + 1))
      if [ "$LISTENER_FAIL_STREAK" -ge "$MAX_LISTENER_RESTARTS" ]; then
        sleep "$AUTH_RETRY_COOLDOWN"
      else
        sleep "$LISTENER_FAIL_DELAY"
      fi
      return 1
    fi

    log_err "Listener exited immediately with status ${listener_status}."
    LISTENER_FAIL_STREAK=$((LISTENER_FAIL_STREAK + 1))
    sleep "$LISTENER_FAIL_DELAY"
    return 1
  fi

  LISTENER_FAIL_STREAK=0
  log "Started listener (pid=$listener_pid)."
  return 0
}

stop_listener() {
  cleanup_listener_pidfile "$LISTENER_PID_FILE"
  if [ ! -f "$LISTENER_PID_FILE" ]; then
    cleanup_stale_listener "$JSONL_PATH"
    return
  fi

  local pid
  pid="$(cat "$LISTENER_PID_FILE" 2>/dev/null || true)"
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
  fi
  rm -f "$LISTENER_PID_FILE"

  cleanup_stale_listener "$JSONL_PATH"
}

run_ingest_cycle() {
  local mode=$1
  local ok=0
  local listener_was_running=0

  if listener_is_running; then
    listener_was_running=1
    stop_listener
    cleanup_stale_listener "$JSONL_PATH"
    sleep 1
  fi

  run_snapshot_cycle || true

  local ingest_cmd=(npm run ingest-dm-jsonl -- --file "$JSONL_FILE" --state-file "$STATE_FILE")

  if [ "$mode" = "profile" ]; then
    log "ingesting $JSONL_FILE + reconcile (state=$STATE_FILE)"
    (
      cd "$ROOT_DIR"
      "${ingest_cmd[@]}"
    ) >> "$LOG_DIR/dm-ingest.log" 2>&1
    local ingest_status=$?

    if [ "$ingest_status" -ne 0 ]; then
      log "ingest failed (status $ingest_status)"
      echo "[$(date -Is)] ingest failed (status $ingest_status)" >> "$LOG_DIR/dm-ingest.log"
      [ "$listener_was_running" -eq 1 ] && start_listener || true
      return 1
    fi

    (
      cd "$ROOT_DIR"
      npm run reconcile-dm-psych
    ) >> "$LOG_DIR/dm-ingest.log" 2>&1
    local reconcile_status=$?

    if [ "$reconcile_status" -ne 0 ]; then
      log "reconcile failed (status $reconcile_status)"
      echo "[$(date -Is)] reconcile failed (status $reconcile_status)" >> "$LOG_DIR/dm-ingest.log"
      [ "$listener_was_running" -eq 1 ] && start_listener || true
      return 1
    fi

    if [ "$RESPONSE_ENABLED" = "1" ] || [ "$RESPONSE_ENABLED" = "true" ]; then
      if has_response_status_column; then
        (
          cd "$ROOT_DIR"
          DM_SESSION_PATH="$SESSION_PATH" \
          DM_RESPONSE_LIMIT="${DM_RESPONSE_LIMIT:-20}" \
          DM_MAX_RETRIES="${DM_MAX_RETRIES:-3}" \
          DM_RESPONSE_MODE="${DM_RESPONSE_MODE:-conversational}" \
          DM_PERSONA_NAME="${DM_PERSONA_NAME:-Lobster Llama}" \
          DM_RESPONSE_TEMPLATE="${DM_RESPONSE_TEMPLATE:-Thanks for reaching out — I captured this and will use it to improve your profile dataset. To help it, share: your current role/company, 2-3 priorities, and how you prefer to communicate.}" \
          bash tools/telethon_collector/run-dm-response.sh
        ) >> "$LOG_DIR/dm-respond.log" 2>&1
        local respond_status=$?
        if [ "$respond_status" -ne 0 ]; then
          log "response cycle failed (status $respond_status)"
          echo "[$(date -Is)] response cycle failed (status $respond_status)" >> "$LOG_DIR/dm-respond.log"
        fi
      else
        log "response_status column missing; skipping responder until schema is migrated"
        echo "[$(date -Is)] skipping response cycle: dm_messages.response_status missing" >> "$LOG_DIR/dm-respond.log"
      fi
    fi
  else
    log "ingesting $JSONL_FILE + state=$STATE_FILE"
    (
      cd "$ROOT_DIR"
      "${ingest_cmd[@]}"
    ) >> "$LOG_DIR/dm-ingest.log" 2>&1
    local ingest_status=$?
    if [ "$ingest_status" -ne 0 ]; then
      log "ingest failed (status $ingest_status)"
      echo "[$(date -Is)] ingest failed (status $ingest_status)" >> "$LOG_DIR/dm-ingest.log"
      [ "$listener_was_running" -eq 1 ] && start_listener || true
      return 1
    fi
  fi

  if [ "$listener_was_running" -eq 1 ]; then
    start_listener || true
  fi

  return "$ok"
}


acquire_lock() {
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "Another tg-live supervisor is active for this workspace." >&2
    exit 1
  fi
}

cleanup() {
  log "Received stop signal, cleaning up..."
  stop_listener
  rm -f "$SUPERVISOR_PID_FILE"
  exec 9>&-
  exit 0
}

trap cleanup INT TERM

if ! check_requirements; then
  exit 1
fi

if [ "$INTERVAL" -le 0 ]; then
  log "INTERVAL must be > 0"
  exit 1
fi

acquire_lock

# Ensure no orphaned supervisor
if is_running "$SUPERVISOR_PID_FILE"; then
  echo "Supervisor already running (pid $(cat "$SUPERVISOR_PID_FILE").)"
  exit 0
fi

echo "$$" > "$SUPERVISOR_PID_FILE"
cleanup_stale_listener "$JSONL_PATH"
start_listener || true

backoff=0
while true; do
  if ! is_running "$LISTENER_PID_FILE"; then
    if [ "$LISTENER_FAIL_STREAK" -ge "$MAX_LISTENER_RESTARTS" ]; then
      log "Listener has failed too many times; entering cooldown"
      sleep "$AUTH_RETRY_COOLDOWN"
      LISTENER_FAIL_STREAK=0
    else
      log "listener not running; restarting"
      start_listener || true
    fi
  fi

  if run_ingest_cycle "$MODE"; then
    backoff=0
    log "cycle complete"
  else
    if [ "$backoff" -lt 10 ]; then
      backoff=10
    elif [ "$backoff" -lt 120 ]; then
      backoff=$((backoff + 10))
    fi
    log "cycle failed; retrying in ${backoff}s"
    sleep "$backoff"
  fi

  sleep "$INTERVAL"

done
