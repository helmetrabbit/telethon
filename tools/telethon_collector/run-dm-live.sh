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
SESSION_PATH="${5:-$ROOT_DIR/data/.state/telethon-dm-live.session}"

if [[ "$JSONL_FILE" = /* ]]; then
  JSONL_PATH="$JSONL_FILE"
else
  JSONL_PATH="$ROOT_DIR/$JSONL_FILE"
fi

mkdir -p "$PID_DIR" "$LOG_DIR" "$(dirname "$STATE_FILE")"
mkdir -p "$(dirname "$SESSION_PATH")"

# Normalize and validate args
if [ "$INTERVAL" -le 0 ]; then
  echo "INTERVAL must be > 0" >&2
  exit 1
fi

is_running() {
  local pid_file=$1
  [ -f "$pid_file" ] || return 1
  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  [ -n "$pid" ] || return 1
  kill -0 "$pid" 2>/dev/null
}

cleanup_stale_listener() {
  local out_path="$1"
  local pids
  # kill any old listeners targeting this exact output file path (prevents sqlite session lock races)
  pids=$(pgrep -f "python3 listen-dms.py --out ${out_path}" || true)
  if [ -n "$pids" ]; then
    echo "Cleaning up old listener processes: $pids"
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

start_listener() {
  cleanup_listener_pidfile "$LISTENER_PID_FILE"
  cleanup_stale_listener "$JSONL_PATH"

  # ensure output file exists before listener writes
  mkdir -p "$(dirname "$JSONL_PATH")"
  : >> "$JSONL_PATH"

  (
    cd "$ROOT_DIR"
    DM_AUTO_ACK="1" \
    DM_AUTO_ACK_TEXT="Got it — I captured this message and will process it now." \
    TG_SESSION_PATH="$SESSION_PATH" \
    make tg-listen-dm out="$JSONL_PATH" session_path="$SESSION_PATH"
  ) >> "$LOG_DIR/dm-listener.log" 2>&1 &
  listener_pid=$!
  echo "$listener_pid" > "$LISTENER_PID_FILE"
  echo "Started listener (pid=$listener_pid)."
}

stop_listener() {
  cleanup_listener_pidfile "$LISTENER_PID_FILE"
  if [ ! -f "$LISTENER_PID_FILE" ]; then
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

  local ingest_cmd=(npm run ingest-dm-jsonl -- --file "$JSONL_FILE" --state-file "$STATE_FILE")

  if [ "$mode" = "profile" ]; then
    echo "[$(date -Is)] ingesting $JSONL_FILE + reconcile (state=$STATE_FILE)"
    (
      cd "$ROOT_DIR"
      "${ingest_cmd[@]}"
    ) >> "$LOG_DIR/dm-ingest.log" 2>&1
    local ingest_status=$?

    if [ "$ingest_status" -ne 0 ]; then
      echo "[$(date -Is)] ingest failed (status $ingest_status)" >> "$LOG_DIR/dm-ingest.log"
      return 1
    fi

    (
      cd "$ROOT_DIR"
      npm run reconcile-dm-psych
    ) >> "$LOG_DIR/dm-ingest.log" 2>&1
    local reconcile_status=$?

    if [ "$reconcile_status" -ne 0 ]; then
      echo "[$(date -Is)] reconcile failed (status $reconcile_status)" >> "$LOG_DIR/dm-ingest.log"
      return 1
    fi

    if [ "$RESPONSE_ENABLED" = "1" ] || [ "$RESPONSE_ENABLED" = "true" ]; then
      (
        cd "$ROOT_DIR"
        DM_SESSION_PATH="$SESSION_PATH" \
        DM_RESPONSE_LIMIT="${DM_RESPONSE_LIMIT:-20}" \
        DM_MAX_RETRIES="${DM_MAX_RETRIES:-3}" \
        DM_RESPONSE_TEMPLATE="${DM_RESPONSE_TEMPLATE:-Got it — I captured this message and will reply shortly.}" \
        bash tools/telethon_collector/run-dm-response.sh
      ) >> "$LOG_DIR/dm-respond.log" 2>&1
      local respond_status=$?
      if [ "$respond_status" -ne 0 ]; then
        echo "[$(date -Is)] response cycle failed (status $respond_status)" >> "$LOG_DIR/dm-respond.log"
      fi
    fi
  else
    echo "[$(date -Is)] ingesting $JSONL_FILE + state=$STATE_FILE"
    (
      cd "$ROOT_DIR"
      "${ingest_cmd[@]}"
    ) >> "$LOG_DIR/dm-ingest.log" 2>&1
    local ingest_status=$?
    if [ "$ingest_status" -ne 0 ]; then
      echo "[$(date -Is)] ingest failed (status $ingest_status)" >> "$LOG_DIR/dm-ingest.log"
      return 1
    fi
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
  echo "Received stop signal, cleaning up..."
  stop_listener
  rm -f "$SUPERVISOR_PID_FILE"
  exec 9>&-
  exit 0
}

trap cleanup INT TERM

acquire_lock

# Ensure no orphaned supervisor
if is_running "$SUPERVISOR_PID_FILE"; then
  echo "Supervisor already running (pid $(cat "$SUPERVISOR_PID_FILE").)"
  exit 0
fi

echo "$$" > "$SUPERVISOR_PID_FILE"

cleanup_stale_listener "$JSONL_PATH"
start_listener

backoff=0
while true; do
  if ! is_running "$LISTENER_PID_FILE"; then
    echo "[$(date -Is)] listener not running; restarting"
    start_listener
  fi

  if run_ingest_cycle "$MODE"; then
    backoff=0
    echo "[$(date -Is)] cycle complete"
  else
    backoff=$((backoff + 5))
    if [ "$backoff" -lt 10 ]; then
      backoff=10
    elif [ "$backoff" -gt 120 ]; then
      backoff=120
    fi
    echo "[$(date -Is)] cycle failed; retrying in ${backoff}s"
    sleep "$backoff"
  fi

  sleep "$INTERVAL"
done
