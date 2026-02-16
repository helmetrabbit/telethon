#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PID_DIR="$ROOT_DIR/data/.pids"
LISTENER_PID_FILE="$PID_DIR/tg-listen-dm.pid"
SUPERVISOR_PID_FILE="$PID_DIR/tg-live-supervisor.pid"
LOG_DIR="$ROOT_DIR/data/logs"
JSONL_FILE="${1:-data/exports/telethon_dms_live.jsonl}"
INTERVAL="${2:-30}"
MODE="${3:-profile}"  # profile|ingest
STATE_FILE="${4:-$ROOT_DIR/data/.state/dm-live.state.json}"

if [[ "$JSONL_FILE" = /* ]]; then
  JSONL_PATH="$JSONL_FILE"
else
  JSONL_PATH="$ROOT_DIR/$JSONL_FILE"
fi

mkdir -p "$PID_DIR" "$LOG_DIR" "$(dirname "$STATE_FILE")"

# Normalize arguments
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

start_listener() {
  if is_running "$LISTENER_PID_FILE"; then
    return 0
  fi

  # ensure output file exists before listener writes
  mkdir -p "$(dirname "$JSONL_PATH")"
  : >> "$JSONL_PATH"

  (
    cd "$ROOT_DIR"
    make tg-listen-dm out="$JSONL_PATH"
  ) >> "$LOG_DIR/dm-listener.log" 2>&1 &
  listener_pid=$!
  echo "$listener_pid" > "$LISTENER_PID_FILE"
  echo "Started listener (pid=$listener_pid)."
}

stop_listener() {
  if [ -f "$LISTENER_PID_FILE" ]; then
    local pid
    pid="$(cat "$LISTENER_PID_FILE" 2>/dev/null || true)"
    if [ -n "$pid" ]; then
      kill "$pid" 2>/dev/null || true
    fi
    rm -f "$LISTENER_PID_FILE"
  fi
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
  else
    echo "[$(date -Is)] ingesting $JSONL_FILE (state=$STATE_FILE)"
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

cleanup() {
  echo "Received stop signal, cleaning up..."
  stop_listener
  rm -f "$SUPERVISOR_PID_FILE"
  exit 0
}

trap cleanup INT TERM

# Ensure no orphaned supervisor
if is_running "$SUPERVISOR_PID_FILE"; then
  echo "Supervisor already running (pid $(cat "$SUPERVISOR_PID_FILE"))."
  exit 0
fi

echo "$$" > "$SUPERVISOR_PID_FILE"

start_listener

backoff=0
while true; do
  if ! is_running "$LISTENER_PID_FILE"; then
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
