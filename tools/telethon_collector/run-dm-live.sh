#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PID_DIR="$ROOT_DIR/data/.pids"
LISTENER_PID_FILE="$PID_DIR/tg-listen-dm.pid"
INGEST_PID_FILE="$PID_DIR/tg-ingest-dm.pid"
LOG_DIR="$ROOT_DIR/data/logs"
JSONL_FILE="${1:-data/exports/telethon_dms_live.jsonl}"
INTERVAL="${2:-30}"

mkdir -p "$PID_DIR" "$LOG_DIR"

start_listener() {
  if [ -f "$LISTENER_PID_FILE" ] && kill -0 "$(cat "$LISTENER_PID_FILE")" 2>/dev/null; then
    echo "Listener already running (pid $(cat "$LISTENER_PID_FILE"))."
    return 0
  fi

  # ensure output file exists before listener writes
  mkdir -p "$(dirname "$ROOT_DIR/$JSONL_FILE")"
  : > "$ROOT_DIR/$JSONL_FILE"

  (
    cd "$ROOT_DIR"
    make tg-listen-dm
  ) >> "$LOG_DIR/dm-listener.log" 2>&1 &
  listener_pid=$!
  echo "$listener_pid" > "$LISTENER_PID_FILE"
  echo "Started listener (pid=$listener_pid)."
}

start_ingest() {
  if [ -f "$INGEST_PID_FILE" ] && kill -0 "$(cat "$INGEST_PID_FILE")" 2>/dev/null; then
    echo "Ingest loop already running (pid $(cat "$INGEST_PID_FILE"))."
    return 0
  fi

  (
    cd "$ROOT_DIR"
    make tg-listen-ingest-dm file="$JSONL_FILE" interval="$INTERVAL"
  ) >> "$LOG_DIR/dm-ingest.log" 2>&1 &
  ingest_pid=$!
  echo "$ingest_pid" > "$INGEST_PID_FILE"
  echo "Started ingest loop (pid=$ingest_pid)."
}

start_listener
start_ingest

echo "DM live pipeline started."