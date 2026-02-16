#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOCK_FILE="$ROOT_DIR/data/.pids/dm-response.lock"
mkdir -p "$ROOT_DIR/data/.pids"
SESSION_PATH="${DM_SESSION_PATH:-${TG_SESSION_PATH:-$ROOT_DIR/tools/telethon_collector/telethon.session}}"
if [[ "${SESSION_PATH}" != /* ]] && [ "${SESSION_PATH}" != "" ]; then
  SESSION_PATH="$ROOT_DIR/$SESSION_PATH"
fi
LIMIT="${DM_RESPONSE_LIMIT:-20}"
MAX_RETRIES="${DM_MAX_RETRIES:-3}"
MODE="${DM_RESPONSE_MODE:-conversational}"
PERSONA_NAME="${DM_PERSONA_NAME:-Lobster Llama}"
TEMPLATE="${DM_RESPONSE_TEMPLATE:-"Thanks for reaching out — I captured this and will use it to improve your profile dataset. To help it, share: your current role/company, 2-3 priorities, and how you prefer to communicate."}"
DRY_RUN="${DM_RESPONSE_DRY_RUN:-0}"

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

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "Responder already running; skipping this run."
  exit 0
fi
if [ ! -f "$SESSION_PATH" ]; then
  echo "Session file not found: $SESSION_PATH" >&2
  echo "Set DM_SESSION_PATH/TG_SESSION_PATH to an existing authenticated Telethon session and retry." >&2
  exit 1
fi

(
  cd "$ROOT_DIR/tools/telethon_collector"
  . .venv/bin/activate
  if [ "$DRY_RUN" = "1" ] || [ "$DRY_RUN" = "true" ]; then
    TG_ALLOW_INTERACTIVE=0 python3 respond-dm-pending.py \
      --session-path "$SESSION_PATH" \
      --limit "$LIMIT" \
      --max-retries "$MAX_RETRIES" \
      --mode "$MODE" \
      --persona-name "$PERSONA_NAME" \
      --template "$TEMPLATE" \
      --dry-run
  else
    TG_ALLOW_INTERACTIVE=0 python3 respond-dm-pending.py \
      --session-path "$SESSION_PATH" \
      --limit "$LIMIT" \
      --max-retries "$MAX_RETRIES" \
      --mode "$MODE" \
      --persona-name "$PERSONA_NAME" \
      --template "$TEMPLATE"
  fi
)
