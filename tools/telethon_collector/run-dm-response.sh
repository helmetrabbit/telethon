#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SESSION_PATH="${DM_SESSION_PATH:-${TG_SESSION_PATH:-$ROOT_DIR/tools/telethon_collector/telethon.session}}"
LIMIT="${DM_RESPONSE_LIMIT:-20}"
MAX_RETRIES="${DM_MAX_RETRIES:-3}"
TEMPLATE="${DM_RESPONSE_TEMPLATE:-"Got your message: \"{excerpt}\". Thanks for reaching out — I\'ll review and reply with full context shortly."}"
DRY_RUN="${DM_RESPONSE_DRY_RUN:-0}"

(
  cd "$ROOT_DIR/tools/telethon_collector"
  . .venv/bin/activate
  if [ "$DRY_RUN" = "1" ] || [ "$DRY_RUN" = "true" ]; then
    python3 respond-dm-pending.py \
      --session-path "$SESSION_PATH" \
      --limit "$LIMIT" \
      --max-retries "$MAX_RETRIES" \
      --template "$TEMPLATE" \
      --dry-run
  else
    python3 respond-dm-pending.py \
      --session-path "$SESSION_PATH" \
      --limit "$LIMIT" \
      --max-retries "$MAX_RETRIES" \
      --template "$TEMPLATE"
  fi
) 
