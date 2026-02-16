#!/usr/bin/env bash
set -euo pipefail

PY="tools/telethon_collector/.venv/bin/python"
SCRIPT="tools/telethon_collector/backfill_user_names.py"

if [[ ! -x "$PY" ]]; then
  echo "⚠️ Telethon venv missing at $PY. Skipping name backfill (run: make tg:venv)."
  exit 0
fi

export PYTHONUNBUFFERED=1
exec "$PY" "$SCRIPT" "$@"
