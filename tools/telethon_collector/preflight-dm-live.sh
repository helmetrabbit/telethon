#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SESSION_PATH_INPUT="${1:-${TG_SESSION_PATH:-tools/telethon_collector/telethon_openclaw.session}}"

if [[ "$SESSION_PATH_INPUT" = /* ]]; then
  SESSION_PATH="$SESSION_PATH_INPUT"
else
  SESSION_PATH="$ROOT_DIR/$SESSION_PATH_INPUT"
fi

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
if [ -f "$SCRIPT_DIR/.env" ]; then
  set -a
  . "$SCRIPT_DIR/.env"
  set +a
fi

DB_CONN="${DATABASE_URL:-${PG_DSN:-}}"
VENV_PY="$SCRIPT_DIR/.venv/bin/python"

echo "[preflight] root=$ROOT_DIR"
echo "[preflight] session=$SESSION_PATH"

if [ ! -x "$VENV_PY" ]; then
  echo "[preflight] ERROR: Missing Telethon virtualenv at $SCRIPT_DIR/.venv"
  echo "[preflight] Fix:"
  echo "  python3 -m venv tools/telethon_collector/.venv"
  echo "  . tools/telethon_collector/.venv/bin/activate"
  echo "  pip install -r tools/telethon_collector/requirements.txt"
  exit 1
fi

if [ ! -f "$SESSION_PATH" ]; then
  echo "[preflight] ERROR: Telethon session file not found: $SESSION_PATH"
  echo "[preflight] Set SESSION_PATH or TG_SESSION_PATH to an authenticated .session file"
  exit 1
fi

if [ -z "$DB_CONN" ]; then
  echo "[preflight] ERROR: DATABASE_URL/PG_DSN not configured"
  echo "[preflight] Add DATABASE_URL in .env"
  exit 1
fi

if ! "$VENV_PY" -c "import telethon, psycopg" >/dev/null 2>&1; then
  echo "[preflight] ERROR: Python deps missing in venv (telethon/psycopg)"
  echo "[preflight] Fix: . tools/telethon_collector/.venv/bin/activate && pip install -r tools/telethon_collector/requirements.txt"
  exit 1
fi

if [ -z "${OPENROUTER_API_KEY:-}" ]; then
  echo "[preflight] WARN: OPENROUTER_API_KEY not set; responder will run deterministic fallback only"
fi

echo "[preflight] OK"
