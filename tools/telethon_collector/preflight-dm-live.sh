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

# Detect "ops drift": multiple checkouts/processes running DM workers will race to respond
# with different code and break UX in unpredictable ways.
#
# We fail closed by default when we detect foreign workers. Override with:
#   DM_PREFLIGHT_ALLOW_FOREIGN_WORKERS=1 make tg-live-start
if [ -d /proc ] && command -v pgrep >/dev/null 2>&1; then
  PIDS="$(
    (
      pgrep -f "respond-dm-pending.py" || true
      pgrep -f "run-dm-response.sh" || true
      pgrep -f "run-dm-live.sh" || true
      pgrep -f "listen-dms.py --out" || true
    ) | sort -u
  )"

  FOREIGN=""
  for PID in $PIDS; do
    if [ "$PID" = "$$" ]; then
      continue
    fi
    CMD="$(ps -p "$PID" -o args= 2>/dev/null || true)"
    CWD="$(readlink -f "/proc/$PID/cwd" 2>/dev/null || true)"

    # Treat as "ours" if either cmd or cwd contains this checkout root path.
    if echo "$CMD" | grep -Fq "$ROOT_DIR" || echo "$CWD" | grep -Fq "$ROOT_DIR"; then
      continue
    fi

    if [ -n "$CMD" ]; then
      FOREIGN="${FOREIGN}\n- pid=${PID} cwd=${CWD:-unknown} cmd=${CMD}"
    else
      FOREIGN="${FOREIGN}\n- pid=${PID} cwd=${CWD:-unknown} (cmd unavailable)"
    fi
  done

  if [ -n "$FOREIGN" ]; then
    echo "[preflight] ERROR: Found DM worker process(es) not running from this checkout."
    echo "[preflight] This causes inconsistent replies (multiple versions racing). Stop them first:"
    printf "%b\n" "$FOREIGN"
    echo "[preflight] Suggested diagnosis:"
    echo "  ps -eo pid,ppid,lstart,cmd | rg -i 'respond-dm-pending.py|run-dm-response.sh|run-dm-live.sh|listen-dms.py' | rg -v 'rg -i'"
    echo "  for pid in <pid...>; do echo \"PID=$pid CWD=\$(readlink -f /proc/$pid/cwd)\"; done"
    if [ "${DM_PREFLIGHT_ALLOW_FOREIGN_WORKERS:-0}" = "1" ]; then
      echo "[preflight] WARN: DM_PREFLIGHT_ALLOW_FOREIGN_WORKERS=1 set; continuing anyway."
    else
      echo "[preflight] Set DM_PREFLIGHT_ALLOW_FOREIGN_WORKERS=1 to bypass (not recommended)."
      exit 1
    fi
  fi
fi

echo "[preflight] OK"
