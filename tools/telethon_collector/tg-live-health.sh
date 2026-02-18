#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [ -n "${OPENCLAW_CANONICAL_ROOT:-}" ] && [ "$ROOT_DIR" != "$OPENCLAW_CANONICAL_ROOT" ]; then
  echo "[health] FAIL non-canonical checkout"
  echo "[health] OPENCLAW_CANONICAL_ROOT=$OPENCLAW_CANONICAL_ROOT"
  echo "[health] root=$ROOT_DIR"
  exit 1
fi

SHA="unknown"
if command -v git >/dev/null 2>&1; then
  SHA="$(cd "$ROOT_DIR" && git rev-parse --short HEAD 2>/dev/null || echo unknown)"
fi

echo "[health] root=$ROOT_DIR build=$SHA"

if ! command -v pgrep >/dev/null 2>&1; then
  echo "[health] WARN pgrep not found; cannot do process-level assertions"
  exit 0
fi

fail=0

check_family() {
  local label="$1"
  local pattern="$2"
  local allow_zero="$3"

  local pids
  pids="$(pgrep -f "$pattern" | sort -u || true)"
  local count
  count="$(echo "$pids" | sed '/^$/d' | wc -l | tr -d ' ')"

  if [ "$count" -eq 0 ] && [ "$allow_zero" = "1" ]; then
    echo "[health] $label: ok (0 running)"
    return 0
  fi

  if [ "$count" -eq 1 ]; then
    local pid
    pid="$(echo "$pids" | head -n 1)"
    local cmd cwd
    cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    cwd=""
    if [ -d /proc ] && [ -e "/proc/$pid/cwd" ]; then
      cwd="$(readlink "/proc/$pid/cwd" 2>/dev/null || true)"
    fi
    if [ -n "$cwd" ] && ! echo "$cwd" | grep -Fq "$ROOT_DIR"; then
      echo "[health] $label: FAIL foreign worker pid=$pid cwd=$cwd cmd=$cmd"
      fail=1
      return 0
    fi
    echo "[health] $label: ok pid=$pid cwd=${cwd:-unknown}"
    return 0
  fi

  echo "[health] $label: FAIL expected 1, found $count"
  echo "$pids" | sed '/^$/d' | while read -r pid; do
    cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    cwd=""
    if [ -d /proc ] && [ -e "/proc/$pid/cwd" ]; then
      cwd="$(readlink "/proc/$pid/cwd" 2>/dev/null || true)"
    fi
    echo "[health] - pid=$pid cwd=${cwd:-unknown} cmd=$cmd"
  done
  fail=1
}

# One supervisor (run-dm-live.sh) is the critical invariant.
check_family "supervisor" "tools/telethon_collector/run-dm-live.sh" "0"

# These are usually present when the pipeline is healthy, but allow zero for maintenance windows.
check_family "listener" "listen-dms.py --out" "1"
check_family "responder" "respond-dm-pending.py" "1"

if [ "$fail" -ne 0 ]; then
  echo "[health] FAIL"
  exit 1
fi

echo "[health] OK"

