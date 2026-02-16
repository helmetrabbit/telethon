#!/usr/bin/env bash
set -euo pipefail

# Secure Telethon bootstrap:
# - Prompts for TG_API_ID / TG_API_HASH at runtime (hash hidden)
# - Does NOT write API credentials into workspace files
# - Stores session under ~/.telethon-secrets (outside OpenClaw workspace mount)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COLLECTOR_DIR="$ROOT_DIR/tools/telethon_collector"
VENV_DIR="$COLLECTOR_DIR/.venv"
SECRETS_DIR="${HOME}/.telethon-secrets"

DEFAULT_LABEL="lobster_llama"
DEFAULT_PHONE="+19406239182"

if [[ ! -d "$COLLECTOR_DIR" ]]; then
  echo "Collector directory not found: $COLLECTOR_DIR" >&2
  exit 1
fi

mkdir -p "$SECRETS_DIR"
chmod 700 "$SECRETS_DIR"

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  echo "Creating venv at $VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

echo "Installing/updating Telethon deps in venv..."
"$VENV_DIR/bin/pip" install -q -r "$COLLECTOR_DIR/requirements.txt"

read -r -p "Session label [$DEFAULT_LABEL]: " SESSION_LABEL
SESSION_LABEL="${SESSION_LABEL:-$DEFAULT_LABEL}"
SESSION_PATH="$SECRETS_DIR/${SESSION_LABEL}.session"

read -r -p "TG_API_ID: " TG_API_ID
while [[ -z "${TG_API_ID}" ]]; do
  read -r -p "TG_API_ID cannot be empty. Enter TG_API_ID: " TG_API_ID
done

read -r -s -p "TG_API_HASH (hidden): " TG_API_HASH
echo
while [[ -z "${TG_API_HASH}" ]]; do
  read -r -s -p "TG_API_HASH cannot be empty. Enter TG_API_HASH: " TG_API_HASH
  echo
done

read -r -p "TG_PHONE [$DEFAULT_PHONE]: " TG_PHONE
TG_PHONE="${TG_PHONE:-$DEFAULT_PHONE}"

echo
echo "Starting first Telethon login flow..."
echo "You may be prompted for Telegram code and 2FA password."
echo "Session target: $SESSION_PATH"
echo

TG_API_ID="$TG_API_ID" \
TG_API_HASH="$TG_API_HASH" \
TG_PHONE="$TG_PHONE" \
TG_SESSION_PATH="$SESSION_PATH" \
"$VENV_DIR/bin/python" "$COLLECTOR_DIR/list_dialogs.py"

if [[ -f "$SESSION_PATH" ]]; then
  chmod 600 "$SESSION_PATH"
else
  echo "Session file was not created: $SESSION_PATH" >&2
  exit 1
fi

PROFILE_PATH="$SECRETS_DIR/${SESSION_LABEL}.profile"
cat >"$PROFILE_PATH" <<EOF
TG_PHONE=$TG_PHONE
TG_SESSION_PATH=$SESSION_PATH
EOF
chmod 600 "$PROFILE_PATH"

echo
echo "Bootstrap complete."
echo "Session file: $SESSION_PATH"
echo "Profile file (non-secret): $PROFILE_PATH"
echo "API credentials were not written to disk by this script."
