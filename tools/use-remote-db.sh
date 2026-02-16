#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

REMOTE_URL_DEFAULT="postgresql://tgprofile:localdev@litterbox:5433/tgprofile?sslmode=disable"
REMOTE_URL_IP_DEFAULT="postgresql://tgprofile:localdev@100.110.29.6:5433/tgprofile?sslmode=disable"

if [[ ! -f "$ENV_FILE" ]]; then
  cat >"$ENV_FILE" <<'EOT'
POSTGRES_USER=tgprofile
POSTGRES_PASSWORD=localdev
POSTGRES_DB=tgprofile
POSTGRES_PORT=5432
DATABASE_URL=
DATABASE_URL_DOCKER=postgresql://tgprofile:localdev@postgres:5432/tgprofile?sslmode=disable
EOT
fi

set_kv() {
  local key="$1"
  local value="$2"
  if rg -q "^${key}=" "$ENV_FILE"; then
    # macOS/BSD sed requires -i ''
    sed -i '' -E "s#^${key}=.*#${key}=${value}#" "$ENV_FILE"
  else
    printf '\n%s=%s\n' "$key" "$value" >>"$ENV_FILE"
  fi
}

# Prefer MagicDNS host; fall back to Tailnet IP if user passes --ip.
if [[ "${1:-}" == "--ip" ]]; then
  set_kv "DATABASE_URL" "$REMOTE_URL_IP_DEFAULT"
else
  set_kv "DATABASE_URL" "$REMOTE_URL_DEFAULT"
fi

# Keep docker-compose local migration URL intact.
if ! rg -q '^DATABASE_URL_DOCKER=' "$ENV_FILE"; then
  set_kv "DATABASE_URL_DOCKER" "postgresql://tgprofile:localdev@postgres:5432/tgprofile?sslmode=disable"
fi

echo "Updated .env DATABASE_URL for REMOTE server Postgres."
echo "  DATABASE_URL=$(rg -n '^DATABASE_URL=' "$ENV_FILE" | head -n 1 | cut -d= -f2-)"
