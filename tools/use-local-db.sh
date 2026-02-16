#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

LOCAL_URL="postgresql://tgprofile:localdev@localhost:5432/tgprofile?sslmode=disable"

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
    sed -i '' -E "s#^${key}=.*#${key}=${value}#" "$ENV_FILE"
  else
    printf '\n%s=%s\n' "$key" "$value" >>"$ENV_FILE"
  fi
}

set_kv "DATABASE_URL" "$LOCAL_URL"

if ! rg -q '^DATABASE_URL_DOCKER=' "$ENV_FILE"; then
  set_kv "DATABASE_URL_DOCKER" "postgresql://tgprofile:localdev@postgres:5432/tgprofile?sslmode=disable"
fi

echo "Updated .env DATABASE_URL for LOCAL Postgres (localhost:5432)."
echo "  DATABASE_URL=$(rg -n '^DATABASE_URL=' "$ENV_FILE" | head -n 1 | cut -d= -f2-)"
