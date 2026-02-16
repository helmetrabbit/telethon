#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOTE="${1:-cat@96.43.135.91}"
OUT_FILE="${2:-$ROOT_DIR/remoteserver/OPENCLAW_AUDIT_LATEST.md}"

mkdir -p "$(dirname "$OUT_FILE")"

{
  echo "# OpenClaw Audit Snapshot"
  echo
  echo "- Generated (UTC): $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  printf -- '- Remote: `%s`\n' "$REMOTE"
  echo

  ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE" 'bash -s' <<'REMOTE'
set -euo pipefail

print_block() {
  local title="$1"
  shift
  echo "## $title"
  echo
  echo '```text'
  "$@"
  echo '```'
  echo
}

host_runtime() {
  echo "hostname: $(hostname)"
  echo "user: $(whoami)"
  echo "time: $(date -Is)"
  echo "os: $(grep -E '^PRETTY_NAME=' /etc/os-release | cut -d= -f2- | tr -d '\"')"
  echo "kernel: $(uname -r)"
  echo "uptime: $(uptime | sed 's/^ *//')"
  echo "docker: $(docker --version 2>/dev/null || echo 'missing')"
  echo "compose: $(docker compose version 2>/dev/null || echo 'missing')"
  echo "tailscale: $(tailscale version 2>/dev/null | head -n1 || echo 'missing')"
  echo "tailnet_ip: $(tailscale ip -4 2>/dev/null | head -n1 || echo 'unknown')"
}

openclaw_repo() {
  if [ ! -d "$HOME/openclaw" ]; then
    echo "~/openclaw not found"
    return
  fi
  cd "$HOME/openclaw"
  echo "path: $(pwd)"
  echo "commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "branch: $(git branch --show-current 2>/dev/null || echo unknown)"
  if docker ps --format '{{.Names}}' | grep -qx 'openclaw-openclaw-gateway-1'; then
    echo "openclaw_version: $(docker exec openclaw-openclaw-gateway-1 node dist/index.js --version 2>/dev/null || echo unknown)"
  else
    echo "openclaw_version: unknown (gateway container not running)"
  fi
}

containers() {
  docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
}

ports() {
  ss -tulpn | grep -E '(:22|:18789|:18790|:5432|:5433)' || true
}

openclaw_json_summary() {
  if [ -f "$HOME/.openclaw/openclaw.json" ] && command -v jq >/dev/null 2>&1; then
    jq -r '{gateway:{tlsEnabled:.gateway.tls.enabled,controlUiAllowInsecureAuth:.gateway.controlUi.allowInsecureAuth,tokenPresent:(.gateway.auth.token|type=="string" and length>0)},counts:{channels:(.channels|length),hooks:(.hooks|length),skills:(.skills|length)}}' "$HOME/.openclaw/openclaw.json"
  else
    echo "openclaw.json or jq not available"
  fi
}

env_keys() {
  if [ -f "$HOME/openclaw/.env" ]; then
    awk -F= '/^(OPENCLAW_|OPENAI_|ANTHROPIC_|GOOGLE_|AZURE_|XAI_|OPENROUTER_)/ && length($2)>0 {print $1}' "$HOME/openclaw/.env" | sort
  else
    echo "~/openclaw/.env not found"
  fi
}

workspace_db_url() {
  if [ -f "$HOME/.openclaw/workspace/telethon/.env" ]; then
    rg -n '^DATABASE_URL=' "$HOME/.openclaw/workspace/telethon/.env" | sed -E 's#(://[^:]+:)[^@]+@#\1********@#'
  else
    echo "~/.openclaw/workspace/telethon/.env not found"
  fi
}

workspace_rw() {
  if ! docker ps --format '{{.Names}}' | grep -qx 'openclaw-openclaw-gateway-1'; then
    echo "openclaw-openclaw-gateway-1 not running"
    return
  fi
  docker exec openclaw-openclaw-gateway-1 sh -lc '
    set -e
    id
    ls -ld /home/node/.openclaw/workspace /home/node/.openclaw/workspace/telethon
    probe=/home/node/.openclaw/workspace/telethon/.openclaw_rw_probe_$$
    echo "probe" > "$probe"
    ls -l "$probe"
    rm -f "$probe"
    echo "workspace_write_test=PASS"
  '
}

openclaw_permissions() {
  ls -ld "$HOME/.openclaw" "$HOME/.openclaw"/* 2>/dev/null || true
}

host_tools() {
  for t in git rg jq make node npm python3 pip3 psql docker tailscale curl unzip zip tmux; do
    if command -v "$t" >/dev/null 2>&1; then
      v="$($t --version 2>/dev/null | head -n1 || true)"
      printf '%-10s %s\n' "$t" "$v"
    else
      printf '%-10s %s\n' "$t" 'MISSING'
    fi
  done
}

container_tools() {
  if ! docker ps --format '{{.Names}}' | grep -qx 'openclaw-openclaw-gateway-1'; then
    echo "openclaw-openclaw-gateway-1 not running"
    return
  fi
  docker exec openclaw-openclaw-gateway-1 sh -lc 'for t in node npm python3 pip3 git rg jq psql make gcc g++ cargo rustc go; do if command -v "$t" >/dev/null 2>&1; then v=$($t --version 2>/dev/null | head -n1); echo "$t: $v"; else echo "$t: MISSING"; fi; done'
}

gateway_health() {
  if docker ps --format '{{.Names}}' | grep -qx 'openclaw-openclaw-gateway-1'; then
    docker exec openclaw-openclaw-gateway-1 node dist/index.js health
  else
    echo "openclaw-openclaw-gateway-1 not running"
  fi
}

db_access() {
  if ! docker ps --format '{{.Names}}' | grep -qx 'tgprofile-postgres'; then
    echo "tgprofile-postgres not running"
    return
  fi
  docker exec -e PGPASSWORD=localdev -i tgprofile-postgres psql -U tgprofile -d tgprofile -P pager=off -F $'\t' -A <<'SQL'
select r.rolname as role, r.rolcanlogin, r.rolsuper, r.rolcreatedb, r.rolcreaterole
from pg_roles r where r.rolname='tgprofile';
select has_database_privilege('tgprofile','tgprofile','CONNECT,CREATE,TEMPORARY') as db_privs_ok;
select has_schema_privilege('tgprofile','public','USAGE,CREATE') as schema_privs_ok;
select count(*) as total_tables,
       sum(case when has_table_privilege('tgprofile', format('%I.%I', schemaname, tablename), 'SELECT,INSERT,UPDATE,DELETE') then 1 else 0 end) as rw_tables
from pg_tables where schemaname='public';
select count(*) as total_sequences,
       sum(case when has_sequence_privilege('tgprofile', format('%I.%I', sequence_schema, sequence_name), 'USAGE,SELECT,UPDATE') then 1 else 0 end) as rw_sequences
from information_schema.sequences where sequence_schema='public';
select defaclobjtype,
       pg_get_userbyid(defaclrole) as definer,
       coalesce((select nspname from pg_namespace where oid=defaclnamespace), '<all>') as schema,
       defaclacl::text
from pg_default_acl
where pg_get_userbyid(defaclrole)='tgprofile'
order by 1;
SQL
}

telethon_readiness() {
  if ! docker ps --format '{{.Names}}' | grep -qx 'openclaw-openclaw-gateway-1'; then
    echo "openclaw-openclaw-gateway-1 not running"
    return
  fi
  docker exec openclaw-openclaw-gateway-1 sh -lc '
    set -e
    base=/home/node/.openclaw/workspace/telethon/tools/telethon_collector
    if [ ! -d "$base" ]; then
      echo "telethon_collector=missing"
      exit 0
    fi
    echo "telethon_collector=present"
    if [ -x "$base/.venv/bin/python" ]; then
      echo "telethon_venv=present"
      "$base/.venv/bin/python" - <<'"'"'PY'"'"'
import telethon
print("telethon_import=PASS version=" + telethon.__version__)
PY
    else
      echo "telethon_venv=missing"
    fi
    if [ -f "$base/.env" ]; then
      python3 - <<'"'"'PY'"'"'
from pathlib import Path
p = Path("/home/node/.openclaw/workspace/telethon/tools/telethon_collector/.env")
keys = ["TG_API_ID", "TG_API_HASH", "TG_PHONE", "TG_SESSION_PATH"]
vals = {}
for line in p.read_text().splitlines():
    s = line.strip()
    if not s or s.startswith("#") or "=" not in s:
        continue
    k, v = s.split("=", 1)
    vals[k.strip()] = v.strip()
for k in keys:
    print(f"{k}=" + ("set" if vals.get(k, "") else "empty"))
PY
    else
      echo "telethon_env_file=missing"
    fi
    [ -f "$base/telethon.session" ] && echo "telethon_session=present" || echo "telethon_session=missing"
    if command -v nc >/dev/null 2>&1; then
      (nc -vz -w 5 api.telegram.org 443 >/dev/null 2>&1 && echo "telegram_egress=PASS") || echo "telegram_egress=FAIL"
    else
      echo "telegram_egress=unknown (nc missing)"
    fi
  '
}

db_stats() {
  if docker ps --format '{{.Names}}' | grep -qx 'tgprofile-postgres'; then
    docker exec -e PGPASSWORD=localdev -i tgprofile-postgres psql -U tgprofile -d tgprofile -Atc "select current_database(), current_user, pg_size_pretty(pg_database_size(current_database()));"
  else
    echo "tgprofile-postgres not running"
  fi
}

print_block "Host and Runtime" host_runtime
print_block "OpenClaw Repo" openclaw_repo
print_block "Containers" containers
print_block "Listening Ports (selected)" ports
print_block "OpenClaw JSON Summary" openclaw_json_summary
print_block "Configured Env Keys (names only)" env_keys
print_block "Workspace DATABASE_URL (redacted)" workspace_db_url
print_block "Workspace Access Probe" workspace_rw
print_block "OpenClaw State Permissions" openclaw_permissions
print_block "Host Tooling" host_tools
print_block "OpenClaw Container Tooling" container_tools
print_block "Gateway Health" gateway_health
print_block "tgprofile DB Access" db_access
print_block "Telethon Readiness" telethon_readiness
print_block "tgprofile DB Stats" db_stats
REMOTE
} >"$OUT_FILE"

echo "Wrote $OUT_FILE"
