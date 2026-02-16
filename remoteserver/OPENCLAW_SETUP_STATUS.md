# OpenClaw Remote Server Status (litterbox)

Last audited: **2026-02-16 06:15 UTC** via SSH.

## Environment Snapshot

- Server hostname: `litterbox`
- Public IP: `96.43.135.91`
- Tailscale IP: `100.110.29.6`
- Tailscale DNS: `litterbox.tail5951f7.ts.net`
- OS: Ubuntu 24.04.2 LTS
- Kernel: `6.8.0-90-generic`
- Docker: `28.3.2`
- Docker Compose: `v2.38.2`
- Tailscale: `1.94.2`

## OpenClaw State

- Repo path: `~/openclaw`
- Repo commit: `80abb5ab9` on `main`
- OpenClaw runtime version: `2026.2.15`
- Docker image in use: `openclaw:local`
- Default model from gateway logs: `openai-codex/gpt-5.3-codex-spark`
- Control UI is TLS-enabled (`gateway.tls.enabled: true`)
- `gateway.controlUi.allowInsecureAuth: true`
- Gateway token is configured and required for dashboard access

## Running Containers and Port Exposure

- `openclaw-openclaw-gateway-1`
  - Ports: `100.110.29.6:18789->18789`, `100.110.29.6:18790->18790`
  - Network: `openclaw_default`
- `telethon-viewer`
  - Purpose: static viewer host for this workspace (`/home/cat/.openclaw/workspace/telethon`)
  - Port: `100.110.29.6:4173->4173`
  - URL: `http://100.110.29.6:4173/viewer/`
- `tgprofile-postgres`
  - Port: `100.110.29.6:5433->5432`
  - Health: `healthy`
  - DB size: ~`374 MB`
  - Network: `openclaw_default`
- `timescaledb` (unrelated service)
  - Port: `0.0.0.0:5432->5432` and `[::]:5432->5432`
  - Network: `explorer_default`

## Access Paths

- OpenClaw UI: `https://100.110.29.6:18789/` or `https://litterbox:18789/`
- Gateway WS in Control UI settings: `wss://100.110.29.6:18790`
- Viewer UI: `http://100.110.29.6:4173/viewer/`

## Runtime-Specific Postgres Endpoint (Do Not Mix)

| Runtime context | Use this endpoint | Example URL |
|---|---|---|
| OpenClaw container on server (`openclaw-openclaw-gateway-1` / `openclaw-cli`) | Docker network service name | `postgresql://tgprofile:localdev@tgprofile-postgres:5432/tgprofile?sslmode=disable` |
| Server host shell (`cat@litterbox`) | Tailnet bind on host | `postgresql://tgprofile:localdev@100.110.29.6:5433/tgprofile?sslmode=disable` |
| Laptop over Tailscale | Tailnet bind on host | `postgresql://tgprofile:localdev@100.110.29.6:5433/tgprofile` |

## Workspace + DB Verification (OpenClaw Runtime)

Validated on **2026-02-16**:
- Workspace mount is read/write from OpenClaw container (`workspace_write_test=PASS`).
- Workspace `DATABASE_URL` points to `tgprofile-postgres:5432`.
- SQL auth/query/write from OpenClaw runtime passes (`db_query=PASS`, write probe pass).
- `tgprofile` has explicit grants/default privileges on public tables/sequences.

## Telethon Readiness (OpenClaw Runtime)

Current status:
- Telethon collector is present in workspace.
- Python venv exists at `tools/telethon_collector/.venv`.
- Telethon import passes (`telethon==1.42.0`).
- Telegram egress check passes (`api.telegram.org:443`).
- Missing inputs for actual account login/use:
  - `TG_API_ID` empty
  - `TG_API_HASH` empty
  - `TG_PHONE` empty
  - `telethon.session` missing

## Installed Tooling (Host)

Present: `git`, `rg`, `jq`, `make`, `node`, `npm`, `python3`, `docker`, `tailscale`, `curl`, `tmux`

Missing: `psql`, `pip3`, `zip`, `unzip`

## Installed Tooling (Inside OpenClaw Gateway Container)

Present: `node`, `npm`, `python3`, `pip3`, `git`, `rg`, `jq`, `psql`, `nc`, `make`, `gcc`, `g++`

Missing: `cargo`, `rustc`, `go`

## Findings and Recommendations

1. `timescaledb` is publicly exposed on `:5432`.
- Risk: not OpenClaw-related, but externally reachable.
- Recommendation: bind to localhost or Tailscale IP only, or stop service if unused.

2. `~/.openclaw` subdirectory permissions drifted from earlier hardening notes.
- Current: top-level is `700`, sensitive files are `600`, but at least `gateway/` and `devices/` are `755`.
- Recommendation: set all state subdirs to `700` unless a specific reason exists.

3. Host tooling is still light for direct host-side operations.
- Recommendation: install `postgresql-client`, `python3-pip`, `zip`, `unzip` on host.

4. Container tool fixes are applied, and build args are now configured for persistence.
- Current: `OPENCLAW_DOCKER_APT_PACKAGES` is set in `~/openclaw/.env`.
- Recommendation: rebuild/recreate OpenClaw container to fully bake these into image lifecycle.

5. Gateway auth mode is currently convenience-oriented (`gateway.controlUi.allowInsecureAuth: true`).
- Recommendation: if you want stricter operator security, set it to `false` and use paired-device auth only.

6. `tgprofile` currently has broad DB power (superuser-level).
- Recommendation: create a least-privilege app role and rotate `DATABASE_URL` to that role.

7. Telethon account setup is blocked only on credentials/session bootstrap.
- Recommendation: fill `tools/telethon_collector/.env`, run first login once to generate `telethon.session`.

## Refresh Procedure (Keep This Current)

Run from this repo:

```bash
bash remoteserver/audit-openclaw-server.sh
```

This regenerates `remoteserver/OPENCLAW_AUDIT_LATEST.md` with a fresh snapshot including:
- Workspace write probe
- Postgres access checks
- Telethon readiness checks

## Useful Commands

Restart gateway:

```bash
ssh cat@96.43.135.91 'cd ~/openclaw && docker compose restart openclaw-gateway'
```

Open gateway logs:

```bash
ssh cat@96.43.135.91 'cd ~/openclaw && docker compose logs -f openclaw-gateway'
```

OpenClaw health:

```bash
ssh cat@96.43.135.91 'docker exec openclaw-openclaw-gateway-1 node dist/index.js health'
```

Run current audit:

```bash
bash remoteserver/audit-openclaw-server.sh
```
