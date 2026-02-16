# OpenClaw Remote Server Status (litterbox)

Last audited: **2026-02-16 05:28 UTC** via SSH.

## Environment Snapshot

- Server hostname: `litterbox`
- Public IP: `96.43.135.91`
- Tailscale IP: `100.110.29.6`
- Tailscale DNS: `litterbox.tail5951f7.ts.net`
- OS: Ubuntu 24.04.2 LTS
- Kernel: `6.8.0-90-generic`
- Uptime at audit: ~24 days
- Docker: `28.3.2`
- Docker Compose: `v2.38.2`
- Tailscale: `1.94.2`

## OpenClaw State

- Repo path: `~/openclaw`
- Repo commit: `80abb5ab9` on `main`
- OpenClaw runtime version: `2026.2.15`
- Docker image in use: `openclaw:local` (built ~20h before audit)
- Default model from gateway logs: `openai-codex/gpt-5.3-codex-spark`
- Control UI is TLS-enabled (`gateway.tls.enabled: true`)
- `gateway.controlUi.allowInsecureAuth: true`
- Gateway token is configured and required for dashboard access
- OAuth/device identity files exist under `~/.openclaw/identity`

## Running Containers and Port Exposure

- `openclaw-openclaw-gateway-1`
  - Ports: `100.110.29.6:18789->18789`, `100.110.29.6:18790->18790`
  - Network: `openclaw_default`
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

## Runtime-Specific Postgres Endpoint (Do Not Mix)

| Runtime context | Use this endpoint | Example URL |
|---|---|---|
| OpenClaw container on server (`openclaw-openclaw-gateway-1` / `openclaw-cli`) | Docker network service name | `postgresql://tgprofile:localdev@tgprofile-postgres:5432/tgprofile?sslmode=disable` |
| Server host shell (`cat@litterbox`) | Tailnet bind on host | `postgresql://tgprofile:localdev@100.110.29.6:5433/tgprofile?sslmode=disable` |
| Laptop over Tailscale | Tailnet bind on host | `postgresql://tgprofile:localdev@100.110.29.6:5433/tgprofile` |

Notes:
- For OpenClaw running on the server, `tgprofile-postgres:5432` is the primary endpoint.
- `litterbox:5433` may be inconsistent depending on runtime DNS path; `100.110.29.6:5433` is the stable external endpoint.

## Postgres Access Verification (OpenClaw)

Validated on **2026-02-16**:
- Workspace `DATABASE_URL` is set to `...@tgprofile-postgres:5432/...` on server.
- `tgprofile` role has read/write access across existing public tables and sequences.
- Explicit grants/default privileges were applied so future objects keep the same access profile.
- Write probe passed (`CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`) inside a rolled-back transaction.

## Installed Tooling (Host)

Present: `git`, `rg`, `jq`, `make`, `node`, `npm`, `python3`, `docker`, `tailscale`, `curl`, `tmux`

Missing: `psql`, `pip3`, `zip`, `unzip`

## Installed Tooling (Inside OpenClaw Gateway Container)

Present: `node`, `npm`, `python3`, `git`, `make`, `gcc`, `g++`

Missing: `pip3`, `rg`, `jq`, `psql`, `cargo`, `rustc`, `go`

## Findings and Recommendations

1. `timescaledb` is publicly exposed on `:5432`.
- Risk: not OpenClaw-related, but externally reachable.
- Recommendation: bind to localhost or Tailscale IP only, or stop service if unused.

2. `~/.openclaw` subdirectory permissions drifted from earlier hardening notes.
- Current: top-level is `700`, sensitive files are `600`, but at least `gateway/` and `devices/` are `755`.
- Recommendation: set all state subdirs to `700` unless a specific reason exists.

3. OpenClaw host tooling is usable but not fully operator-friendly.
- Recommendation: install `postgresql-client`, `python3-pip`, `zip`, `unzip` on host for easier maintenance.

4. Container apt extras are currently empty (`OPENCLAW_DOCKER_APT_PACKAGES=`).
- Recommendation: if you want richer in-agent tooling, set this and rebuild `openclaw:local`, for example:
  - `ripgrep jq postgresql-client unzip zip`

5. Gateway auth mode is currently convenience-oriented (`gateway.controlUi.allowInsecureAuth: true`).
- Recommendation: if you want stricter operator security, set it to `false` and use paired-device auth only.

6. `tgprofile` currently has broad DB power (superuser-level).
- Recommendation: if you want least-privilege hardening, create a non-superuser app role for OpenClaw and rotate `DATABASE_URL` to that role.

## Current Behavior Notes

- Gateway health check is passing (`node dist/index.js health`).
- Dashboard auth failures in logs are mostly from sessions that did not paste a token yet (`token_missing`).
- Successful authenticated dashboard sessions are present in recent logs.

## Refresh Procedure (Keep This Current)

Run from this repo:

```bash
bash remoteserver/audit-openclaw-server.sh
```

This regenerates `remoteserver/OPENCLAW_AUDIT_LATEST.md` with a fresh timestamped snapshot.

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

List listening ports:

```bash
ssh cat@96.43.135.91 'ss -tulpn'
```
