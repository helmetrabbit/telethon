# Remote Server Docs

This folder tracks live operational state for the OpenClaw server (`litterbox`).

## Endpoint Rule (Most Important)

- OpenClaw running on the server (Docker): use `tgprofile-postgres:5432`.
- Laptop or any external client over Tailscale: use `100.110.29.6:5433`.

## Files

- `OPENCLAW_SETUP_STATUS.md`: human-curated summary and recommendations.
- `OPENCLAW_AUDIT_LATEST.md`: generated snapshot from the audit script.
- `audit-openclaw-server.sh`: regenerates `OPENCLAW_AUDIT_LATEST.md`.
- `tgprofile-postgres-tunnel.md`: runtime-specific endpoint and connection commands.

## Refresh Audit Snapshot

Run from repo root:

```bash
bash remoteserver/audit-openclaw-server.sh
```

Optional arguments:

```bash
bash remoteserver/audit-openclaw-server.sh cat@96.43.135.91 remoteserver/OPENCLAW_AUDIT_LATEST.md
```

The generated audit now includes:
- workspace read/write probe from inside OpenClaw runtime
- Postgres access/privilege checks for `tgprofile`
- Telethon runtime readiness (venv/import/env/session/Telegram egress)
- viewer endpoint health check (`http://<tailnet-ip>:4173/viewer/`)
