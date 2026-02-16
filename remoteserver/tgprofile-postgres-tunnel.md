# Postgres Endpoint by Runtime

Last validated: **2026-02-16**.

## Primary Rule

- If OpenClaw is running on the server in Docker, use `tgprofile-postgres:5432`.
- If the client is outside that Docker network (laptop/Tailscale), use `100.110.29.6:5433`.

## Runtime Matrix

| Runtime context | Endpoint | URL |
|---|---|---|
| OpenClaw on server (`openclaw-openclaw-gateway-1`, `openclaw-cli`) | `tgprofile-postgres:5432` | `postgresql://tgprofile:localdev@tgprofile-postgres:5432/tgprofile?sslmode=disable` |
| Server host shell (`cat@litterbox`) | `100.110.29.6:5433` | `postgresql://tgprofile:localdev@100.110.29.6:5433/tgprofile?sslmode=disable` |
| Laptop over Tailscale | `100.110.29.6:5433` | `postgresql://tgprofile:localdev@100.110.29.6:5433/tgprofile` |

## Why This Split Exists

- `tgprofile-postgres` is a Docker service name resolvable only inside the Docker network.
- `100.110.29.6:5433` is the host's Tailscale-exposed bind for external clients.
- `litterbox:5433` can be DNS-path dependent; use IP if you want deterministic behavior.

## Commands

OpenClaw/server-container runtime:

```bash
DATABASE_URL="postgresql://tgprofile:localdev@tgprofile-postgres:5432/tgprofile?sslmode=disable"
```

Laptop runtime:

```bash
psql "postgresql://tgprofile:localdev@100.110.29.6:5433/tgprofile"
```

SSH tunnel fallback (laptop):

```bash
ssh -N -L 5433:100.110.29.6:5433 cat@96.43.135.91
psql "postgresql://tgprofile:localdev@127.0.0.1:5433/tgprofile"
```

## Quick Health Checks

From your laptop:

```bash
tailscale ping 100.110.29.6
nc -vz -G 3 100.110.29.6 5433
```

From server (no host `psql` required):

```bash
ssh cat@96.43.135.91 \
  'docker exec -e PGPASSWORD=localdev -i tgprofile-postgres psql -U tgprofile -d tgprofile -Atc "select now(), pg_size_pretty(pg_database_size(current_database()));"'
```
