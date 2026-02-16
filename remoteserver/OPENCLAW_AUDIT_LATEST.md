# OpenClaw Audit Snapshot

- Generated (UTC): 2026-02-16T07:22:35Z
- Remote: `cat@96.43.135.91`

## Host and Runtime

```text
hostname: litterbox
user: cat
time: 2026-02-16T07:22:39+00:00
os: Ubuntu 24.04.2 LTS
kernel: 6.8.0-90-generic
uptime: 07:22:39 up 24 days, 15:58,  1 user,  load average: 0.24, 0.31, 0.23
docker: Docker version 28.3.2, build 578ccf6
compose: Docker Compose version v2.38.2
tailscale: 1.94.2
tailnet_ip: 100.110.29.6
```

## OpenClaw Repo

```text
path: /home/cat/openclaw
commit: 80abb5ab9
branch: main
openclaw_version: 2026.2.15
```

## Containers

```text
NAMES                         IMAGE                  STATUS                  PORTS
telethon-viewer               python:3.11-alpine     Up 6 minutes            100.110.29.6:4173->4173/tcp
openclaw-openclaw-gateway-1   openclaw:local         Up 16 hours             100.110.29.6:18789-18790->18789-18790/tcp
tgprofile-postgres            postgres:16-alpine     Up 17 hours (healthy)   100.110.29.6:5433->5432/tcp
timescaledb                   explorer-timescaledb   Up 3 weeks              0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp
```

## Listening Ports (selected)

```text
tcp   LISTEN 0      4096                       0.0.0.0:22         0.0.0.0:*          
tcp   LISTEN 0      4096                  100.110.29.6:5433       0.0.0.0:*          
tcp   LISTEN 0      4096                  100.110.29.6:4173       0.0.0.0:*          
tcp   LISTEN 0      4096                       0.0.0.0:5432       0.0.0.0:*          
tcp   LISTEN 0      4096                  100.110.29.6:18790      0.0.0.0:*          
tcp   LISTEN 0      4096                  100.110.29.6:18789      0.0.0.0:*          
tcp   LISTEN 0      4096                          [::]:22            [::]:*          
tcp   LISTEN 0      4096                          [::]:5432          [::]:*          
```

## OpenClaw JSON Summary

```text
{
  "gateway": {
    "tlsEnabled": true,
    "controlUiAllowInsecureAuth": true,
    "tokenPresent": true
  },
  "counts": {
    "channels": 0,
    "hooks": 0,
    "skills": 0
  }
}
```

## Configured Env Keys (names only)

```text
OPENCLAW_BRIDGE_PORT
OPENCLAW_CONFIG_DIR
OPENCLAW_DOCKER_APT_PACKAGES
OPENCLAW_GATEWAY_BIND
OPENCLAW_GATEWAY_PORT
OPENCLAW_GATEWAY_TOKEN
OPENCLAW_IMAGE
OPENCLAW_WORKSPACE_DIR
```

## Workspace DATABASE_URL (redacted)

```text
1:DATABASE_URL=postgresql://tgprofile:********@tgprofile-postgres:5432/tgprofile?sslmode=disable
```

## Workspace Access Probe

```text
uid=1000(node) gid=1000(node) groups=1000(node)
drwx------  6 node node 4096 Feb 16 06:55 /home/node/.openclaw/workspace
drwxr-xr-x 13 node node 4096 Feb 16 07:20 /home/node/.openclaw/workspace/telethon
-rw-r--r-- 1 node node 6 Feb 16 07:22 /home/node/.openclaw/workspace/telethon/.openclaw_rw_probe_6914
workspace_write_test=PASS
```

## OpenClaw State Permissions

```text
drwx------ 11 cat cat 4096 Feb 16 05:27 /home/cat/.openclaw
drwx------  3 cat cat 4096 Feb 15 09:50 /home/cat/.openclaw/agents
drwx------  2 cat cat 4096 Feb 15 09:54 /home/cat/.openclaw/canvas
drwx------  2 cat cat 4096 Feb 15 09:53 /home/cat/.openclaw/completions
drwx------  2 cat cat 4096 Feb 15 15:35 /home/cat/.openclaw/cron
drwxr-xr-x  2 cat cat 4096 Feb 16 07:20 /home/cat/.openclaw/devices
-rw-------  1 cat cat  176 Feb 16 05:27 /home/cat/.openclaw/exec-approvals.json
drwxr-xr-x  3 cat cat 4096 Feb 15 15:35 /home/cat/.openclaw/gateway
drwx------  2 cat cat 4096 Feb 15 10:00 /home/cat/.openclaw/identity
drwx------  2 cat cat 4096 Feb 15 09:51 /home/cat/.openclaw/logs
-rw-------  1 cat cat 1510 Feb 15 15:34 /home/cat/.openclaw/openclaw.json
-rw-------  1 cat cat 1235 Feb 15 09:52 /home/cat/.openclaw/openclaw.json.bak
-rw-------  1 cat cat   49 Feb 15 09:54 /home/cat/.openclaw/update-check.json
drwx------  6 cat cat 4096 Feb 16 06:55 /home/cat/.openclaw/workspace
```

## Host Tooling

```text
git        git version 2.43.0
rg         ripgrep 14.1.0
jq         jq-1.7
make       GNU Make 4.3
node       v18.19.1
npm        9.2.0
python3    Python 3.12.3
pip3       MISSING
psql       MISSING
docker     Docker version 28.3.2, build 578ccf6
tailscale  1.94.2
curl       curl 8.5.0 (x86_64-pc-linux-gnu) libcurl/8.5.0 OpenSSL/3.0.13 zlib/1.3 brotli/1.1.0 zstd/1.5.5 libidn2/2.3.7 libpsl/0.21.2 (+libidn2/2.3.7) libssh/0.10.6/openssl/zlib nghttp2/1.59.0 librtmp/2.3 OpenLDAP/2.6.7
unzip      MISSING
zip        MISSING
tmux       
```

## OpenClaw Container Tooling

```text
node: v22.22.0
npm: 10.9.4
python3: Python 3.11.2
pip3: pip 23.0.1 from /usr/lib/python3/dist-packages/pip (python 3.11)
git: git version 2.39.5
rg: ripgrep 13.0.0
jq: jq-1.6
psql: psql (PostgreSQL) 15.16 (Debian 15.16-0+deb12u1)
make: GNU Make 4.3
gcc: gcc (Debian 12.2.0-14+deb12u1) 12.2.0
g++: g++ (Debian 12.2.0-14+deb12u1) 12.2.0
cargo: MISSING
rustc: MISSING
go: MISSING
```

## Gateway Health

```text
Agents: main (default)
Heartbeat interval: 30m (main)
Session store (main): /home/node/.openclaw/agents/main/sessions/sessions.json (1 entries)
- agent:main:main (10m ago)
```

## tgprofile DB Access

```text
role	rolcanlogin	rolsuper	rolcreatedb	rolcreaterole
tgprofile	t	t	t	t
(1 row)
db_privs_ok
t
(1 row)
schema_privs_ok
t
(1 row)
total_tables	rw_tables
19	19
(1 row)
total_sequences	rw_sequences
12	12
(1 row)
defaclobjtype	definer	schema	defaclacl
S	tgprofile	public	{tgprofile=rwU/tgprofile}
f	tgprofile	public	{tgprofile=X/tgprofile}
r	tgprofile	public	{tgprofile=arwdDxt/tgprofile}
(3 rows)
```

## Telethon Readiness

```text
telethon_collector=present
telethon_venv=present
telethon_import=PASS version=1.42.0
TG_API_ID=set
TG_API_HASH=set
TG_PHONE=set
TG_SESSION_PATH=set
telethon_session=missing
telegram_egress=PASS
```

## Viewer Check

```text
viewer_url=http://100.110.29.6:4173/viewer/
HTTP/1.0 200 OK
```

## tgprofile DB Stats

```text
tgprofile|tgprofile|375 MB
```

