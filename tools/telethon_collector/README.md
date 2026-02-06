# Telethon Collector

Collects messages and participants from a single Telegram group using your own Telegram account via [Telethon](https://docs.telethon.dev/).

## Prerequisites

- Python ≥ 3.10
- A Telegram account
- API credentials from [my.telegram.org/apps](https://my.telegram.org/apps)

## Setup

### 1. Create a virtual environment

```bash
# From repo root:
make tg:venv
```

Or manually:

```bash
cd tools/telethon_collector
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp tools/telethon_collector/.env.example tools/telethon_collector/.env
```

Edit `tools/telethon_collector/.env` and fill in:

| Variable | Description |
|---|---|
| `TG_API_ID` | Numeric API ID from my.telegram.org |
| `TG_API_HASH` | API hash string from my.telegram.org |
| `TG_PHONE` | Your phone number in international format (e.g. `+15551234567`) |
| `TG_SESSION_PATH` | Path to store the session file (default: `tools/telethon_collector/telethon.session`) |

### 3. First run — authenticate

On the first run, Telethon will prompt you for a verification code sent to your Telegram app. This creates a session file so subsequent runs are automatic.

## Usage

### List your dialogs (find group IDs)

```bash
make tg:list-dialogs
```

This prints all your chats with their title, ID, username, and type. Use this to identify the exact group name or ID for collection.

### Collect a group

```bash
# By exact title:
make tg:collect GROUP="BD in Web3"

# By username:
make tg:collect GROUP="@bdinweb3"

# By numeric ID:
make tg:collect GROUP="-1001234567890"

# With options:
make tg:collect GROUP="BD in Web3" LIMIT=10000 SINCE="2025-01-01"
```

Or directly:

```bash
source tools/telethon_collector/.venv/bin/activate
python tools/telethon_collector/collect_group_export.py \
  --group "BD in Web3" \
  --out data/exports/telethon_bd_web3.json \
  --limit 5000
```

### Arguments

| Arg | Default | Description |
|---|---|---|
| `--group` | (required) | Group title, @username, or numeric ID |
| `--out` | `data/exports/telethon_bd_web3.json` | Output file path |
| `--limit` | `5000` | Max messages to fetch |
| `--since` | (none) | Only messages after this date (YYYY-MM-DD) |
| `--include-participants` | `true` | Attempt to collect full participant list |

## Output

A single JSON file at the `--out` path with this structure:

```json
{
  "name": "BD in Web3",
  "type": "supergroup",
  "id": 1234567890,
  "collected_at": "2026-02-06T12:00:00+00:00",
  "participants_status": "ok",
  "participants_error": null,
  "participants_count": 150,
  "messages_count": 3000,
  "limits": { "since": null, "limit": 5000 },
  "participants": [ ... ],
  "messages": [ ... ]
}
```

### Participant fallback

If Telegram restricts participant enumeration (admin-only groups, privacy settings, etc.), the collector gracefully falls back:

- Messages are still collected normally
- Message senders are still discoverable
- `participants_status` is set to `"unavailable"`
- `participants_error` contains the error description
- `participants` is an empty array
- `participants_count` is `null`

This is expected for many large groups. The ingest pipeline will still work — it ingests users from both `participants[]` and message senders.

## Files

| File | Purpose |
|---|---|
| `requirements.txt` | Pinned Python dependencies |
| `.env.example` | Template for credentials |
| `.env` | Your credentials (gitignored) |
| `*.session` | Telethon session file (gitignored) |
| `list_dialogs.py` | List all your Telegram chats |
| `collect_group_export.py` | Main collector script |
