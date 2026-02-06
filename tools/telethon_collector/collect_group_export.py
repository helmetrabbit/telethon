#!/usr/bin/env python3
"""
Collect messages and participants from a single Telegram group and
write a JSON export compatible with the tg-profile-engine ingest pipeline.

Usage:
    python tools/telethon_collector/collect_group_export.py \
        --group "BD in Web3" \
        --out data/exports/telethon_bd_web3.json \
        --limit 5000

See tools/telethon_collector/README.md for full documentation.
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import (
    ChatAdminRequiredError,
    ChannelPrivateError,
    FloodWaitError,
)
from telethon.tl.types import Channel, Chat, User, MessageService

# ── Load .env from the collector directory ──────────────
_SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(_SCRIPT_DIR / ".env")

API_ID = os.getenv("TG_API_ID")
API_HASH = os.getenv("TG_API_HASH")
SESSION_PATH = os.getenv("TG_SESSION_PATH", str(_SCRIPT_DIR / "telethon.session"))

# Max retries for FloodWaitError during participant enumeration
_FLOOD_MAX_RETRIES = 3


# ── Argument parsing ────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Collect Telegram group messages + participants to JSON.",
    )
    p.add_argument(
        "--group",
        required=True,
        help="Group title, @username, or numeric ID.",
    )
    p.add_argument(
        "--out",
        default="data/exports/telethon_bd_web3.json",
        help="Output JSON path (default: data/exports/telethon_bd_web3.json).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Max messages to fetch (default: 5000).",
    )
    p.add_argument(
        "--since",
        default=None,
        help="Only messages after this date (YYYY-MM-DD).",
    )
    p.add_argument(
        "--include-participants",
        default="true",
        choices=["true", "false"],
        help="Attempt to collect full participant list (default: true).",
    )
    return p.parse_args()


# ── Group resolution ────────────────────────────────────

async def resolve_group(client: TelegramClient, group_arg: str):
    """
    Try to resolve the group argument to a Telethon entity.
    Falls back to fuzzy-matching dialog titles if direct resolution fails.
    """
    # Try numeric ID
    try:
        group_id = int(group_arg)
        return await client.get_entity(group_id)
    except (ValueError, Exception):
        pass

    # Try direct resolution (username or exact title)
    try:
        return await client.get_entity(group_arg)
    except Exception:
        pass

    # Fallback: fuzzy-match against dialog titles
    print(f"\n⚠️  Could not resolve '{group_arg}' directly. Searching dialogs...\n")

    matches = []
    needle = group_arg.lower().strip().lstrip("@")
    async for dialog in client.iter_dialogs():
        title = (dialog.title or "").lower()
        username = getattr(dialog.entity, "username", None) or ""
        if needle in title or needle == username.lower():
            matches.append(dialog)
        if len(matches) >= 10:
            break

    if not matches:
        print("❌  No matching dialogs found. Use list_dialogs.py to see all chats.")
        sys.exit(1)

    print("Did you mean one of these?\n")
    for i, d in enumerate(matches, 1):
        eid = d.entity.id
        uname = getattr(d.entity, "username", None) or ""
        print(f"  {i}. {d.title}  (id={eid}  username={uname})")

    print(f"\nRe-run with the exact title, @username, or numeric ID.")
    sys.exit(1)


# ── Entity type helper ──────────────────────────────────

def _group_type(entity) -> str:
    if isinstance(entity, Channel):
        if entity.megagroup:
            return "supergroup"
        if entity.broadcast:
            return "channel"
        return "channel"
    if isinstance(entity, Chat):
        return "group"
    return "unknown"


# ── Sender cache ────────────────────────────────────────

class SenderCache:
    """Cache sender entities to avoid repeated API calls."""

    def __init__(self, client: TelegramClient):
        self._client = client
        self._cache: dict[int, User | None] = {}

    async def get(self, sender_id: int | None) -> User | None:
        if sender_id is None:
            return None
        if sender_id in self._cache:
            return self._cache[sender_id]
        try:
            entity = await self._client.get_entity(sender_id)
            if isinstance(entity, User):
                self._cache[sender_id] = entity
                return entity
        except Exception:
            pass
        self._cache[sender_id] = None
        return None


# ── Message export ──────────────────────────────────────

def _display_name(user: User | None) -> str | None:
    if user is None:
        return None
    parts = [user.first_name or "", user.last_name or ""]
    name = " ".join(p for p in parts if p).strip()
    return name or None


async def collect_messages(
    client: TelegramClient,
    entity,
    limit: int,
    since: datetime | None,
) -> list[dict]:
    """Collect messages in Telegram Desktop export format."""
    sender_cache = SenderCache(client)
    messages = []
    count = 0

    print(f"\n📨 Collecting messages (limit={limit})...")

    async for msg in client.iter_messages(entity, limit=limit, offset_date=since, reverse=False):
        # Skip service messages (joins, leaves, pin, etc.)
        if isinstance(msg, MessageService):
            continue
        if msg.action is not None:
            continue

        sender_id = msg.sender_id
        sender = await sender_cache.get(sender_id)

        text = msg.message or ""
        from_name = _display_name(sender)
        from_id = f"user{sender_id}" if sender_id else None

        msg_obj = {
            "id": msg.id,
            "type": "message",
            "date": msg.date.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "from": from_name,
            "from_id": from_id,
            "text": text,
            "reply_to_message_id": msg.reply_to.reply_to_msg_id if msg.reply_to else None,
        }
        messages.append(msg_obj)
        count += 1

        if count % 500 == 0:
            print(f"   ... {count} messages collected")

    print(f"   ✅ {count} messages collected")
    return messages


# ── Participant export ──────────────────────────────────

def _participant_obj(user: User) -> dict:
    parts = [user.first_name or "", user.last_name or ""]
    display = " ".join(p for p in parts if p).strip() or None

    return {
        "user_id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "display_name": display,
        "bot": bool(user.bot),
        "deleted": bool(user.deleted),
        "scam": getattr(user, "scam", None),
        "fake": getattr(user, "fake", None),
        "verified": getattr(user, "verified", None),
        "premium": getattr(user, "premium", None),
    }


async def collect_participants(
    client: TelegramClient,
    entity,
) -> tuple[list[dict], str, str | None, int | None]:
    """
    Attempt to collect the full participant list.

    Returns:
        (participants, status, error, count)
    """
    print("\n👥 Collecting participants...")
    participants = []
    retries = 0

    try:
        async for user in client.iter_participants(entity):
            if isinstance(user, User):
                participants.append(_participant_obj(user))

                if len(participants) % 200 == 0:
                    print(f"   ... {len(participants)} participants collected")

        count = len(participants)
        print(f"   ✅ {count} participants collected")
        return participants, "ok", None, count

    except FloodWaitError as e:
        while retries < _FLOOD_MAX_RETRIES:
            retries += 1
            wait = e.seconds
            print(f"   ⏳ FloodWait: sleeping {wait}s (retry {retries}/{_FLOOD_MAX_RETRIES})...")
            time.sleep(wait)
            try:
                async for user in client.iter_participants(entity):
                    if isinstance(user, User):
                        participants.append(_participant_obj(user))
                count = len(participants)
                print(f"   ✅ {count} participants collected (after retry)")
                return participants, "ok", None, count
            except FloodWaitError as e2:
                e = e2
                continue

        error_msg = f"FloodWaitError after {_FLOOD_MAX_RETRIES} retries (last wait: {e.seconds}s)"
        print(f"   ⚠️  {error_msg}")
        return [], "unavailable", error_msg, None

    except (ChatAdminRequiredError, ChannelPrivateError) as e:
        error_msg = f"{type(e).__name__}: {e}"
        print(f"   ⚠️  Participant list unavailable: {error_msg}")
        return [], "unavailable", error_msg, None

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        print(f"   ⚠️  Participant list unavailable: {error_msg}")
        return [], "unavailable", error_msg, None


# ── Main ────────────────────────────────────────────────

async def main() -> None:
    args = parse_args()

    if not API_ID or not API_HASH:
        print("❌  TG_API_ID and TG_API_HASH must be set in .env", file=sys.stderr)
        sys.exit(1)

    include_participants = args.include_participants == "true"
    since_dt = None
    if args.since:
        since_dt = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # ── Connect ──────────────────────────────────────
    client = TelegramClient(SESSION_PATH, int(API_ID), API_HASH)
    await client.start()
    print(f"\n✅ Connected as: {(await client.get_me()).first_name}")

    # ── Resolve group ────────────────────────────────
    print(f"\n🔍 Resolving group: {args.group}")
    entity = await resolve_group(client, args.group)
    group_title = getattr(entity, "title", str(entity.id))
    group_type = _group_type(entity)
    print(f"   Found: {group_title} (id={entity.id}, type={group_type})")

    # ── Collect participants ─────────────────────────
    if include_participants:
        participants, p_status, p_error, p_count = await collect_participants(client, entity)
    else:
        participants, p_status, p_error, p_count = [], "unavailable", "Skipped (--include-participants false)", None

    # ── Collect messages ─────────────────────────────
    messages = await collect_messages(client, entity, args.limit, since_dt)

    # ── Build export object ──────────────────────────
    export_obj = {
        "name": group_title,
        "type": group_type,
        "id": entity.id,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "participants_status": p_status,
        "participants_error": p_error,
        "participants_count": p_count,
        "messages_count": len(messages),
        "limits": {
            "since": args.since,
            "limit": args.limit,
        },
        "participants": participants,
        "messages": messages,
    }

    # ── Write output ─────────────────────────────────
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(export_obj, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n{'━' * 50}")
    print(f"✅ Export complete:")
    print(f"   Group:              {group_title}")
    print(f"   Type:               {group_type}")
    print(f"   Messages:           {len(messages)}")
    print(f"   Participants:       {p_status} ({p_count if p_count is not None else 'N/A'})")
    if p_error:
        print(f"   Participant error:  {p_error}")
    print(f"   Output:             {out_path}")
    print()

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
