#!/usr/bin/env python3
"""
Listen only to Telegram private DMs and persist each new message to JSONL.

This intentionally excludes group chats/supergroups/channels.

Usage:
    source tools/telethon_collector/.venv/bin/activate
    python tools/telethon_collector/listen-dms.py \
      --out data/exports/telethon_dms_live.jsonl

Output schema (one JSON object per line):
  {
    "message_id": 123,
    "chat_id": 456,
    "chat_type": "private",
    "direction": "inbound|outbound",
    "sender_id": "user789",
    "sender_name": "Alice",
    "peer_id": "user101",
    "peer_name": "Bob",
    "text": "...",
    "date": "2026-02-16T08:00:00+00:00",
    "reply_to_message_id": 120,
    "views": 0,
    "forwards": 0,
    "has_links": false,
    "has_mentions": true,
    "text_len": 42
  }
"""

import argparse
import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import PeerUser

# ── Load .env from collector directory ─────────────────
_SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(_SCRIPT_DIR / ".env")

API_ID = os.getenv("TG_API_ID")
API_HASH = os.getenv("TG_API_HASH")
SESSION_PATH = os.getenv("TG_SESSION_PATH", str(_SCRIPT_DIR / "telethon.session"))

LINK_RE = re.compile(r"https?://\S+")
MENTION_RE = re.compile(r"@[A-Za-z0-9_]+")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Listen for private Telegram DMs only.")
    p.add_argument(
        "--out",
        default="data/exports/telethon_dms_live.jsonl",
        help="JSONL output path (default: data/exports/telethon_dms_live.jsonl)",
    )
    p.add_argument(
        "--skip-outgoing",
        action="store_true",
        help="If set, skip outbound DM messages from the logged-in account.",
    )
    return p.parse_args()


def looks_like_message(msg) -> bool:
    # Avoid service-only or empty placeholder updates
    return bool(msg.message or "")


def _display_name(user) -> str | None:
    if user is None:
        return None
    parts = [user.first_name or "", user.last_name or ""]
    display = " ".join(p for p in parts if p).strip()
    return display or user.username


def serialize_message(msg, sender, peer):
    sender_name = _display_name(sender)
    peer_name = _display_name(peer)

    return {
        "message_id": msg.id,
        "chat_id": msg.chat_id,
        "chat_type": "private",
        "direction": "outbound" if msg.out else "inbound",
        "sender_id": f"user{sender.id}" if sender else None,
        "sender_name": sender_name,
        "sender_username": sender.username if sender else None,
        "sender_is_bot": bool(getattr(sender, "bot", False)),
        "peer_id": f"user{peer.id}" if peer else None,
        "peer_name": peer_name,
        "peer_username": peer.username if peer else None,
        "text": msg.message,
        "text_len": len(msg.message or ""),
        "date": msg.date.astimezone(timezone.utc).isoformat(),
        "reply_to_message_id": msg.reply_to.reply_to_msg_id if getattr(msg, "reply_to", None) else None,
        "views": int(getattr(msg, "views", 0) or 0),
        "forwards": int(getattr(msg, "forwards", 0) or 0),
        "has_links": bool(LINK_RE.search(msg.message or "") is not None),
        "has_mentions": bool(MENTION_RE.search(msg.message or "") is not None),
        "raw_peer_type": type(getattr(msg, "to_id", None)).__name__ if getattr(msg, "to_id", None) else None,
    }


async def main() -> None:
    args = parse_args()

    if not API_ID or not API_HASH:
        raise SystemExit("TG_API_ID and TG_API_HASH must be set in tools/telethon_collector/.env")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    client = TelegramClient(SESSION_PATH, int(API_ID), API_HASH)

    async def on_startup(_: TelegramClient):
        me = await client.get_me()
        print(f"✅ DM listener connected as {me.first_name} ({me.id})")
        print("ℹ️  Filtering to private chats only (no groups/channels).")
        print(f"📝 Writing raw DM events to: {out_path}")
        print("Press Ctrl+C to stop.")

    @client.on(events.NewMessage)
    async def handler(event):
        msg = event.message
        if not looks_like_message(msg):
            return

        # Keep only private DMs
        if not isinstance(getattr(msg, "to_id", None), PeerUser):
            return

        if args.skip_outgoing and msg.out:
            return

        sender = await event.get_sender()
        if sender is None:
            return

        peer = None
        try:
            peer = await event.get_chat()
        except Exception:
            peer = None

        row = serialize_message(msg, sender, peer)
        row["captured_at"] = datetime.now(timezone.utc).isoformat()

        # Persist one JSON object per line
        with out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

        direction = row["direction"]
        sender_label = row["sender_name"] or row["sender_username"] or row["sender_id"] or "unknown"
        peer_label = row["peer_name"] or row["peer_username"] or row["peer_id"] or "unknown"
        print(f"{datetime.now(timezone.utc).isoformat()}  [{direction}] {sender_label} -> {peer_label}: {str(msg.message or '')[:100]}")

    await client.start()
    await on_startup(client)
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SessionPasswordNeededError:
        raise SystemExit("2FA is enabled; please log in once interactively to create the session file.")
    except KeyboardInterrupt:
        print("\n🛑 DM listener stopped by user.")
