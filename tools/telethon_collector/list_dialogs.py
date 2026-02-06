#!/usr/bin/env python3
"""
List all Telegram dialogs (chats, groups, channels) visible to the
authenticated user. Use this to identify the exact title / ID / username
of the group you want to collect.

Usage:
    python tools/telethon_collector/list_dialogs.py
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, User

# ── Load .env from the collector directory ──────────────
_SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(_SCRIPT_DIR / ".env")

API_ID = os.getenv("TG_API_ID")
API_HASH = os.getenv("TG_API_HASH")
SESSION_PATH = os.getenv("TG_SESSION_PATH", str(_SCRIPT_DIR / "telethon.session"))


def _entity_type(entity) -> str:
    if isinstance(entity, User):
        return "user"
    if isinstance(entity, Channel):
        if entity.megagroup:
            return "supergroup"
        if entity.broadcast:
            return "channel"
        return "channel"
    if isinstance(entity, Chat):
        return "group"
    return "unknown"


async def main() -> None:
    if not API_ID or not API_HASH:
        print("❌  TG_API_ID and TG_API_HASH must be set in .env", file=sys.stderr)
        sys.exit(1)

    client = TelegramClient(SESSION_PATH, int(API_ID), API_HASH)
    await client.start()

    print(f"\n{'#':<4} {'Type':<12} {'ID':<16} {'Username':<24} Title")
    print("─" * 90)

    idx = 0
    async for dialog in client.iter_dialogs():
        idx += 1
        entity = dialog.entity
        dtype = _entity_type(entity)
        username = getattr(entity, "username", None) or ""
        title = dialog.title or ""
        eid = entity.id
        print(f"{idx:<4} {dtype:<12} {eid:<16} {('@' + username) if username else '':<24} {title}")

    print(f"\n✅  Total dialogs: {idx}\n")
    await client.disconnect()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
