#!/usr/bin/env python3
"""
Fallback catch-up pass for private DMs.

Fetches recent private messages from all private dialogs and appends any messages
newer than the last-seen id per peer into the same JSONL schema used by
`listen-dms.py`.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import User

_SCRIPT_DIR = Path(__file__).resolve().parent

load_dotenv(_SCRIPT_DIR / ".env")

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Catch up private Telegram DMs into JSONL.")
    p.add_argument("--out", default="data/exports/telethon_dms_live.jsonl")
    p.add_argument("--state-file", default="data/.state/dm-live-catchup.state.json")
    p.add_argument("--session-path", default=None)
    p.add_argument("--limit", type=int, default=30)
    return p.parse_args()


def resolve_session_path(raw: str | None) -> str:
    if not raw:
        raw = str(_SCRIPT_DIR / "telethon_openclaw.session")

    path = Path(raw)
    if path.is_absolute():
        return str(path)

    root_candidate = Path(__file__).resolve().parents[2] / path
    local_candidate = _SCRIPT_DIR / path
    if root_candidate.exists():
        return str(root_candidate)
    return str(local_candidate)


def display_name(entity: User | None) -> str | None:
    if not entity:
        return None
    parts = [entity.first_name or "", entity.last_name or ""]
    return (" ".join(p for p in parts if p) or entity.username)


def looks_like_message(msg) -> bool:
    return bool(msg.message or "")


def serialize_message(msg, *, me: User, peer: User, account_id: str, direction: str) -> dict[str, Any]:
    peer_id = f"user{peer.id}"

    if direction == "outbound":
        sender = me
        sender_id = account_id
        peer_name = display_name(peer)
        sender_name = display_name(me)
        sender_username = me.username
        peer_username = peer.username
    else:
        sender = peer
        sender_id = f"user{peer.id}"
        peer_name = display_name(me)
        sender_name = display_name(peer)
        sender_username = peer.username
        peer_username = me.username

    return {
        "message_id": msg.id,
        "chat_id": msg.chat_id,
        "account_id": account_id,
        "chat_type": "private",
        "direction": direction,
        "sender_id": sender_id,
        "sender_name": sender_name,
        "sender_username": sender_username,
        "sender_is_bot": bool(getattr(sender, "bot", False)),
        "peer_id": peer_id,
        "peer_name": peer_name,
        "peer_username": peer_username,
        "text": msg.message,
        "text_len": len(msg.message or ""),
        "date": msg.date.astimezone(timezone.utc).isoformat(),
        "reply_to_message_id": msg.reply_to.reply_to_msg_id if getattr(msg, "reply_to", None) else None,
        "views": int(getattr(msg, "views", 0) or 0),
        "forwards": int(getattr(msg, "forwards", 0) or 0),
        "has_links": False,
        "has_mentions": False,
        "raw_peer_type": "User",
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "last_seen": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "last_seen": {}}

    if not isinstance(data, dict):
        return {"version": 1, "last_seen": {}}

    if "last_seen" not in data or not isinstance(data["last_seen"], dict):
        data["last_seen"] = {}

    return data


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def append_line(out_path: Path, row: dict[str, Any]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


async def main() -> None:
    args = parse_args()
    api_id = int(__import__("os").getenv("TG_API_ID", "0"))
    api_hash = __import__("os").getenv("TG_API_HASH", "")
    session_path = resolve_session_path(args.session_path)

    if not api_id or not api_hash:
        raise SystemExit("TG_API_ID/TG_API_HASH must be set")

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = Path(__file__).resolve().parents[1] / out_path
    state_path = Path(args.state_file)
    if not state_path.is_absolute():
        state_path = Path(__file__).resolve().parents[1] / state_path

    state = load_state(state_path)
    last_seen = state.setdefault("last_seen", {})

    client = TelegramClient(session_path, api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        raise SystemExit("Telegram session is not authorized")

    me = await client.get_me()
    if not me:
        raise SystemExit("Could not resolve account user")

    account_id = f"user{me.id}"

    dialogs = await client.get_dialogs(limit=120)
    for d in dialogs:
        peer = d.entity
        if not isinstance(peer, User):
            continue
        if peer.bot:
            continue
        if peer.id == me.id:
            continue

        key = str(peer.id)
        seen = int(last_seen.get(key, 0) or 0)
        msgs = await client.get_messages(peer, limit=max(1, args.limit))
        max_seen = seen
        for m in reversed(msgs):
            if not looks_like_message(m):
                continue

            if int(m.id) <= seen:
                continue

            direction = "outbound" if m.out else "inbound"
            row = serialize_message(m, me=me, peer=peer, account_id=account_id, direction=direction)
            append_line(out_path, row)

            if int(m.id) > max_seen:
                max_seen = int(m.id)

        if max_seen != seen:
            last_seen[key] = max_seen

    await client.disconnect()
    save_state(state_path, state)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
