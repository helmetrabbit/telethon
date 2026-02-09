#!/usr/bin/env python3
"""
Collect messages and participants from a single Telegram group and
write a JSON export compatible with the tg-profile-engine ingest pipeline.

Usage:
    python tools/telethon_collector/collect_group_export.py \
        --group "BD in Web3" \
        --out data/exports/telethon_bd_web3.json

    # Incremental update (re-run same command — fetches only new messages):
    python tools/telethon_collector/collect_group_export.py \
        --group "BD in Web3" \
        --out data/exports/telethon_bd_web3.json

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
        default=None,
        help="Max messages to fetch (default: all messages).",
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
    """Cache sender entities to avoid repeated API calls.

    Pre-seed with the participant list so most messages resolve instantly
    without any API round-trip.
    """

    def __init__(self, client: TelegramClient):
        self._client = client
        self._cache: dict[int, User | None] = {}
        self._misses = 0

    def seed_from_participants(self, participants: list[dict]) -> None:
        """Pre-populate cache from collected participants (dict format)."""
        for p in participants:
            uid = p.get("user_id")
            if uid is not None:
                # Store a lightweight object with the fields we need
                self._cache[uid] = p  # type: ignore
        print(f"   📦 Sender cache pre-seeded with {len(self._cache)} participants")

    def _extract_name(self, entry) -> str | None:
        """Extract display name from either a User object or a participant dict."""
        if entry is None:
            return None
        if isinstance(entry, dict):
            return entry.get("display_name")
        # It's a User object
        parts = [entry.first_name or "", entry.last_name or ""]
        name = " ".join(p for p in parts if p).strip()
        return name or None

    def _extract_username(self, entry) -> str | None:
        if entry is None:
            return None
        if isinstance(entry, dict):
            return entry.get("username")
        return getattr(entry, "username", None)

    async def get(self, sender_id: int | None) -> tuple[str | None, str | None]:
        """Return (display_name, from_id_str) without API calls when possible."""
        if sender_id is None:
            return None, None
        from_id = f"user{sender_id}"

        if sender_id in self._cache:
            entry = self._cache[sender_id]
            return self._extract_name(entry), from_id

        # Cache miss — must resolve via API (uncommon if participants were seeded)
        self._misses += 1
        try:
            entity = await self._client.get_entity(sender_id)
            if isinstance(entity, User):
                self._cache[sender_id] = entity
                return self._extract_name(entity), from_id
        except Exception:
            pass
        self._cache[sender_id] = None
        return None, from_id


# ── Message export ──────────────────────────────────────

def _display_name(user: User | None) -> str | None:
    if user is None:
        return None
    parts = [user.first_name or "", user.last_name or ""]
    name = " ".join(p for p in parts if p).strip()
    return name or None


# ── Incremental + checkpoint support ───────────────────

def _checkpoint_path(out_path: Path) -> Path:
    """Return the path for the in-progress checkpoint file."""
    return out_path.with_suffix(".checkpoint.json")


def _save_checkpoint(path: Path, messages: list[dict], meta: dict) -> None:
    """Save an in-progress checkpoint to disk."""
    data = {**meta, "messages": messages}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _load_checkpoint(out_path: Path) -> tuple[list[dict] | None, int | None, dict | None]:
    """Load checkpoint if it exists. Returns (messages, max_id, meta) or (None, None, None)."""
    cp = _checkpoint_path(out_path)
    if not cp.exists():
        return None, None, None
    try:
        data = json.loads(cp.read_text(encoding="utf-8"))
        msgs = data.get("messages", [])
        max_id = max((m["id"] for m in msgs if "id" in m), default=None)
        meta = {k: v for k, v in data.items() if k != "messages"}
        return msgs, max_id, meta
    except Exception:
        return None, None, None


def _load_existing(out_path: Path) -> tuple[dict | None, int | None]:
    """If an output file exists, load it and return (export_obj, max_message_id)."""
    if not out_path.exists():
        return None, None
    try:
        data = json.loads(out_path.read_text(encoding="utf-8"))
        existing_ids = [m["id"] for m in data.get("messages", []) if "id" in m]
        max_id = max(existing_ids) if existing_ids else None
        return data, max_id
    except Exception:
        return None, None


def _format_eta(seconds: float) -> str:
    """Format seconds into a human-readable ETA string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"


def _serialize_reactions(msg) -> dict | None:
    """Helper to serialize message reactions (Telethon MessageReactions object)."""
    if not hasattr(msg, "reactions") or not msg.reactions:
        return None

    # msg.reactions -> MessageReactions(results=[ReactionCount(...), ...], ...)
    out_results = []
    
    # Use 'results' if present
    results = getattr(msg.reactions, "results", [])
    for r in results:
        count = getattr(r, "count", 0)
        
        # Extract the reaction symbol
        emoji = None
        # r.reaction is usually ReactionEmoji(emoticon='👍') or ReactionCustomEmoji(document_id=123)
        rxn = getattr(r, "reaction", None)
        if rxn:
            if hasattr(rxn, "emoticon"):
                emoji = rxn.emoticon
            elif hasattr(rxn, "document_id"):
                emoji = f"custom_emoji_id:{rxn.document_id}"
            else:
                emoji = str(rxn)
        
        out_results.append({"count": count, "emoji": emoji})

    return {
        "results": out_results
    }


_CHECKPOINT_INTERVAL = 1000  # Save to disk every N messages


async def collect_messages(
    client: TelegramClient,
    entity,
    sender_cache: SenderCache,
    limit: int | None,
    since: datetime | None,
    min_id: int = 0,
    out_path: Path | None = None,
    checkpoint_meta: dict | None = None,
) -> list[dict]:
    """Collect messages in Telegram Desktop export format.

    If min_id > 0, only fetches messages with id > min_id (incremental mode).
    Saves a checkpoint every 1000 messages so interruptions lose minimal work.
    """
    messages = []
    count = 0
    skipped_service = 0
    t0 = time.monotonic()
    last_checkpoint_count = 0

    limit_str = str(limit) if limit else "all"
    mode = "incremental (new only)" if min_id > 0 else "full"
    print(f"\n📨 Collecting messages (limit={limit_str}, mode={mode})...")
    if out_path:
        print(f"   💾 Checkpoints every {_CHECKPOINT_INTERVAL:,} messages → {_checkpoint_path(out_path)}")

    async for msg in client.iter_messages(entity, limit=limit, offset_date=since, reverse=False, min_id=min_id):
        # Skip service messages (joins, leaves, pin, etc.)
        if isinstance(msg, MessageService) or msg.action is not None:
            skipped_service += 1
            continue

        sender_id = msg.sender_id
        from_name, from_id = await sender_cache.get(sender_id)

        text = msg.message or ""

        msg_obj = {
            "id": msg.id,
            "type": "message",
            "date": msg.date.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "from": from_name,
            "from_id": from_id,
            "text": text,
            "reply_to_message_id": msg.reply_to.reply_to_msg_id if msg.reply_to else None,
            "views": getattr(msg, "views", 0) or 0,
            "forwards": getattr(msg, "forwards", 0) or 0,
            "reply_count": getattr(msg.replies, "replies", 0) if hasattr(msg, "replies") and msg.replies else 0,
            "reactions": _serialize_reactions(msg),
        }
        messages.append(msg_obj)
        count += 1

        # ── Progress every 100 messages ──────────
        if count % 100 == 0:
            elapsed = time.monotonic() - t0
            rate = count / elapsed if elapsed > 0 else 0
            if limit:
                remaining = limit - count
                eta = _format_eta(remaining / rate) if rate > 0 else "?"
            else:
                eta = "—"
            print(f"   ... {count:,} messages  ({rate:.0f} msg/s, {elapsed:.0f}s elapsed, "
                  f"ETA {eta}, {sender_cache._misses} misses, {skipped_service} svc skipped)")

        # ── Checkpoint every 1000 messages ───────
        if out_path and count - last_checkpoint_count >= _CHECKPOINT_INTERVAL:
            _save_checkpoint(_checkpoint_path(out_path), messages, checkpoint_meta or {})
            last_checkpoint_count = count
            print(f"   💾 Checkpoint saved: {count:,} messages")

    elapsed = time.monotonic() - t0
    rate = count / elapsed if elapsed > 0 else 0
    print(f"   ✅ {count:,} messages collected in {elapsed:.1f}s ({rate:.0f} msg/s)")
    if skipped_service:
        print(f"   ℹ️  {skipped_service} service messages skipped")
    if sender_cache._misses:
        print(f"   ℹ️  {sender_cache._misses} sender cache misses (API lookups)")

    # Clean up checkpoint file on successful completion
    if out_path:
        cp = _checkpoint_path(out_path)
        if cp.exists():
            cp.unlink()
            print(f"   🧹 Checkpoint file removed (collection complete)")

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
        "lang_code": getattr(user, "lang_code", None),
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

    out_path = Path(args.out)

    # ── Check for checkpoint from interrupted run ────
    cp_messages, cp_max_id, cp_meta = _load_checkpoint(out_path)
    if cp_messages and cp_max_id:
        print(f"\n🔄 Resuming from checkpoint: {len(cp_messages):,} messages (max id={cp_max_id})")
        print(f"   Will continue fetching from where we left off")

    # ── Check for existing export (incremental mode) ─
    existing_export, existing_max_id = _load_existing(out_path)
    if existing_export and existing_max_id:
        existing_count = len(existing_export.get("messages", []))
        print(f"\n♻️  Existing export found: {existing_count:,} messages (max id={existing_max_id})")
        print(f"   Will fetch only messages newer than id {existing_max_id}")

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

    # ── Pre-seed sender cache from participants ──────
    sender_cache = SenderCache(client)
    if participants:
        sender_cache.seed_from_participants(participants)

    # ── Determine starting point ─────────────────────
    # Priority: checkpoint > existing export > fresh start
    if cp_messages and cp_max_id:
        min_id = cp_max_id
    elif existing_max_id:
        min_id = existing_max_id
    else:
        min_id = 0

    # Metadata for checkpoint files
    checkpoint_meta = {
        "name": group_title,
        "type": group_type,
        "id": entity.id,
        "checkpoint": True,
    }

    # ── Collect messages ─────────────────────────────
    new_messages = await collect_messages(
        client, entity, sender_cache, args.limit, since_dt,
        min_id=min_id, out_path=out_path, checkpoint_meta=checkpoint_meta,
    )

    # ── Merge all sources ────────────────────────────
    all_prior: list[dict] = []
    if existing_export:
        all_prior.extend(existing_export.get("messages", []))
    if cp_messages:
        all_prior.extend(cp_messages)

    if all_prior:
        # Deduplicate by message id, keeping the latest version
        seen_ids: dict[int, dict] = {}
        for m in all_prior:
            seen_ids[m["id"]] = m
        for m in new_messages:
            seen_ids[m["id"]] = m
        messages = list(seen_ids.values())
        prior_count = len(all_prior)
        new_count = len(new_messages)
        print(f"\n🔀 Merged: {prior_count:,} prior + {new_count:,} new = {len(messages):,} total (deduplicated)")
    elif not new_messages:
        messages = []
        print(f"\n⚠️  No messages collected.")
    else:
        messages = new_messages

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
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(export_obj, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n{'━' * 50}")
    print(f"✅ Export complete:")
    print(f"   Group:              {group_title}")
    print(f"   Type:               {group_type}")
    print(f"   Messages:           {len(messages):,}")
    print(f"   Participants:       {p_status} ({p_count if p_count is not None else 'N/A'})")
    if p_error:
        print(f"   Participant error:  {p_error}")
    print(f"   Output:             {out_path}")
    if existing_max_id:
        print(f"   Mode:               incremental (re-run to fetch newer messages)")
    else:
        print(f"   Mode:               full collection")
    print()

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
