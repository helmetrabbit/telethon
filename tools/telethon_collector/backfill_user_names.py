#!/usr/bin/env python3
"""
Backfill missing Telegram display names from Telethon.

Primary target: users without handle/username whose display_name is missing or placeholder.

Resolution order:
1) Session SQLite entity cache (fast, no network)
2) Live Telethon lookup (if API/session available)

Usage:
  tools/telethon_collector/.venv/bin/python tools/telethon_collector/backfill_user_names.py
  tools/telethon_collector/.venv/bin/python tools/telethon_collector/backfill_user_names.py --limit 1000 --only-no-handle true
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from dotenv import load_dotenv
import psycopg
from telethon import TelegramClient
from telethon.tl.types import User

_SCRIPT_DIR = Path(__file__).resolve().parent
_ROOT_DIR = _SCRIPT_DIR.parent.parent

# Load project .env first, then collector-specific .env overrides
load_dotenv(_ROOT_DIR / ".env")
load_dotenv(_SCRIPT_DIR / ".env")

PLACEHOLDERS = {"unknown", "deleted account"}
EXTERNAL_ID_RE = re.compile(r"^user(\d+)$")


@dataclass
class Candidate:
    user_id: int
    external_id: str
    telegram_user_id: int
    handle: Optional[str]
    display_name: Optional[str]


@dataclass
class SessionEntity:
    name: Optional[str]
    username: Optional[str]


def parse_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def normalize_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    cleaned = " ".join(str(name).strip().split())
    if not cleaned:
        return None
    if cleaned.lower() in PLACEHOLDERS:
        return None
    return cleaned


def normalize_handle(handle: Optional[str]) -> Optional[str]:
    if not handle:
        return None
    h = handle.strip().lstrip("@")
    return h or None


def resolve_session_path(raw: Optional[str]) -> Optional[Path]:
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = (_ROOT_DIR / path).resolve()
    return path


def load_session_entities(session_path: Optional[Path]) -> Dict[int, SessionEntity]:
    cache: Dict[int, SessionEntity] = {}
    if not session_path or not session_path.exists():
        return cache

    conn = sqlite3.connect(str(session_path))
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, username
            FROM entities
            WHERE id > 0
            """
        )
        for ent_id, name, username in cur.fetchall():
            if not isinstance(ent_id, int):
                continue
            cache[ent_id] = SessionEntity(
                name=normalize_name(name),
                username=normalize_handle(username),
            )
    finally:
        conn.close()
    return cache


def parse_telegram_user_id(external_id: str) -> Optional[int]:
    m = EXTERNAL_ID_RE.match(external_id)
    if not m:
        return None
    return int(m.group(1))


def display_name_from_user(entity: User) -> Optional[str]:
    parts = [entity.first_name or "", entity.last_name or ""]
    full = " ".join(p for p in parts if p).strip()
    return normalize_name(full)


def bool_arg(value: str) -> bool:
    return parse_bool(value)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill missing display_name via Telethon lookup")
    p.add_argument("--limit", type=int, default=5000, help="Max users to process (default: 5000)")
    p.add_argument("--only-no-handle", type=bool_arg, default=True, help="Only process users missing handle (default: true)")
    p.add_argument("--live-lookup", type=bool_arg, default=False, help="Use live Telethon API lookups for cache misses (default: false)")
    p.add_argument("--dry-run", type=bool_arg, default=False, help="Do not write DB updates (default: false)")
    p.add_argument("--strict", type=bool_arg, default=False, help="Exit non-zero if Telethon creds/session missing (default: false)")
    return p.parse_args()


def fetch_candidates(conn: psycopg.Connection, limit: int, only_no_handle: bool) -> list[Candidate]:
    no_handle_clause = "AND (handle IS NULL OR btrim(handle) = '')" if only_no_handle else ""
    query = f"""
      SELECT id, external_id, handle, display_name
      FROM users
      WHERE platform = 'telegram'
        AND (display_name IS NULL OR btrim(display_name) = '' OR lower(btrim(display_name)) IN ('unknown','deleted account'))
        AND external_id ~ '^user[0-9]+$'
        {no_handle_clause}
      ORDER BY COALESCE(last_msg_at, created_at) DESC NULLS LAST, id DESC
      LIMIT %s
    """

    candidates: list[Candidate] = []
    with conn.cursor() as cur:
        cur.execute(query, (limit,))
        for row in cur.fetchall():
            user_id, external_id, handle, display_name = row
            tid = parse_telegram_user_id(external_id)
            if tid is None:
                continue
            candidates.append(
                Candidate(
                    user_id=int(user_id),
                    external_id=str(external_id),
                    telegram_user_id=tid,
                    handle=normalize_handle(handle),
                    display_name=display_name,
                )
            )
    return candidates


def update_user(conn: psycopg.Connection, user_id: int, name: Optional[str], handle: Optional[str], dry_run: bool) -> tuple[bool, bool]:
    name_written = False
    handle_written = False

    if dry_run:
        return bool(name), bool(handle)

    with conn.cursor() as cur:
        if name:
            cur.execute(
                """
                UPDATE users
                SET display_name = %s,
                    display_name_source = 'telethon_lookup',
                    display_name_updated_at = now()
                WHERE id = %s
                  AND (display_name IS NULL OR btrim(display_name) = '' OR lower(btrim(display_name)) IN ('unknown','deleted account'))
                """,
                (name, user_id),
            )
            name_written = cur.rowcount > 0

        if handle:
            cur.execute(
                """
                UPDATE users
                SET handle = %s
                WHERE id = %s
                  AND (handle IS NULL OR btrim(handle) = '')
                """,
                (handle, user_id),
            )
            handle_written = cur.rowcount > 0

    return name_written, handle_written


async def resolve_live(client: TelegramClient, telegram_user_id: int) -> tuple[Optional[str], Optional[str]]:
    try:
        entity = await client.get_entity(telegram_user_id)
    except Exception:
        return None, None

    if not isinstance(entity, User):
        return None, None

    return display_name_from_user(entity), normalize_handle(getattr(entity, "username", None))


async def run() -> int:
    args = parse_args()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("❌ DATABASE_URL missing.", file=sys.stderr)
        return 1

    session_path = resolve_session_path(os.getenv("TG_SESSION_PATH", str(_SCRIPT_DIR / "telethon.session")))
    session_cache = load_session_entities(session_path)

    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")
    requested_live_lookup = bool(args.live_lookup)
    have_live_lookup = bool(requested_live_lookup and api_id and api_hash and session_path and session_path.exists())

    if not have_live_lookup and not session_cache:
        msg = "⚠️ Telethon session/cache not available; skipping name backfill."
        if args.strict:
            print(msg, file=sys.stderr)
            return 1
        print(msg)
        return 0

    conn = psycopg.connect(database_url)
    conn.autocommit = False

    try:
        candidates = fetch_candidates(conn, args.limit, args.only_no_handle)
        if not candidates:
            print("✅ No candidate users found for Telethon name backfill.")
            return 0

        print(f"🔎 Candidates: {len(candidates)} (limit={args.limit}, only_no_handle={args.only_no_handle})")
        print(f"📦 Session cache entities: {len(session_cache)}")

        client: Optional[TelegramClient] = None
        if have_live_lookup:
            client = TelegramClient(str(session_path), int(api_id), api_hash)
            await client.connect()
            if not await client.is_user_authorized():
                print("⚠️ Live Telethon lookup disabled: session is not authorized (run tg:list-dialogs to re-auth).")
                await client.disconnect()
                client = None
                have_live_lookup = False
        elif requested_live_lookup:
            print("⚠️ Live Telethon lookup disabled: missing TG_API_ID/TG_API_HASH/session.")

        print(f"🌐 Live Telethon lookup: {'enabled' if have_live_lookup else 'disabled'}")

        looked_up = 0
        from_cache = 0
        from_live = 0
        updated_names = 0
        updated_handles = 0
        unresolved = 0

        for idx, c in enumerate(candidates, start=1):
            looked_up += 1
            resolved_name: Optional[str] = None
            resolved_handle: Optional[str] = None

            cached = session_cache.get(c.telegram_user_id)
            if cached:
                resolved_name = normalize_name(cached.name)
                resolved_handle = normalize_handle(cached.username)
                if resolved_name or resolved_handle:
                    from_cache += 1

            if (not resolved_name and client is not None):
                try:
                    live_name, live_handle = await asyncio.wait_for(
                        resolve_live(client, c.telegram_user_id),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    live_name, live_handle = None, None
                if live_name or live_handle:
                    from_live += 1
                if live_name:
                    resolved_name = live_name
                if live_handle:
                    resolved_handle = live_handle

            if not resolved_name and not resolved_handle:
                unresolved += 1
                continue

            wrote_name, wrote_handle = update_user(
                conn,
                user_id=c.user_id,
                name=resolved_name,
                handle=resolved_handle,
                dry_run=args.dry_run,
            )
            if wrote_name:
                updated_names += 1
            if wrote_handle:
                updated_handles += 1

            if idx % 50 == 0 or idx == len(candidates):
                print(
                    f"   progress {idx}/{len(candidates)} | names+{updated_names} handles+{updated_handles} unresolved={unresolved}"
                )

        if args.dry_run:
            conn.rollback()
            print("🧪 Dry-run complete (no DB writes).")
        else:
            conn.commit()

        if client is not None:
            await client.disconnect()

        print("\n✅ Telethon name backfill complete:")
        print(f"   looked_up:      {looked_up}")
        print(f"   from_cache:     {from_cache}")
        print(f"   from_live:      {from_live}")
        print(f"   names_updated:  {updated_names}")
        print(f"   handles_updated:{updated_handles}")
        print(f"   unresolved:     {unresolved}")
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"❌ backfill_user_names failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
