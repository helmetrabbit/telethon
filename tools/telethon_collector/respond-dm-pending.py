#!/usr/bin/env python3
"""
Process unanswered inbound DM messages and send lightweight responses.

This keeps a small state machine in dm_messages:
- pending   -> queued for response
- sending   -> in-flight
- responded -> answered by outbound message
- failed    -> send attempt failed, can be retried
- not_applicable -> outbound/user-agent ignored messages
"""

import argparse
import asyncio
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from psycopg import connect, OperationalError
from psycopg.rows import dict_row
from telethon import TelegramClient

_SCRIPT_DIR = Path(__file__).resolve().parent
_ROOT_DIR = _SCRIPT_DIR.parent.parent
load_dotenv(_ROOT_DIR / '.env')
load_dotenv(_SCRIPT_DIR / '.env')

DATABASE_URL = os.getenv('DATABASE_URL') or os.getenv('PG_DSN')
API_ID = os.getenv('TG_API_ID')
API_HASH = os.getenv('TG_API_HASH')
_default_session = os.getenv('TG_SESSION_PATH', str(_SCRIPT_DIR / 'telethon.session'))

_PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Resolve unanswered inbound DM messages.')
    p.add_argument('--limit', type=int, default=20, help='Maximum pending messages to process (default: 20)')
    p.add_argument('--max-retries', type=int, default=3, help='Maximum delivery retries (default: 3)')
    p.add_argument('--session-path', default=_default_session, help='Telethon session path to use')
    p.add_argument(
        '--template',
        default=(
            'Got your message: "{excerpt}". Thanks for reaching out — I\'ll review and reply with full context shortly.'
        ),
        help='Response template. Supports {sender_name}, {sender_handle}, {text}, {excerpt}, {now_utc}',
    )
    p.add_argument('--dry-run', action='store_true', help='Process without sending messages')
    p.add_argument('--skip-answered-check', action='store_true', help='Skip reconciliation against existing outbound responses')
    return p.parse_args()


def parse_external_id(raw: str) -> Optional[int]:
    if not raw:
        return None
    if raw.startswith('user'):
        raw = raw[4:]
    if raw.isdigit():
        return int(raw)
    return None


def render_template(template: str, row: Dict[str, Any]) -> str:
    sender_name = row['display_name'] or row['sender_handle'] or 'friend'
    sender_handle = f"@{row['sender_handle']}" if row['sender_handle'] else 'there'
    text = row['text'] or ''
    excerpt = text[:120] + ('…' if len(text) > 120 else '')

    return _PLACEHOLDER_RE.sub(
        lambda m: {
            'sender_name': sender_name,
            'sender_handle': sender_handle,
            'text': text,
            'excerpt': excerpt,
            'now_utc': datetime.now(timezone.utc).isoformat(),
        }.get(m.group(1), m.group(0)),
        template,
    )


def mark_auto_responded(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dm_messages
            SET response_status = 'responded',
                responded_at = GREATEST(dm_messages.responded_at, o.sent_at)
            FROM dm_messages o
            WHERE dm_messages.direction = 'inbound'
              AND dm_messages.response_status IN ('pending', 'failed', 'sending')
              AND o.conversation_id = dm_messages.conversation_id
              AND o.direction = 'outbound'
              AND o.sent_at >= dm_messages.sent_at
            """,
        )
        return cur.rowcount or 0


def recover_stale_sending(conn, stale_minutes: int = 10) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dm_messages
            SET response_status = 'failed',
                response_last_error = 'recovered from stale sending state',
                response_attempted_at = now()
            WHERE response_status = 'sending'
              AND response_attempted_at < now() - (%s * interval '1 minute')
            """,
            [stale_minutes],
        )
        return cur.rowcount or 0


def claim_pending(conn, limit: int, max_retries: int) -> List[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            WITH pending AS (
              SELECT id
              FROM dm_messages
              WHERE direction = 'inbound'
                AND response_status IN ('pending', 'failed')
                AND response_attempts < %s
              ORDER BY sent_at ASC
              LIMIT %s
              FOR UPDATE SKIP LOCKED
            ),
            claimed AS (
              UPDATE dm_messages
              SET response_status = 'sending',
                  response_attempted_at = now(),
                  response_attempts = response_attempts + 1,
                  response_last_error = NULL
              WHERE id IN (SELECT id FROM pending)
              RETURNING id, conversation_id, external_message_id, text, sender_id, response_attempts, response_status
            )
            SELECT
              c.id,
              c.conversation_id,
              c.external_message_id,
              c.text,
              c.response_attempts,
              c.response_status,
              u.external_id AS sender_external_id,
              u.handle AS sender_handle,
              u.display_name
            FROM claimed c
            JOIN users u ON u.id = (SELECT sender_id FROM dm_messages WHERE id = c.id)
            """,
            [max_retries, limit],
        )
        rows = list(cur.fetchall())

    if not rows:
        conn.commit()
        return []

    return rows
def mark_responded(conn, msg_id: int, outgoing_external_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dm_messages
            SET response_status = 'responded',
                response_message_external_id = %s,
                responded_at = now(),
                response_last_error = NULL
            WHERE id = %s
            """,
            [outgoing_external_id, msg_id],
        )


def mark_failed(conn, msg_id: int, reason: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dm_messages
            SET response_status = 'failed',
                response_last_error = %s,
                response_attempted_at = now()
            WHERE id = %s
            """,
            [reason[:2048], msg_id],
        )


async def main() -> None:
    args = parse_args()

    if not DATABASE_URL:
        raise SystemExit('DATABASE_URL or PG_DSN must be set.')
    if not API_ID or not API_HASH:
        raise SystemExit('TG_API_ID and TG_API_HASH must be set in tools/telethon_collector/.env')

    session_path = Path(args.session_path)
    session_path.parent.mkdir(parents=True, exist_ok=True)

    conn = connect(DATABASE_URL)
    try:
        auto_responded = 0 if args.skip_answered_check else mark_auto_responded(conn)
        stale_recovered = 0 if args.skip_answered_check else recover_stale_sending(conn, stale_minutes=10)
        pending = claim_pending(conn, args.limit, args.max_retries)
    except Exception:
        conn.close()
        raise

    if not pending:
        conn.close()
        print(f"No pending DM responses to send. (auto-responded={auto_responded})")
        return

    if args.dry_run:
        client = None
    else:
        client = TelegramClient(str(session_path), int(API_ID), API_HASH)
        await client.start()

    sent = 0
    failed = 0
    skipped = 0
    try:
        for row in pending:
            try:
                peer_id = parse_external_id(row['sender_external_id'])
                if not peer_id:
                    raise ValueError('unparseable recipient id')

                # If this inbound message was answered by someone else since we claimed it, skip.
                # quick re-check to avoid duplicate outbound response.
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM dm_messages o
                        WHERE o.conversation_id = %s
                          AND o.direction = 'outbound'
                          AND o.sent_at >= (SELECT sent_at FROM dm_messages WHERE id = %s)
                        LIMIT 1
                        """,
                        [row['conversation_id'], row['id']],
                    )
                    if cur.fetchone():
                        conn.commit()
                        skipped += 1
                        mark_failed(conn, row['id'], 'already_responded_externally')
                        continue

                text = render_template(args.template, {
                    'sender_handle': row['sender_handle'],
                    'display_name': row['display_name'],
                    'text': row['text'] or '',
                })

                if args.dry_run:
                    print(f"DRY-RUN would reply to {row['sender_external_id']} with: {text[:160]}")
                    mark_responded(conn, row['id'], 'dry-run')
                    conn.commit()
                    sent += 1
                    continue

                sentMsg = await client.send_message(peer_id, text)
                mark_responded(conn, row['id'], str(sentMsg.id))
                sent += 1
                conn.commit()
            except Exception as exc:
                failed += 1
                mark_failed(conn, row['id'], str(exc))
                conn.commit()
                print(f"⚠️  failed to respond to inbound dm id={row['id']}: {exc}")
    finally:
        if client is not None:
            await client.disconnect()

    conn.close()

    print(f"dm responder: responded={sent}, skipped={skipped}, failed={failed}, auto-responded={auto_responded}, recovered={stale_recovered}")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except OperationalError as exc:
        raise SystemExit(f'Database connection failed: {exc}')
    except Exception as exc:
        raise SystemExit(f'Response worker failed: {exc}')
