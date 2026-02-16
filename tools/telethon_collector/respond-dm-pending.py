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
from typing import Any, Dict, List, Optional, Set

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
        '--mode',
        choices=['template', 'conversational'],
        default=os.getenv('DM_RESPONSE_MODE', 'conversational'),
        help='Reply generation mode (default: conversational)',
    )
    p.add_argument(
        '--persona-name',
        default=os.getenv('DM_PERSONA_NAME', 'Lobster Llama'),
        help='Visible persona name used in conversational responses',
    )
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


def _clean_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def infer_slots_from_text(text: Optional[str]) -> Set[str]:
    source = (text or '').lower()
    found: Set[str] = set()
    if not source:
        return found

    role_markers = (
        "i'm a ",
        "i am a ",
        "i'm an ",
        "i am an ",
        "my role is ",
        "i work as ",
        "my title is ",
    )
    company_markers = (
        "work at ",
        "working at ",
        "joined ",
        "company is ",
    )
    contact_markers = (
        "prefer",
        "best way to reach me",
        "contact me",
        "dm me",
        "telegram",
        "email",
        "text me",
        "call me",
    )
    priority_markers = (
        "priority",
        "priorities",
        "focused on",
        "focus is",
        "right now i'm focused",
    )

    if any(marker in source for marker in role_markers):
        found.add('primary_role')
    if any(marker in source for marker in company_markers):
        found.add('primary_company')
    if any(marker in source for marker in contact_markers):
        found.add('preferred_contact_style')
    if any(marker in source for marker in priority_markers):
        found.add('notable_topics')
    return found


def fetch_latest_profile(conn, sender_db_id: Optional[int]) -> Dict[str, Any]:
    if not sender_db_id:
        return {
            'primary_role': None,
            'primary_company': None,
            'preferred_contact_style': None,
            'notable_topics': [],
        }

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT primary_role, primary_company, preferred_contact_style, notable_topics
            FROM user_psychographics
            WHERE user_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            [sender_db_id],
        )
        row = cur.fetchone()

    if not row:
        return {
            'primary_role': None,
            'primary_company': None,
            'preferred_contact_style': None,
            'notable_topics': [],
        }

    topics_raw = row.get('notable_topics')
    topics: List[str] = []
    if isinstance(topics_raw, list):
        for item in topics_raw:
            if isinstance(item, str):
                clean = _clean_text(item)
                if clean:
                    topics.append(clean)

    return {
        'primary_role': _clean_text(row.get('primary_role')) or None,
        'primary_company': _clean_text(row.get('primary_company')) or None,
        'preferred_contact_style': _clean_text(row.get('preferred_contact_style')) or None,
        'notable_topics': topics,
    }


def _pick(options: List[str], seed: int) -> str:
    if not options:
        return ''
    return options[seed % len(options)]


def render_conversational_reply(row: Dict[str, Any], profile: Dict[str, Any], persona_name: str) -> str:
    msg_id = int(row.get('id') or 0)
    observed_slots = infer_slots_from_text(row.get('text'))

    ack_options = [
        "Love the context, thanks for sharing that.",
        "Super helpful, thanks for the update.",
        "Nice, that gives me a much clearer signal.",
    ]
    ack_line = _pick(ack_options, msg_id)

    missing_order = ['primary_role', 'primary_company', 'notable_topics', 'preferred_contact_style']
    missing = []
    for slot in missing_order:
        value = profile.get(slot)
        if slot == 'notable_topics':
            has_value = isinstance(value, list) and len(value) > 0
        else:
            has_value = bool(value)
        if not has_value and slot not in observed_slots:
            missing.append(slot)

    role_questions = [
        "What title best matches what you do day to day right now?",
        "Quick one: what role should I pin you as right now?",
    ]
    company_questions = [
        "What company or project are you currently spending most of your time on?",
        "Which company/project should I map you to at the moment?",
    ]
    priority_questions = [
        "What are your top 2 priorities this month?",
        "What are the main things you want to push forward right now?",
    ]
    contact_questions = [
        "What communication style do you prefer from me: short bullets, detailed notes, or quick back-and-forth?",
        "How do you want me to communicate with you: concise, detailed, or somewhere in between?",
    ]

    if missing:
        slot = missing[0]
        question_map = {
            'primary_role': role_questions,
            'primary_company': company_questions,
            'notable_topics': priority_questions,
            'preferred_contact_style': contact_questions,
        }
        next_question = _pick(question_map[slot], msg_id + 1)
    else:
        next_question = _pick(
            [
                "If anything changed in your role, company, or priorities, send it and I’ll keep your profile fresh.",
                "If you have a new update, drop it here and I’ll keep your profile in sync.",
            ],
            msg_id + 2,
        )

    if row.get('response_attempts') == 1:
        intro = f"I'm {persona_name}."
        return f"{ack_line} {intro} {next_question}"
    return f"{ack_line} {next_question}"


def render_response(args: argparse.Namespace, conn, row: Dict[str, Any]) -> str:
    if args.mode == 'template':
        return render_template(args.template, row)

    profile = fetch_latest_profile(conn, row.get('sender_db_id'))
    return render_conversational_reply(row, profile, args.persona_name)


def mark_auto_responded(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH matched AS (
              SELECT
                m.id AS inbound_id,
                o.external_message_id AS outbound_external_id,
                o.sent_at AS outbound_sent_at
              FROM dm_messages m
              JOIN LATERAL (
                SELECT o.external_message_id, o.sent_at
                FROM dm_messages o
                WHERE o.conversation_id = m.conversation_id
                  AND o.direction = 'outbound'
                  AND o.sent_at >= m.sent_at
                ORDER BY o.sent_at ASC, o.id ASC
                LIMIT 1
              ) o ON TRUE
              WHERE m.direction = 'inbound'
                AND m.response_status IN ('pending', 'failed', 'sending')
            )
            UPDATE dm_messages m
            SET response_status = 'responded',
                response_message_external_id = COALESCE(m.response_message_external_id, matched.outbound_external_id),
                responded_at = COALESCE(m.responded_at, matched.outbound_sent_at),
                response_last_error = NULL
            FROM matched
            WHERE m.id = matched.inbound_id
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
    candidate_limit = max(limit * 10, limit)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            WITH candidates AS (
              SELECT m.id, m.conversation_id, m.sent_at
              FROM dm_messages m
              WHERE m.direction = 'inbound'
                AND m.response_status IN ('pending', 'failed')
                AND m.response_attempts < %s
                AND NOT EXISTS (
                  SELECT 1
                  FROM dm_messages o
                  WHERE o.conversation_id = m.conversation_id
                    AND o.direction = 'outbound'
                    AND o.sent_at >= m.sent_at
                )
              ORDER BY m.sent_at ASC
              LIMIT %s
              FOR UPDATE SKIP LOCKED
            ),
            pending AS (
              SELECT ranked.id
              FROM (
                SELECT
                  c.id,
                  c.sent_at,
                  ROW_NUMBER() OVER (
                    PARTITION BY c.conversation_id
                    ORDER BY c.sent_at ASC, c.id ASC
                  ) AS conversation_rank
                FROM candidates c
              ) ranked
              WHERE ranked.conversation_rank = 1
              ORDER BY ranked.sent_at ASC, ranked.id ASC
              LIMIT %s
            ),
            claimed AS (
              UPDATE dm_messages
              SET response_status = 'sending',
                  response_attempted_at = now(),
                  response_attempts = response_attempts + 1,
                  response_last_error = NULL
              WHERE id IN (SELECT id FROM pending)
              RETURNING id, conversation_id, external_message_id, text, sender_id, sent_at, response_attempts, response_status
            )
            SELECT
              c.id,
              c.conversation_id,
              c.sender_id AS sender_db_id,
              c.external_message_id,
              c.text,
              c.response_attempts,
              c.response_status,
              c.sent_at,
              u.external_id AS sender_external_id,
              u.handle AS sender_handle,
              u.display_name
            FROM claimed c
            JOIN users u ON u.id = (SELECT sender_id FROM dm_messages WHERE id = c.id)
            """,
            [max_retries, candidate_limit, limit],
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


def mark_not_applicable(conn, msg_id: int, reason: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE dm_messages
            SET response_status = 'not_applicable',
                response_last_error = %s,
                response_attempted_at = now()
            WHERE id = %s
            """,
            [reason[:2048], msg_id],
        )


def mark_responded_from_existing_outbound(conn, msg_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH matched AS (
              SELECT
                m.id AS inbound_id,
                o.external_message_id AS outbound_external_id,
                o.sent_at AS outbound_sent_at
              FROM dm_messages m
              JOIN LATERAL (
                SELECT o.external_message_id, o.sent_at
                FROM dm_messages o
                WHERE o.conversation_id = m.conversation_id
                  AND o.direction = 'outbound'
                  AND o.sent_at >= m.sent_at
                ORDER BY o.sent_at ASC, o.id ASC
                LIMIT 1
              ) o ON TRUE
              WHERE m.id = %s
            )
            UPDATE dm_messages m
            SET response_status = 'responded',
                response_message_external_id = COALESCE(m.response_message_external_id, matched.outbound_external_id),
                responded_at = COALESCE(m.responded_at, matched.outbound_sent_at),
                response_last_error = NULL
            FROM matched
            WHERE m.id = matched.inbound_id
            RETURNING m.id
            """,
            [msg_id],
        )
        return cur.fetchone() is not None


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
    dispatched_signatures = set()
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
                        skipped += 1
                        if not mark_responded_from_existing_outbound(conn, row['id']):
                            mark_not_applicable(conn, row['id'], 'already_responded_externally')
                        conn.commit()
                        continue

                text = render_response(args, conn, row)
                batch_key = (row['conversation_id'], row['sender_external_id'], row['sent_at'], text)
                if batch_key in dispatched_signatures:
                    skipped += 1
                    mark_not_applicable(conn, row['id'], 'duplicate_text_in_same_batch')
                    conn.commit()
                    continue

                # Optional idempotence guard: avoid re-sending exact same outgoing text.
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM dm_messages o
                        WHERE o.conversation_id = %s
                          AND o.direction = 'outbound'
                          AND o.sent_at >= (SELECT sent_at FROM dm_messages WHERE id = %s)
                          AND o.text = %s
                        LIMIT 1
                        """,
                        [row['conversation_id'], row['id'], text],
                    )
                    if cur.fetchone():
                        skipped += 1
                        mark_not_applicable(conn, row['id'], 'duplicate_text_already_sent')
                        conn.commit()
                        continue

                if args.dry_run:
                    print(f"DRY-RUN would reply to {row['sender_external_id']} with: {text[:160]}")
                    mark_responded(conn, row['id'], 'dry-run')
                    conn.commit()
                    sent += 1
                    dispatched_signatures.add(batch_key)
                    continue

                sentMsg = await client.send_message(peer_id, text)
                mark_responded(conn, row['id'], str(sentMsg.id))
                sent += 1
                dispatched_signatures.add(batch_key)
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
