#!/usr/bin/env python3
"""
Real-time Telegram ingestion listener.

- Group/channel messages stream into existing tables:
  users, groups, messages, message_mentions, memberships
- Private chats (DMs) stream into dedicated tables:
  dm_conversations, dm_messages, dm_interpretations

Usage:
  make tg:listen
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import psycopg
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import Chat, Channel, Message, User

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent

# load both repo .env and collector .env
load_dotenv(REPO_ROOT / '.env')
load_dotenv(SCRIPT_DIR / '.env')

TG_API_ID = os.getenv('TG_API_ID')
TG_API_HASH = os.getenv('TG_API_HASH')
TG_SESSION_PATH = os.getenv('TG_SESSION_PATH', 'tools/telethon_collector/telethon_openclaw.session')
DATABASE_DSN = os.getenv('DATABASE_URL') or os.getenv('PG_DSN')

if not TG_API_ID or not TG_API_HASH:
  raise SystemExit('Missing TG_API_ID / TG_API_HASH in .env')
if not DATABASE_DSN:
  raise SystemExit('Missing DATABASE_URL (or PG_DSN) in env')

MENTION_RE = re.compile(r'@([a-zA-Z][\w]{2,31})')
LINK_RE = re.compile(r'https?://[^\s)\]>]+')


def now_iso() -> str:
  return datetime.now(timezone.utc).isoformat()


def sha256(text: str) -> str:
  return hashlib.sha256(text.encode('utf-8')).hexdigest()


def to_text(value: object | None) -> str:
  if value is None:
    return ''
  if isinstance(value, str):
    return value
  return str(value)


def mentions_from_text(raw: str) -> list[str]:
  return sorted({m.group(1).lower() for m in MENTION_RE.finditer(raw or '')})


def serialize_message(msg: Message) -> str:
  try:
    return json.dumps(msg.to_dict(), ensure_ascii=False, default=str)
  except Exception:
    return json.dumps({'id': msg.id, 'chat_id': msg.chat_id}, ensure_ascii=False)


def display_name_from(user: User | None) -> Optional[str]:
  if not user:
    return None
  parts = [getattr(user, 'first_name', None), getattr(user, 'last_name', None)]
  parts = [p for p in parts if p]
  name = ' '.join(parts).strip()
  return name or None



def infer_group_kind(chat) -> str:
  if isinstance(chat, Channel):
    # Telegram supergroups are Channel + megagroup=True
    if bool(getattr(chat, 'broadcast', False)):
      return 'work'
    if bool(getattr(chat, 'megagroup', False)):
      return 'bd'
    return 'general_chat'
  if isinstance(chat, Chat):
    return 'general_chat'
  return 'unknown'


def upsert_user(cursor, external_id: int, handle: Optional[str], display_name: Optional[str], is_premium: bool = False) -> int:
  cursor.execute(
    '''
    INSERT INTO users (platform, external_id, handle, display_name, is_premium)
    VALUES ('telegram', %s, %s, %s, %s)
    ON CONFLICT (platform, external_id)
    DO UPDATE SET
      handle = COALESCE(EXCLUDED.handle, users.handle),
      display_name = COALESCE(EXCLUDED.display_name, users.display_name),
      is_premium = COALESCE(EXCLUDED.is_premium, users.is_premium),
      updated_at = NOW()
    RETURNING id
    ''',
    (f'user{external_id}', handle, display_name, is_premium),
  )
  row = cursor.fetchone()
  return int(row[0])


def upsert_group(cursor, external_id: int, title: Optional[str], kind: str = 'unknown') -> int:
  cursor.execute(
    '''
    INSERT INTO groups (platform, external_id, title, kind)
    VALUES ('telegram', %s, %s, %s::group_kind)
    ON CONFLICT (platform, external_id)
    DO UPDATE SET title = COALESCE(EXCLUDED.title, groups.title), updated_at = NOW()
    RETURNING id
    ''',
    (str(external_id), title, kind),
  )
  row = cursor.fetchone()
  return int(row[0])


def ensure_membership(cursor, group_id: int, user_id: int) -> None:
  cursor.execute(
    '''
    INSERT INTO memberships (group_id, user_id, first_seen_at, last_seen_at, msg_count, is_current_member)
    VALUES (%s, %s, NOW(), NOW(), 1, true)
    ON CONFLICT (group_id, user_id) DO UPDATE
      SET last_seen_at = NOW(), msg_count = memberships.msg_count + 1, is_current_member = true
    ''',
    (group_id, user_id),
  )


def upsert_group_message(cursor, group_id: int, sender_user_id: Optional[int], msg: Message, mentions: list[str], reply_to: Optional[str]) -> Optional[int]:
  text = to_text(msg.message)
  cursor.execute(
    '''
    INSERT INTO messages (
      group_id, user_id, external_message_id, sent_at, text, text_len,
      reply_to_external_message_id, has_links, has_mentions, raw_ref_row_id,
      views, forwards, reply_count, reaction_count, reactions, media_type
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (group_id, external_message_id) DO UPDATE
      SET text = EXCLUDED.text,
          text_len = EXCLUDED.text_len,
          has_links = EXCLUDED.has_links,
          has_mentions = EXCLUDED.has_mentions,
          sent_at = EXCLUDED.sent_at
    RETURNING id
    ''',
    (
      group_id,
      sender_user_id,
      str(msg.id),
      msg.date.astimezone(timezone.utc),
      text,
      len(text),
      reply_to,
      bool(LINK_RE.search(text or '')),
      bool(mentions),
      0,
      0,
      0,
      0,
      json.dumps({'results': []}),
      (msg.media.__class__.__name__ if getattr(msg, 'media', None) else None),
    ),
  )
  row = cursor.fetchone()
  return int(row[0])


def link_message_mentions(cursor, message_id: int, text: str) -> None:
  for handle in mentions_from_text(text):
    cursor.execute(
      'SELECT id FROM users WHERE platform = %s AND lower(handle) = lower(%s) LIMIT 1',
      ('telegram', handle),
    )
    row = cursor.fetchone()
    mentioned_user_id = int(row[0]) if row else None
    cursor.execute(
      '''
      INSERT INTO message_mentions (message_id, mentioned_handle, mentioned_user_id)
      VALUES (%s, %s, %s)
      ON CONFLICT DO NOTHING
      ''',
      (message_id, handle, mentioned_user_id),
    )


def upsert_dm_conversation(cursor, account_user_id: int, subject_user_id: int, chat_id: int, title: Optional[str]) -> int:
  cursor.execute(
    '''
    INSERT INTO dm_conversations (
      account_user_id, subject_user_id, platform, external_chat_id, title, last_activity_at
    )
    VALUES (%s, %s, 'telegram', %s, %s, NOW())
    ON CONFLICT (platform, account_user_id, external_chat_id)
    DO UPDATE SET
      subject_user_id = EXCLUDED.subject_user_id,
      title = COALESCE(EXCLUDED.title, dm_conversations.title),
      last_activity_at = NOW()
    RETURNING id
    ''',
    (account_user_id, subject_user_id, str(chat_id), title),
  )
  row = cursor.fetchone()
  return int(row[0])


def upsert_dm_message(cursor, conversation_id: int, direction: str, msg: Message, mentions: list[str]) -> Optional[int]:
  text = to_text(msg.message)
  msg_hash = sha256(f'{msg.chat_id}:{msg.id}:{text}')
  cursor.execute(
    '''
    INSERT INTO dm_messages (
      conversation_id, external_message_id, direction, message_text,
      text_hash, sent_at, raw_json, response_to_external_message_id,
      has_links, has_mentions, extracted_handles
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
    ON CONFLICT (conversation_id, external_message_id)
    DO UPDATE SET
      direction = EXCLUDED.direction,
      message_text = EXCLUDED.message_text,
      text_hash = EXCLUDED.text_hash,
      sent_at = EXCLUDED.sent_at,
      raw_json = EXCLUDED.raw_json,
      response_to_external_message_id = EXCLUDED.response_to_external_message_id,
      has_links = EXCLUDED.has_links,
      has_mentions = EXCLUDED.has_mentions,
      extracted_handles = EXCLUDED.extracted_handles
    RETURNING id
    ''',
    (
      conversation_id,
      str(msg.id),
      direction,
      text,
      msg_hash,
      msg.date.astimezone(timezone.utc),
      serialize_message(msg),
      str(msg.reply_to_msg_id) if getattr(msg, 'reply_to_msg_id', None) else None,
      bool(LINK_RE.search(text or '')),
      bool(mentions),
      mentions,
    ),
  )
  row = cursor.fetchone()
  return int(row[0])


def classify_dm_snippet(text: str) -> tuple[str, str, bool]:
  lowered = text.lower()
  if any(token in lowered for token in ('interested', 'yes', 'sounds good', 'love to', 'let\'s chat', 'available')):
    return 'positive_response', 'Likely positive or open reply', True
  if any(token in lowered for token in ('not interested', 'no', 'pass', 'unsubscribe', 'remove me')):
    return 'pushback', 'Explicit pushback / decline', False
  if '?' in text:
    return 'inquiry', 'Question or clarification request', True
  return 'general_response', 'Captured for context', False


async def ensure_account_user(client: TelegramClient, db_conn: psycopg.Connection) -> int:
  me = await client.get_me()
  with db_conn.cursor() as cur:
    user_id = upsert_user(
      cur,
      me.id,
      getattr(me, 'username', None),
      display_name_from(me),
      bool(getattr(me, 'premium', False)),
    )
    db_conn.commit()
    return user_id


async def handle_group_message(ctx: tuple[psycopg.Connection, int], event: events.NewMessage.Event) -> None:
  db_conn, account_user_id = ctx
  msg = event.message
  chat = await event.get_chat()

  with db_conn.cursor() as cur:
    group_id = upsert_group(cur, int(event.chat_id), getattr(chat, 'title', None), infer_group_kind(chat))

    sender_user_id = None
    if event.sender_id:
      sender = await event.get_sender()
      if isinstance(sender, User):
        sender_user_id = upsert_user(
          cur,
          sender.id,
          getattr(sender, 'username', None),
          display_name_from(sender),
          bool(getattr(sender, 'premium', False)),
        )

    text = to_text(msg.message)
    mentions = mentions_from_text(text)
    reply_to = str(msg.reply_to_msg_id) if getattr(msg, 'reply_to_msg_id', None) else None
    message_id = upsert_group_message(cur, group_id, sender_user_id, msg, mentions, reply_to)

    if sender_user_id is not None:
      ensure_membership(cur, group_id, sender_user_id)
      cur.execute('UPDATE users SET needs_enrichment = true, last_msg_at = NOW() WHERE id = %s', (sender_user_id,))
      link_message_mentions(cur, message_id, text)

    cur.execute('UPDATE dm_conversations SET last_activity_at = NOW() WHERE account_user_id = %s AND platform = %s', (account_user_id, 'telegram'))
    db_conn.commit()


async def handle_dm_message(ctx: tuple[psycopg.Connection, int], event: events.NewMessage.Event) -> None:
  db_conn, account_user_id = ctx
  msg = event.message
  chat = await event.get_chat()

  if not isinstance(chat, User):
    return

  direction = 'outbound' if bool(event.out) else 'inbound'
  msg_text = to_text(msg.message)
  mentions = mentions_from_text(msg_text)

  with db_conn.cursor() as cur:
    # subject is the counterpart in the private conversation
    subject_user_id = upsert_user(
      cur,
      chat.id,
      getattr(chat, 'username', None),
      display_name_from(chat),
      bool(getattr(chat, 'premium', False)),
    )

    conv_id = upsert_dm_conversation(
      cur,
      account_user_id,
      subject_user_id,
      int(event.chat_id),
      getattr(chat, 'first_name', None) or getattr(chat, 'title', None),
    )

    dm_message_id = upsert_dm_message(cur, conv_id, direction, msg, mentions)

    # Ensure we enrich on inbound replies
    if direction == 'inbound':
      cur.execute('UPDATE users SET needs_enrichment = true, last_msg_at = NOW() WHERE id = %s', (subject_user_id,))

    if dm_message_id is not None and msg_text.strip():
      kind, summary, requires_followup = classify_dm_snippet(msg_text)
      # keep lightweight interpretation trail for follow-up tooling
      cur.execute(
        '''
        INSERT INTO dm_interpretations (dm_message_id, kind, summary, requires_followup)
        VALUES (%s, %s, %s, %s)
        ''',
        (dm_message_id, kind, summary, requires_followup),
      )

    cur.execute('UPDATE dm_conversations SET last_activity_at = NOW() WHERE id = %s', (conv_id,))
    db_conn.commit()


async def run_listener() -> None:
  pg = psycopg.connect(DATABASE_DSN)
  client = TelegramClient(TG_SESSION_PATH, int(TG_API_ID), TG_API_HASH)

  try:
    await client.start()
    account_user_id = await ensure_account_user(client, pg)
    print(f'[{now_iso()}] Listener ready. account_user_id={account_user_id}')

    context = (pg, account_user_id)

    @client.on(events.NewMessage)
    async def on_new_message(event: events.NewMessage.Event) -> None:
      try:
        if event.is_private:
          await handle_dm_message(context, event)
        else:
          await handle_group_message(context, event)
      except Exception as e:
        print(f'[{now_iso()}] message ingest error: {e}', file=sys.stderr)

    await client.run_until_disconnected()
  finally:
    await client.disconnect()
    pg.close()


if __name__ == '__main__':
  while True:
    try:
      asyncio.run(run_listener())
      break
    except (KeyboardInterrupt, SystemExit):
      print('Shutdown complete.')
      raise
    except Exception as e:
      print(f'[{now_iso()}] listener crashed: {e}', file=sys.stderr)
      print('Restarting in 5s...')
      import time
      time.sleep(5)
