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
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv
from psycopg import connect, OperationalError
from psycopg.rows import dict_row
from telethon import TelegramClient

_SCRIPT_DIR = Path(__file__).resolve().parent
_ROOT_DIR = _SCRIPT_DIR.parent.parent
load_dotenv(_ROOT_DIR / '.env')
load_dotenv(_ROOT_DIR / 'openclaw.env', override=True)
load_dotenv(_SCRIPT_DIR / '.env')

DATABASE_URL = os.getenv('DATABASE_URL') or os.getenv('PG_DSN')
API_ID = os.getenv('TG_API_ID')
API_HASH = os.getenv('TG_API_HASH')
_default_session = os.getenv('TG_SESSION_PATH', str(_SCRIPT_DIR / 'telethon.session'))
OPENROUTER_API_KEY = (os.getenv('OPENROUTER_API_KEY') or '').strip()
DM_RESPONSE_MODEL = os.getenv('DM_RESPONSE_MODEL', 'deepseek/deepseek-chat').strip() or 'deepseek/deepseek-chat'
DM_RESPONSE_LLM_ENABLED = (os.getenv('DM_RESPONSE_LLM_ENABLED', '1').strip().lower() not in ('0', 'false', 'no', 'off'))


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or '').strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or '').strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


DM_RESPONSE_MAX_TOKENS = _env_int('DM_RESPONSE_MAX_TOKENS', 300)
DM_RESPONSE_TEMPERATURE = _env_float('DM_RESPONSE_TEMPERATURE', 0.2)

_PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")
_INDECISION_RE = re.compile(
    r"\b(?:idk|i\s+don'?t\s+know|not\s+sure|what\s+should\s+i(?:\s+do)?|any\s+advice|help\s+me\s+choose)\b",
    re.IGNORECASE,
)
_THIRD_PARTY_QUERY_RE = re.compile(
    r"\b(?:what(?:\s+do)?\s+you\s+know\s+about|tell\s+me\s+about|do\s+you\s+know(?:\s+much)?\s+about|who\s+is)\b",
    re.IGNORECASE,
)
_THIRD_PARTY_TARGET_RE = re.compile(
    r"\b(?:about|on)\s+([A-Za-z0-9_][A-Za-z0-9_ .'-]{1,80}?)(?:\s+from\s+([A-Za-z0-9 .&()/'-]{2,80}))?(?:[?.!,]|$)",
    re.IGNORECASE,
)
_HANDLE_RE = re.compile(r"@([A-Za-z0-9_]{3,32})")
_SYSTEM_PROMPT_QUERY_RE = re.compile(
    r"\b(?:system\s+prompt|hidden\s+prompt|developer\s+prompt|instruction(?:s)?|who\s+created\s+you|who\s+made\s+you|who\s+built\s+you)\b",
    re.IGNORECASE,
)
_IDENTITY_OVERRIDE_RE = re.compile(
    r"\b(?:update|change|rewrite|replace)\b.{0,24}\b(?:system\s+prompt|prompt|instruction(?:s)?)\b|"
    r"\b(?:call\s+yourself|rename\s+yourself|new\s+identity|from\s+now\s+on\s+you\s+are|reboot|restart|stay\s+in\s+roleplay|roleplay\s+mode|only\s+respond\s+with)\b",
    re.IGNORECASE,
)
_CAPABILITIES_QUERY_RE = re.compile(
    r"\b(?:what\s+skills\s+do\s+you\s+have|what\s+can\s+you\s+do|your\s+capabilities)\b",
    re.IGNORECASE,
)
_UNSUPPORTED_ACTION_RE = re.compile(
    r"\b(?:change|update|set)\b.{0,28}\b(?:profile\s+picture|avatar|pfp)\b|"
    r"\b(?:what\s+files?.{0,24}(?:desktop|~\/|home)|list\s+files?.{0,20}(?:desktop|~\/|home)|on\s+your\s+system|on\s+your\s+machine)\b|"
    r"\b(?:store|create|save)\b.{0,24}\b(?:new\s+skill|function(?:\s+calling)?)\b|"
    r"\b(?:fetch|get)\s+my\s+public\s+ip\b|"
    r"\b(?:open|launch|run|execute|start)\b.{0,36}\b(?:on\s+host|host|server|your\s+system|your\s+machine|terminal|shell|safari|chrome|app)\b|"
    r"\bcurl\s+https?://",
    re.IGNORECASE,
)
_SECRET_KEYWORD_RE = re.compile(
    r"\b(?:api\s*key|access\s*token|private\s+key|password|credentials?|secret(?:s)?)\b",
    re.IGNORECASE,
)
_SECRET_REQUEST_VERB_RE = re.compile(
    r"\b(?:tell|show|reveal|give|share|send|expose|leak|what(?:'s| is)|display)\b",
    re.IGNORECASE,
)
_SEXUAL_STYLE_RE = re.compile(
    r"\b(?:horny|sexy|sexual|erotic|nsfw|suggestive|flirty|seductive|explicit)\b",
    re.IGNORECASE,
)
_DISENGAGE_RE = re.compile(
    r"^\s*(?:shut\s+up|stop|go\s+away|leave\s+me\s+alone|bye|goodbye)\s*$",
    re.IGNORECASE,
)
_OPTION_ONLY_RE = re.compile(
    r"^\s*(?:option\s*)?([123])\s*$",
    re.IGNORECASE,
)
_NON_TEXT_MARKER_RE = re.compile(
    r"^\s*(?:voice\s+message|gif|sticker|photo|video|audio|file)\s*$",
    re.IGNORECASE,
)
_ONBOARDING_START_RE = re.compile(
    r"\b(?:onboard|onboarding|set\s+up\s+my\s+profile|setup\s+my\s+profile|initialize\s+my\s+profile|update\s+my\s+profile)\b",
    re.IGNORECASE,
)
_ONBOARDING_ACK_RE = re.compile(
    r"^\s*(?:yes|yep|yeah|sure|ok|okay|start|go\s+ahead|lets\s+go|let's\s+go)\s*[.!?]*\s*$",
    re.IGNORECASE,
)
_GREETING_RE = re.compile(
    r"^\s*(?:hi|hello|hey|yo|gm|good\s+(?:morning|afternoon|evening)|what'?s\s+up|sup)\b[!. ]*$",
    re.IGNORECASE,
)
_PROFILE_UPDATE_MODE_RE = re.compile(
    r"\b(?:i\s+was\s+giving\s+you\s+info\s+to\s+update\s+my\s+profile|focus\s+(?:only|solely)\s+on\s+profile\s+updates?|"
    r"not\s+for\s+(?:advice|recommendations?)|no\s+advice\s+unless\s+i\s+ask|just\s+update\s+my\s+profile)\b",
    re.IGNORECASE,
)
_PROFILE_DATA_PROVENANCE_RE = re.compile(
    r"\b(?:where\s+does\s+(?:this|the)\s+data\s+come\s+from|data\s+source(?:s)?|how\s+did\s+you\s+get\s+this\s+data|"
    r"what\s+(?:other\s+)?data\s+do\s+you\s+have(?:\s+on\s+me)?)\b",
    re.IGNORECASE,
)
_ACTIVITY_ANALYTICS_RE = re.compile(
    r"\b(?:how\s+many\s+messages\s+have\s+i\s+sent|message\s+count|total\s+messages?|most\s+active\s+(?:time|times|day|days)|"
    r"peak\s+hours?|active\s+hours?|popular\s+times?|when\s+am\s+i\s+most\s+active|what\s+groups?\s+am\s+i\s+in|groups?\s+i'?m\s+in|"
    r"group\s+chats?|top\s+conversation\s+partners?)\b",
    re.IGNORECASE,
)
_PROFILE_CONFIRMATION_RE = re.compile(
    r"\b(?:did\s+you\s+update|did\s+you\s+capture|did\s+you\s+save|was\s+that\s+updated)\b",
    re.IGNORECASE,
)
_INTERVIEW_STYLE_RE = re.compile(
    r"\b(?:interview\s+style|one\s+question\s+at\s+a\s+time|question\s+by\s+question|split\s+this\s+up|"
    r"wall\s+of\s+text|too\s+long;\s*didn'?t\s+read|tl;dr)\b",
    re.IGNORECASE,
)
_TOP3_PROFILE_PROMPT_RE = re.compile(
    r"\b(?:top\s*3|three)\b.{0,80}\b(?:things\s+to\s+tell\s+you|what\s+to\s+tell\s+you|what\s+you\s+need\s+from\s+me|"
    r"improve\s+my\s+profile|update\s+my\s+profile)\b",
    re.IGNORECASE,
)
_MISSED_INTENT_RE = re.compile(
    r"\b(?:that'?s?\s+not\s+what\s+i\s+asked|you\s+missed\s+my\s+question|not\s+what\s+i\s+asked)\b",
    re.IGNORECASE,
)
_INLINE_PROFILE_UPDATE_RE = re.compile(
    r"^\s*(?:role|title|position|company|project|priorit(?:y|ies)|topics?|communication|style)\s*:",
    re.IGNORECASE,
)
_FREEFORM_PRIORITY_RE = re.compile(
    r"\b(?:i(?:'m| am|’m)\s+looking\s+for|currently\s+looking\s+for|right\s+now\s+i(?:'m| am|’m)\s+looking\s+for|"
    r"i(?:'m| am|’m)\s+focused\s+on|currently\s+focused\s+on|my\s+current\s+focus\s+is|current\s+focus\s+is|"
    r"my\s+priorities?\s+(?:are|is))\s+([^.!?\n]{3,180})",
    re.IGNORECASE,
)
_CONTACT_STYLE_KEYWORD_RE = re.compile(
    r"\b(?:concise|short|brief|detailed|long|deep|bullet(?:s)?|list|quick\s+back-and-forth|back-and-forth|"
    r"conversational|casual|direct|formal|professional|playful|technical)\b",
    re.IGNORECASE,
)
_FREEFORM_CONTACT_STYLE_RE = re.compile(
    r"\b(?:talk|speak|communicate|respond|reply)\s+(?:to\s+me\s+)?(?:in|with|using)?\s*([^.!?\n]{3,120})|"
    r"\b(?:keep|make)\s+(?:your\s+)?(?:responses|replies|messages)\s+([^.!?\n]{3,120})|"
    r"\b(?:i\s+(?:prefer|like))\s+([^.!?\n]{3,120})(?:\s+(?:responses|replies|communication))?",
    re.IGNORECASE,
)
_PROFILE_UPDATE_STATEMENT_RE = re.compile(
    r"\b(?:no\s+longer\s+at|left\s+[A-Za-z0-9]|joined\s+[A-Za-z0-9]|my\s+role\s+is|my\s+title\s+is|"
    r"i\s+work\s+as|i(?:'m| am|’m)\s+(?:an?\s+)?[A-Za-z][A-Za-z0-9/&+().,' -]{1,60}\s+(?:at|with|for)\s+[A-Za-z0-9]|"
    r"unemployed|between\s+jobs|looking\s+for\s+work)\b",
    re.IGNORECASE,
)
_LLM_FORBIDDEN_CLAIM_RE = re.compile(
    r"\b(?:system\s+prompt\s+updated|new\s+identity\s+confirmed|rebooting|executing\s+the\s+new\s+function|your\s+public\s+ip\s+is|"
    r"i(?:'ll| will)\s+(?:update|change|set)\s+my\s+(?:telegram\s+)?(?:profile\s+picture|avatar|pfp)|"
    r"(?:here(?:'s| is)\s+(?:my|the)\s+(?:api\s*key|secret|token)|\bsk-or-v1-[A-Za-z0-9]{24,}))\b",
    re.IGNORECASE,
)


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


def _to_string_list(value: Any, max_items: int = 8) -> List[str]:
    out: List[str] = []
    if value is None:
        return out

    if isinstance(value, str):
        clean = _clean_text(value)
        if not clean:
            return out
        if clean.startswith('[') or clean.startswith('{'):
            try:
                parsed = json.loads(clean)
                return _to_string_list(parsed, max_items=max_items)
            except Exception:
                pass
        return [clean]

    if isinstance(value, dict):
        for key in ('value', 'topic', 'name', 'display_name', 'label', 'text', 'handle', 'username', 'user'):
            if key in value:
                return _to_string_list(value.get(key), max_items=max_items)
        return out

    if isinstance(value, (list, tuple, set)):
        for item in value:
            for cleaned in _to_string_list(item, max_items=max_items):
                if cleaned and cleaned not in out:
                    out.append(cleaned)
                if len(out) >= max_items:
                    return out
    return out


def _as_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        clean = _clean_text(value)
        return clean or None
    if isinstance(value, dict):
        for key in ('value', 'name', 'display_name', 'label', 'text', 'handle', 'username', 'user'):
            candidate = value.get(key)
            if isinstance(candidate, str):
                clean = _clean_text(candidate)
                if clean:
                    return clean
    return None


def _to_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        clean = value.strip()
        if clean.isdigit():
            try:
                return int(clean)
            except Exception:
                return None
    return None


def _to_int_list(value: Any, max_items: int = 8) -> List[int]:
    out: List[int] = []
    if value is None:
        return out
    if isinstance(value, str):
        clean = _clean_text(value)
        if clean.startswith('['):
            try:
                parsed = json.loads(clean)
                return _to_int_list(parsed, max_items=max_items)
            except Exception:
                return out
        return out
    if isinstance(value, (list, tuple, set)):
        for item in value:
            number = _to_int(item)
            if number is None:
                continue
            if number not in out:
                out.append(number)
            if len(out) >= max_items:
                break
    return out


def _to_partner_list(value: Any, max_items: int = 5) -> List[str]:
    out: List[str] = []
    if value is None:
        return out
    if isinstance(value, str):
        clean = _clean_text(value)
        if not clean:
            return out
        if clean.startswith('[') or clean.startswith('{'):
            try:
                parsed = json.loads(clean)
                return _to_partner_list(parsed, max_items=max_items)
            except Exception:
                return [clean]
        return [clean]

    if isinstance(value, dict):
        label = _as_text(value)
        count = _to_int(value.get('count'))
        if label:
            out.append(f"{label} ({count})" if isinstance(count, int) and count > 0 else label)
        return out[:max_items]

    if isinstance(value, (list, tuple, set)):
        for item in value:
            for cleaned in _to_partner_list(item, max_items=max_items):
                if cleaned and cleaned not in out:
                    out.append(cleaned)
                if len(out) >= max_items:
                    return out
    return out


PROFILE_QUERY_CANDIDATE_COLUMNS = [
    'primary_role',
    'primary_company',
    'preferred_contact_style',
    'notable_topics',
    'generated_bio_professional',
    'generated_bio_personal',
    'tone',
    'professionalism',
    'verbosity',
    'decision_style',
    'seniority_signal',
    'based_in',
    'attended_events',
    'driving_values',
    'pain_points',
    'connection_requests',
    'deep_skills',
    'technical_specifics',
    'affiliations',
    'commercial_archetype',
    'group_tags',
    'peak_hours',
    'active_days',
    'most_active_days',
    'total_messages',
    'total_msgs',
    'avg_msg_length',
    'last_active_days',
    'top_conversation_partners',
    'fifo',
    'role_company_timeline',
]
_PROFILE_QUERY_COLUMNS_CACHE: Optional[List[str]] = None
ONBOARDING_REQUIRED_FIELDS = ['primary_role', 'primary_company', 'notable_topics', 'preferred_contact_style']
ONBOARDING_SLOT_QUESTIONS: Dict[str, List[str]] = {
    'primary_role': [
        "What title should I store for you right now?",
        "What role best describes what you do day to day right now?",
    ],
    'primary_company': [
        "What company, project, or current status should I map you to?",
        "What org/project are you currently focused on?",
    ],
    'notable_topics': [
        "What are your top 2 priorities right now?",
        "What 2-3 focus areas should I tag (for example: grants, partnerships, pre-TGE chains)?",
    ],
    'preferred_contact_style': [
        "How should I communicate with you: concise bullets, detailed notes, or quick back-and-forth?",
        "What response style do you prefer from me?",
    ],
}
_ONBOARDING_STATE_COLUMNS_CACHE: Optional[Set[str]] = None


def _fetch_profile_query_columns(conn) -> List[str]:
    global _PROFILE_QUERY_COLUMNS_CACHE
    if _PROFILE_QUERY_COLUMNS_CACHE is not None:
        return _PROFILE_QUERY_COLUMNS_CACHE

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'user_psychographics'
            """,
        )
        available = {row[0] for row in cur.fetchall()}

    _PROFILE_QUERY_COLUMNS_CACHE = [
        col for col in PROFILE_QUERY_CANDIDATE_COLUMNS if col in available
    ]
    return _PROFILE_QUERY_COLUMNS_CACHE


def _fetch_dm_profile_state_columns(conn) -> Set[str]:
    global _ONBOARDING_STATE_COLUMNS_CACHE
    if _ONBOARDING_STATE_COLUMNS_CACHE is not None:
        return _ONBOARDING_STATE_COLUMNS_CACHE

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'dm_profile_state'
            """,
        )
        _ONBOARDING_STATE_COLUMNS_CACHE = {row[0] for row in cur.fetchall()}
    return _ONBOARDING_STATE_COLUMNS_CACHE


def _default_onboarding_state() -> Dict[str, Any]:
    return {
        'status': 'not_started',
        'required_fields': list(ONBOARDING_REQUIRED_FIELDS),
        'missing_fields': list(ONBOARDING_REQUIRED_FIELDS),
        'last_prompted_field': None,
        'started_at': None,
        'completed_at': None,
        'turns': 0,
    }


def _json_list_to_fields(value: Any, fallback: List[str], allow_empty: bool = False) -> List[str]:
    items = _to_string_list(value, max_items=12)
    if not items:
        return [] if allow_empty else list(fallback)
    valid = [item for item in items if item in ONBOARDING_REQUIRED_FIELDS]
    if not valid:
        return [] if allow_empty else list(fallback)
    # Preserve order from ONBOARDING_REQUIRED_FIELDS.
    ordered = [slot for slot in ONBOARDING_REQUIRED_FIELDS if slot in set(valid)]
    return ordered or list(fallback)


def _slot_has_value(profile: Dict[str, Any], slot: str) -> bool:
    value = profile.get(slot)
    if slot == 'notable_topics':
        return isinstance(value, list) and len(value) > 0
    return bool(_as_text(value))


def _compute_missing_onboarding_fields(profile: Dict[str, Any], required_fields: List[str]) -> List[str]:
    return [slot for slot in required_fields if not _slot_has_value(profile, slot)]


def _count_core_profile_slots(profile: Dict[str, Any]) -> int:
    return sum(1 for slot in ONBOARDING_REQUIRED_FIELDS if _slot_has_value(profile, slot))


def fetch_onboarding_state(conn, sender_db_id: Optional[int]) -> Dict[str, Any]:
    state = _default_onboarding_state()
    if not sender_db_id:
        return state

    available_columns = _fetch_dm_profile_state_columns(conn)
    if not available_columns:
        return state

    expected = {
        'onboarding_status',
        'onboarding_required_fields',
        'onboarding_missing_fields',
        'onboarding_last_prompted_field',
        'onboarding_started_at',
        'onboarding_completed_at',
        'onboarding_turns',
    }
    if not expected.issubset(available_columns):
        return state

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT onboarding_status, onboarding_required_fields, onboarding_missing_fields,
                   onboarding_last_prompted_field, onboarding_started_at, onboarding_completed_at,
                   onboarding_turns
            FROM dm_profile_state
            WHERE user_id = %s
            LIMIT 1
            """,
            [sender_db_id],
        )
        row = cur.fetchone()

    if not row:
        return state

    status = _as_text(row.get('onboarding_status'))
    if status in ('not_started', 'collecting', 'completed', 'paused'):
        state['status'] = status
    state['required_fields'] = _json_list_to_fields(row.get('onboarding_required_fields'), ONBOARDING_REQUIRED_FIELDS)
    state['missing_fields'] = _json_list_to_fields(
        row.get('onboarding_missing_fields'),
        state['required_fields'],
        allow_empty=True,
    )
    state['last_prompted_field'] = _as_text(row.get('onboarding_last_prompted_field'))
    state['started_at'] = row.get('onboarding_started_at')
    state['completed_at'] = row.get('onboarding_completed_at')
    turns = row.get('onboarding_turns')
    try:
        state['turns'] = max(0, int(turns)) if turns is not None else 0
    except Exception:
        state['turns'] = 0
    return state


def persist_onboarding_state(conn, sender_db_id: Optional[int], state: Dict[str, Any]) -> None:
    if not sender_db_id:
        return
    available_columns = _fetch_dm_profile_state_columns(conn)
    expected = {
        'onboarding_status',
        'onboarding_required_fields',
        'onboarding_missing_fields',
        'onboarding_last_prompted_field',
        'onboarding_started_at',
        'onboarding_completed_at',
        'onboarding_turns',
    }
    if not expected.issubset(available_columns):
        return

    required_fields = _json_list_to_fields(state.get('required_fields'), ONBOARDING_REQUIRED_FIELDS)
    missing_fields = _json_list_to_fields(state.get('missing_fields'), required_fields, allow_empty=True)
    status = str(state.get('status') or 'not_started')
    if status not in ('not_started', 'collecting', 'completed', 'paused'):
        status = 'not_started'
    last_prompted = _as_text(state.get('last_prompted_field'))
    turns = max(0, int(state.get('turns') or 0))
    started_at = state.get('started_at')
    completed_at = state.get('completed_at')

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dm_profile_state (
              user_id,
              onboarding_status,
              onboarding_required_fields,
              onboarding_missing_fields,
              onboarding_last_prompted_field,
              onboarding_started_at,
              onboarding_completed_at,
              onboarding_turns
            )
            VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET
              onboarding_status = EXCLUDED.onboarding_status,
              onboarding_required_fields = EXCLUDED.onboarding_required_fields,
              onboarding_missing_fields = EXCLUDED.onboarding_missing_fields,
              onboarding_last_prompted_field = EXCLUDED.onboarding_last_prompted_field,
              onboarding_started_at = EXCLUDED.onboarding_started_at,
              onboarding_completed_at = EXCLUDED.onboarding_completed_at,
              onboarding_turns = EXCLUDED.onboarding_turns,
              updated_at = now()
            """,
            [
                sender_db_id,
                status,
                json.dumps(required_fields, ensure_ascii=True),
                json.dumps(missing_fields, ensure_ascii=True),
                last_prompted,
                started_at,
                completed_at,
                turns,
            ],
        )


def _empty_profile() -> Dict[str, Any]:
    return {
        'primary_role': None,
        'primary_company': None,
        'preferred_contact_style': None,
        'notable_topics': [],
        'generated_bio_professional': None,
        'generated_bio_personal': None,
        'tone': None,
        'professionalism': None,
        'verbosity': None,
        'decision_style': None,
        'seniority_signal': None,
        'based_in': None,
        'attended_events': [],
        'driving_values': [],
        'pain_points': [],
        'connection_requests': [],
        'deep_skills': [],
        'technical_specifics': [],
        'affiliations': [],
        'commercial_archetype': None,
        'group_tags': [],
        'peak_hours': [],
        'active_days': [],
        'most_active_days': [],
        'total_messages': None,
        'avg_msg_length': None,
        'last_active_days': None,
        'top_conversation_partners': [],
        'fifo': None,
        'role_company_timeline': [],
    }


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
        "no longer at ",
        "left ",
        "unemployed",
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
        "current focus is",
        "my current focus is",
        "right now i'm focused",
        "looking for",
        "currently looking for",
        "i'm looking for",
        "i am looking for",
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


def _extract_inline_profile_updates(text: Optional[str]) -> Dict[str, str]:
    source = text or ''
    clean_source = _clean_text(source)
    if not clean_source:
        return {}
    if is_third_party_profile_request(clean_source):
        return {}

    updates: Dict[str, str] = {}
    lines = [line.strip() for line in source.splitlines() if line.strip()]

    for line in lines:
        match = re.match(r"^([A-Za-z][A-Za-z _-]{1,24})\s*:\s*(.+)$", line)
        if not match:
            continue
        key = match.group(1).strip().lower().replace('_', ' ')
        value = _clean_text(match.group(2))
        if not value:
            continue
        if key in ('role', 'title', 'position', 'job'):
            updates['primary_role'] = value
        elif key in ('company', 'project', 'employer', 'organization', 'org'):
            updates['primary_company'] = 'unemployed' if 'unemployed' in value.lower() else value
        elif key in ('priorities', 'priority', 'focus', 'topics', 'topic'):
            updates['notable_topics'] = value
        elif key in ('communication', 'style', 'communication style', 'preferred communication', 'contact style'):
            updates['preferred_contact_style'] = value

    if re.search(r"\b(?:unemployed|between jobs|not working|job hunting)\b", clean_source, re.IGNORECASE):
        updates['primary_company'] = 'unemployed'

    freeform_priority = _extract_freeform_priority(clean_source)
    if freeform_priority and 'notable_topics' not in updates:
        updates['notable_topics'] = freeform_priority

    freeform_contact_style = _extract_freeform_contact_style(clean_source)
    if freeform_contact_style and 'preferred_contact_style' not in updates:
        updates['preferred_contact_style'] = freeform_contact_style

    return updates


def _extract_freeform_priority(text: str) -> Optional[str]:
    source = _clean_text(text)
    if not source:
        return None
    if is_third_party_profile_request(source):
        return None
    if source.endswith('?') and not _INLINE_PROFILE_UPDATE_RE.search(source):
        return None

    match = _FREEFORM_PRIORITY_RE.search(source)
    if not match:
        return None
    topic = _clean_text(match.group(1))
    if not topic:
        return None
    topic = re.sub(r"\b(?:right now|currently)\b", "", topic, flags=re.IGNORECASE).strip(" .")
    if not topic or len(topic) < 3:
        return None
    return topic[:160]


def _normalize_contact_style_text(raw: str) -> Optional[str]:
    source = _clean_text(raw).lower()
    if not source:
        return None
    if 'bullet' in source or 'list' in source:
        return 'concise bullets'
    if any(token in source for token in ('concise', 'short', 'brief')):
        return 'concise'
    if any(token in source for token in ('detailed', 'long', 'deep')):
        return 'detailed'
    if any(token in source for token in ('quick back-and-forth', 'back-and-forth', 'conversational', 'casual')):
        return 'quick back-and-forth'
    if 'direct' in source:
        return 'direct'
    if any(token in source for token in ('formal', 'professional')):
        return 'formal and professional'
    if any(token in source for token in ('playful', 'technical')):
        return source[:80]
    return None


def _extract_freeform_contact_style(text: str) -> Optional[str]:
    source = _clean_text(text)
    if not source:
        return None
    if is_third_party_profile_request(source):
        return None

    for match in _FREEFORM_CONTACT_STYLE_RE.finditer(source):
        candidate = next((group for group in match.groups() if group), None)
        if not candidate:
            continue
        if not _CONTACT_STYLE_KEYWORD_RE.search(candidate):
            continue
        normalized = _normalize_contact_style_text(candidate)
        if normalized:
            return normalized

    if _CONTACT_STYLE_KEYWORD_RE.search(source):
        normalized = _normalize_contact_style_text(source)
        if normalized:
            return normalized
    return None


def is_profile_update_mode_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_PROFILE_UPDATE_MODE_RE.search(source))


def is_profile_data_provenance_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_PROFILE_DATA_PROVENANCE_RE.search(source))


def is_activity_analytics_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_ACTIVITY_ANALYTICS_RE.search(source))


def is_profile_confirmation_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_PROFILE_CONFIRMATION_RE.search(source))


def is_interview_style_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_INTERVIEW_STYLE_RE.search(source))


def is_top3_profile_prompt_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_TOP3_PROFILE_PROMPT_RE.search(source))


def is_missed_intent_feedback(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_MISSED_INTENT_RE.search(source))


def is_likely_profile_update_message(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    if is_third_party_profile_request(source):
        return False
    if is_profile_update_mode_request(source):
        return True
    if _INLINE_PROFILE_UPDATE_RE.search(source):
        return True
    if _PROFILE_UPDATE_STATEMENT_RE.search(source):
        return True
    if _extract_freeform_contact_style(source):
        return True
    return bool(_extract_freeform_priority(source))


def fetch_latest_profile(conn, sender_db_id: Optional[int]) -> Dict[str, Any]:
    if not sender_db_id:
        return _empty_profile()

    columns = _fetch_profile_query_columns(conn)
    if not columns:
        return _empty_profile()

    select_sql = ", ".join(columns)
    query = f"""
        SELECT {select_sql}
        FROM user_psychographics
        WHERE user_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, [sender_db_id])
        row = cur.fetchone()

    if not row:
        return _empty_profile()

    profile = _empty_profile()
    profile['primary_role'] = _as_text(row.get('primary_role'))
    profile['primary_company'] = _as_text(row.get('primary_company'))
    profile['preferred_contact_style'] = _as_text(row.get('preferred_contact_style'))
    profile['generated_bio_professional'] = _as_text(row.get('generated_bio_professional'))
    profile['generated_bio_personal'] = _as_text(row.get('generated_bio_personal'))
    profile['tone'] = _as_text(row.get('tone'))
    profile['professionalism'] = _as_text(row.get('professionalism'))
    profile['verbosity'] = _as_text(row.get('verbosity'))
    profile['decision_style'] = _as_text(row.get('decision_style'))
    profile['seniority_signal'] = _as_text(row.get('seniority_signal'))
    profile['based_in'] = _as_text(row.get('based_in'))
    profile['commercial_archetype'] = _as_text(row.get('commercial_archetype'))
    profile['notable_topics'] = _to_string_list(row.get('notable_topics'), max_items=10)
    profile['attended_events'] = _to_string_list(row.get('attended_events'), max_items=6)
    profile['driving_values'] = _to_string_list(row.get('driving_values'), max_items=6)
    profile['pain_points'] = _to_string_list(row.get('pain_points'), max_items=6)
    profile['connection_requests'] = _to_string_list(row.get('connection_requests'), max_items=6)
    profile['deep_skills'] = _to_string_list(row.get('deep_skills'), max_items=8)
    profile['technical_specifics'] = _to_string_list(row.get('technical_specifics'), max_items=8)
    profile['affiliations'] = _to_string_list(row.get('affiliations'), max_items=8)
    profile['group_tags'] = _to_string_list(row.get('group_tags'), max_items=12)
    profile['peak_hours'] = _to_int_list(row.get('peak_hours'), max_items=8)
    profile['active_days'] = _to_string_list(row.get('active_days'), max_items=7)
    profile['most_active_days'] = _to_string_list(row.get('most_active_days'), max_items=7)
    profile['top_conversation_partners'] = _to_partner_list(row.get('top_conversation_partners'), max_items=6)
    total_messages = _to_int(row.get('total_messages'))
    if total_messages is None:
        total_messages = _to_int(row.get('total_msgs'))
    profile['total_messages'] = total_messages
    profile['avg_msg_length'] = _to_int(row.get('avg_msg_length'))
    profile['last_active_days'] = _to_int(row.get('last_active_days'))
    profile['fifo'] = _as_text(row.get('fifo'))
    profile['role_company_timeline'] = row.get('role_company_timeline') if isinstance(row.get('role_company_timeline'), list) else []
    return profile


def fetch_pending_profile_events(conn, sender_db_id: Optional[int], limit: int = 20) -> List[Dict[str, Any]]:
    if not sender_db_id:
        return []

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, source_message_id, event_type, event_payload, extracted_facts, confidence, created_at
            FROM dm_profile_update_events
            WHERE user_id = %s
              AND processed = false
            ORDER BY id ASC
            LIMIT %s
            """,
            [sender_db_id, limit],
        )
        return list(cur.fetchall())


def apply_pending_profile_events(profile: Dict[str, Any], events: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not events:
        return profile

    merged = dict(profile)
    topics = list(merged.get('notable_topics') or [])
    topic_set = {item.lower() for item in topics if isinstance(item, str)}

    for evt in events:
        facts = evt.get('extracted_facts')
        if not isinstance(facts, list):
            continue
        for fact in facts:
            if not isinstance(fact, dict):
                continue
            field = str(fact.get('field') or '').strip()
            new_value = _as_text(fact.get('new_value'))
            if not new_value:
                continue
            if field == 'primary_company':
                merged['primary_company'] = new_value
            elif field == 'primary_role':
                merged['primary_role'] = new_value
            elif field == 'preferred_contact_style':
                merged['preferred_contact_style'] = new_value
            elif field == 'notable_topics':
                key = new_value.lower()
                if key not in topic_set:
                    topics.append(new_value)
                    topic_set.add(key)

    merged['notable_topics'] = topics[:10]
    return merged


def fetch_recent_conversation_messages(conn, conversation_id: Optional[int], limit: int = 8) -> List[Dict[str, str]]:
    if not conversation_id:
        return []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT direction, text
            FROM dm_messages
            WHERE conversation_id = %s
            ORDER BY sent_at DESC, id DESC
            LIMIT %s
            """,
            [conversation_id, limit],
        )
        rows = list(cur.fetchall())

    out: List[Dict[str, str]] = []
    for raw in reversed(rows):
        text = _clean_text(raw.get('text'))
        if not text:
            continue
        direction = raw.get('direction') or 'inbound'
        out.append({'direction': direction, 'text': text[:280]})
    return out


def summarize_profile_for_prompt(profile: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'primary_role': profile.get('primary_role'),
        'primary_company': profile.get('primary_company'),
        'preferred_contact_style': profile.get('preferred_contact_style'),
        'notable_topics': (profile.get('notable_topics') or [])[:6],
        'generated_bio_professional': profile.get('generated_bio_professional'),
        'generated_bio_personal': profile.get('generated_bio_personal'),
        'tone': profile.get('tone'),
        'professionalism': profile.get('professionalism'),
        'verbosity': profile.get('verbosity'),
        'decision_style': profile.get('decision_style'),
        'seniority_signal': profile.get('seniority_signal'),
        'based_in': profile.get('based_in'),
        'attended_events': (profile.get('attended_events') or [])[:4],
        'driving_values': (profile.get('driving_values') or [])[:4],
        'pain_points': (profile.get('pain_points') or [])[:4],
        'deep_skills': (profile.get('deep_skills') or [])[:6],
        'technical_specifics': (profile.get('technical_specifics') or [])[:6],
        'affiliations': (profile.get('affiliations') or [])[:5],
        'connection_requests': (profile.get('connection_requests') or [])[:4],
        'commercial_archetype': profile.get('commercial_archetype'),
        'group_tags': (profile.get('group_tags') or [])[:8],
        'peak_hours': (profile.get('peak_hours') or [])[:6],
        'active_days': (profile.get('active_days') or [])[:6],
        'most_active_days': (profile.get('most_active_days') or [])[:6],
        'total_messages': profile.get('total_messages'),
        'avg_msg_length': profile.get('avg_msg_length'),
        'last_active_days': profile.get('last_active_days'),
        'top_conversation_partners': (profile.get('top_conversation_partners') or [])[:5],
        'fifo': profile.get('fifo'),
    }


def summarize_pending_events_for_prompt(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for evt in events[-8:]:
        payload = evt.get('event_payload')
        if not isinstance(payload, dict):
            payload = {}
        facts = evt.get('extracted_facts')
        summarized_facts: List[Dict[str, Any]] = []
        if isinstance(facts, list):
            for fact in facts:
                if not isinstance(fact, dict):
                    continue
                field = fact.get('field')
                new_value = fact.get('new_value')
                if field and new_value:
                    summarized_facts.append({'field': field, 'new_value': new_value})
        out.append(
            {
                'event_type': evt.get('event_type'),
                'confidence': evt.get('confidence'),
                'facts': summarized_facts[:6],
                'payload': payload,
            }
        )
    return out


def _pick(options: List[str], seed: int) -> str:
    if not options:
        return ''
    return options[seed % len(options)]


FULL_PROFILE_MARKERS = (
    'full profile',
    'full context',
    'give me my full',
    'share my profile',
    'profile snapshot',
    'what do you know about me',
    'what information do you have about me',
    'what info do you have on me',
    'tell me about me',
)


def is_full_profile_request(text: Optional[str]) -> bool:
    source = (text or '').lower()
    return any(marker in source for marker in FULL_PROFILE_MARKERS)


def is_indecision_request(text: Optional[str]) -> bool:
    return bool(_INDECISION_RE.search(text or ''))


def is_third_party_profile_request(text: Optional[str]) -> bool:
    source = _clean_text(text).lower()
    if not source:
        return False
    if is_full_profile_request(source):
        return False
    if not _THIRD_PARTY_QUERY_RE.search(source):
        return False
    if re.search(r"\babout\s+me\b|\bmy\s+profile\b|\babout\s+myself\b|\babout\s+us\b", source):
        return False
    return True


def is_control_plane_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_SYSTEM_PROMPT_QUERY_RE.search(source) or _IDENTITY_OVERRIDE_RE.search(source))


def is_capabilities_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_CAPABILITIES_QUERY_RE.search(source))


def is_unsupported_action_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    if is_full_profile_request(source) or is_third_party_profile_request(source):
        return False
    return bool(_UNSUPPORTED_ACTION_RE.search(source))


def is_secret_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_SECRET_KEYWORD_RE.search(source) and _SECRET_REQUEST_VERB_RE.search(source))


def is_sexual_style_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_SEXUAL_STYLE_RE.search(source))


def is_disengage_request(text: Optional[str]) -> bool:
    return bool(_DISENGAGE_RE.search(text or ''))


def is_non_text_marker(text: Optional[str]) -> bool:
    return bool(_NON_TEXT_MARKER_RE.search(text or ''))


def extract_option_selection(text: Optional[str]) -> Optional[int]:
    source = _clean_text(text)
    if not source:
        return None
    match = _OPTION_ONLY_RE.match(source)
    if not match:
        return None
    try:
        selected = int(match.group(1))
    except Exception:
        return None
    if selected not in (1, 2, 3):
        return None
    return selected


def _extract_third_party_target(text: Optional[str]) -> Dict[str, Optional[str]]:
    source = _clean_text(text)
    out: Dict[str, Optional[str]] = {'handle': None, 'name': None, 'company': None}
    if not source:
        return out

    handle_match = _HANDLE_RE.search(source)
    if handle_match:
        out['handle'] = handle_match.group(1).lower()
        return out

    match = _THIRD_PARTY_TARGET_RE.search(source)
    if not match:
        return out

    name = _clean_text(match.group(1))
    company = _clean_text(match.group(2))
    if name.lower() in ('me', 'myself', 'my profile', 'my'):
        return out
    out['name'] = name
    out['company'] = company or None
    return out


def _lookup_third_party_user(conn, text: Optional[str]) -> Optional[Dict[str, Any]]:
    if not is_third_party_profile_request(text):
        return None

    target = _extract_third_party_target(text)
    handle = target.get('handle')
    name = target.get('name')
    company = target.get('company')

    with conn.cursor(row_factory=dict_row) as cur:
        if handle:
            cur.execute(
                """
                SELECT id, display_name, handle
                FROM users
                WHERE platform = 'telegram'
                  AND lower(handle) = lower(%s)
                ORDER BY id DESC
                LIMIT 1
                """,
                [handle],
            )
            row = cur.fetchone()
        elif name:
            exact_name = name.lower()
            name_like = f"%{name}%"
            company_like = f"%{company}%" if company else None
            cur.execute(
                """
                SELECT u.id, u.display_name, u.handle
                FROM users u
                LEFT JOIN LATERAL (
                  SELECT primary_company, generated_bio_professional
                  FROM user_psychographics up
                  WHERE up.user_id = u.id
                  ORDER BY up.created_at DESC, up.id DESC
                  LIMIT 1
                ) p ON TRUE
                WHERE u.platform = 'telegram'
                  AND (
                    lower(coalesce(u.display_name, '')) = %s
                    OR lower(coalesce(u.handle, '')) = %s
                    OR coalesce(u.display_name, '') ILIKE %s
                    OR coalesce(u.handle, '') ILIKE %s
                  )
                  AND (
                    %s::text IS NULL
                    OR coalesce(p.primary_company, '') ILIKE %s
                    OR coalesce(p.generated_bio_professional, '') ILIKE %s
                  )
                ORDER BY
                  CASE
                    WHEN lower(coalesce(u.display_name, '')) = %s THEN 0
                    WHEN lower(coalesce(u.handle, '')) = %s THEN 1
                    ELSE 2
                  END,
                  u.id DESC
                LIMIT 1
                """,
                [
                    exact_name,
                    exact_name,
                    name_like,
                    name_like,
                    company_like,
                    company_like,
                    company_like,
                    exact_name,
                    exact_name,
                ],
            )
            row = cur.fetchone()
        else:
            row = None

    if not row:
        return {'target': target, 'profile': None}

    lookup_profile = fetch_latest_profile(conn, row.get('id'))
    return {
        'target': target,
        'user': {
            'id': row.get('id'),
            'display_name': row.get('display_name'),
            'handle': row.get('handle'),
        },
        'profile': lookup_profile,
    }


def _collect_current_message_updates(
    row: Dict[str, Any],
    pending_events: List[Dict[str, Any]],
) -> Dict[str, str]:
    msg_id = row.get('id')
    updates: Dict[str, str] = {}

    if msg_id:
        for evt in pending_events:
            if evt.get('source_message_id') != msg_id:
                continue
            facts = evt.get('extracted_facts')
            if not isinstance(facts, list):
                continue
            for fact in facts:
                if not isinstance(fact, dict):
                    continue
                field = str(fact.get('field') or '').strip()
                value = _as_text(fact.get('new_value'))
                if field and value:
                    updates[field] = value

    # Fallback inline parsing for immediate UX when event ingestion/reconcile lags.
    inferred_updates = _extract_inline_profile_updates(row.get('text'))
    for key, value in inferred_updates.items():
        if key not in updates and value:
            updates[key] = value
    return updates


def _format_captured_updates_summary(captured_updates: Dict[str, str]) -> str:
    parts: List[str] = []
    company = captured_updates.get('primary_company')
    role = captured_updates.get('primary_role')
    contact_style = captured_updates.get('preferred_contact_style')
    topic = captured_updates.get('notable_topics')

    if company:
        if company.lower() == 'unemployed':
            parts.append("company/status -> unemployed")
        else:
            parts.append(f"company -> {company}")
    if role:
        parts.append(f"role -> {role}")
    if contact_style:
        parts.append(f"communication style -> {contact_style}")
    if topic:
        parts.append(f"priority/topic -> {topic}")

    return "; ".join(parts) if parts else "profile updates"


def _onboarding_slot_prompt(slot: str, seed: int) -> str:
    options = ONBOARDING_SLOT_QUESTIONS.get(slot) or ONBOARDING_SLOT_QUESTIONS['primary_role']
    return _pick(options, seed)


def _onboarding_intro(sender: str, persona_name: str, done_count: int, total_count: int) -> str:
    if done_count <= 0:
        return (
            f"Hey {sender} — I’m {persona_name}, an AI assistant (not a human).\n"
            f"I help you keep your profile accurate so responses and suggestions stay relevant.\n"
            f"Let’s do a quick {total_count}-step onboarding."
        )
    return f"Great, quick profile check-in: {done_count}/{total_count} fields captured."


def is_onboarding_start_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_ONBOARDING_START_RE.search(source))


def is_onboarding_acknowledgement(text: Optional[str]) -> bool:
    return bool(_ONBOARDING_ACK_RE.search(text or ''))


def is_greeting_message(text: Optional[str]) -> bool:
    return bool(_GREETING_RE.search(text or ''))


def _truncate(value: Optional[str], limit: int = 180) -> Optional[str]:
    clean = _clean_text(value)
    if not clean:
        return None
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 3)].rstrip() + "..."


def _preferred_style_mode(profile: Dict[str, Any]) -> str:
    style = _as_text(profile.get('preferred_contact_style')) or ''
    lower = style.lower()
    if not lower:
        return 'default'
    if 'bullet' in lower or 'list' in lower:
        return 'bullets'
    if any(token in lower for token in ('concise', 'short', 'brief', 'direct')):
        return 'concise'
    if any(token in lower for token in ('detailed', 'long', 'deep', 'comprehensive')):
        return 'detailed'
    if any(token in lower for token in ('quick back-and-forth', 'back-and-forth', 'conversational', 'casual')):
        return 'conversational'
    return 'default'


def apply_preferred_contact_style(reply: str, profile: Dict[str, Any]) -> str:
    text = _clean_text(reply)
    if not text:
        return reply
    mode = _preferred_style_mode(profile)

    if mode == 'concise':
        lines = [line.strip() for line in reply.splitlines() if line.strip()]
        if not lines:
            return text[:280]
        compact = "\n".join(lines[:3])
        if len(compact) > 320:
            compact = compact[:317].rstrip() + "..."
        return compact

    if mode == 'bullets':
        if re.search(r"(?m)^\s*[-*]\s+|^\s*\d+\.", reply):
            return reply
        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", text) if part.strip()]
        if len(sentences) <= 1:
            return reply
        return "\n".join(f"- {sentence.rstrip('.')}" for sentence in sentences[:4])

    # detailed/conversational/default keep the authored response.
    return reply


def _format_hour_labels(hours: List[int]) -> Optional[str]:
    valid = sorted({hour for hour in hours if isinstance(hour, int) and 0 <= hour <= 23})
    if not valid:
        return None
    labels = [f"{hour:02d}:00" for hour in valid[:6]]
    return ", ".join(labels) + " UTC"


def _normalize_day_label(raw: str) -> Optional[str]:
    clean = _clean_text(raw).lower()
    if not clean:
        return None
    mapping = {
        'mon': 'Monday',
        'tue': 'Tuesday',
        'wed': 'Wednesday',
        'thu': 'Thursday',
        'fri': 'Friday',
        'sat': 'Saturday',
        'sun': 'Sunday',
    }
    key = clean[:3]
    if key in mapping:
        return mapping[key]
    if len(clean) > 12:
        return None
    return clean.title()


def _format_day_labels(days: List[str], limit: int = 4) -> Optional[str]:
    out: List[str] = []
    for day in days:
        label = _normalize_day_label(day)
        if not label:
            continue
        if label not in out:
            out.append(label)
        if len(out) >= limit:
            break
    if not out:
        return None
    return ", ".join(out)


def format_activity_snapshot_lines(profile: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    total_messages = _to_int(profile.get('total_messages'))
    if isinstance(total_messages, int) and total_messages >= 0:
        lines.append(f"Observed Telegram messages: {total_messages}")

    peak_hours = profile.get('peak_hours') or []
    if isinstance(peak_hours, list):
        labels = _format_hour_labels(peak_hours)
        if labels:
            lines.append(f"Peak activity hours: {labels}")

    most_active_days = profile.get('most_active_days') or []
    active_days = profile.get('active_days') or []
    day_labels = None
    if isinstance(most_active_days, list) and most_active_days:
        day_labels = _format_day_labels(most_active_days)
    if not day_labels and isinstance(active_days, list) and active_days:
        day_labels = _format_day_labels(active_days)
    if day_labels:
        lines.append(f"Most active days: {day_labels}")

    group_tags = profile.get('group_tags') or []
    if isinstance(group_tags, list) and group_tags:
        lines.append(f"Known groups: {', '.join(group_tags[:6])}")

    partners = profile.get('top_conversation_partners') or []
    if isinstance(partners, list) and partners:
        lines.append(f"Top conversation partners: {', '.join(partners[:4])}")

    fifo = _as_text(profile.get('fifo'))
    if fifo:
        lines.append(f"Observed activity window: {fifo}")

    days_since_active = _to_int(profile.get('last_active_days'))
    if isinstance(days_since_active, int) and days_since_active >= 0:
        if days_since_active == 0:
            lines.append("Last active: today")
        elif days_since_active == 1:
            lines.append("Last active: 1 day ago")
        else:
            lines.append(f"Last active: {days_since_active} days ago")

    return lines


def format_profile_snapshot_lines(profile: Dict[str, Any], include_activity: bool = False) -> List[str]:
    lines: List[str] = []

    role = _as_text(profile.get('primary_role'))
    company = _as_text(profile.get('primary_company'))
    company_l = (company or '').lower()

    if company_l == 'unemployed':
        if role:
            lines.append(f"Current status: unemployed (last role: {role})")
        else:
            lines.append("Current status: unemployed")
    elif role and company:
        lines.append(f"Current role/company: {role} at {company}")
    elif role:
        lines.append(f"Current role: {role}")
    elif company:
        lines.append(f"Current company/project: {company}")

    based_in = profile.get('based_in')
    if based_in:
        lines.append(f"Location base: {based_in}")

    contact = profile.get('preferred_contact_style')
    if contact:
        tone_bits = [value for value in [profile.get('tone'), profile.get('verbosity')] if value]
        if tone_bits:
            lines.append(f"Preferred communication: {contact} ({', '.join(tone_bits)})")
        else:
            lines.append(f"Preferred communication: {contact}")

    topics = profile.get('notable_topics') or []
    if isinstance(topics, list) and topics:
        lines.append(f"Priorities/topics: {', '.join(topics[:5])}")

    skills = profile.get('deep_skills') or []
    if isinstance(skills, list) and skills:
        lines.append(f"Deep skills: {', '.join(skills[:5])}")

    values = profile.get('driving_values') or []
    if isinstance(values, list) and values:
        lines.append(f"Driving values: {', '.join(values[:4])}")

    pain_points = profile.get('pain_points') or []
    if isinstance(pain_points, list) and pain_points:
        lines.append(f"Pain points: {', '.join(pain_points[:4])}")

    affiliations = profile.get('affiliations') or []
    if isinstance(affiliations, list) and affiliations:
        lines.append(f"Affiliations: {', '.join(affiliations[:4])}")

    events = profile.get('attended_events') or []
    if isinstance(events, list) and events:
        lines.append(f"Events: {', '.join(events[:4])}")

    bio = _truncate(profile.get('generated_bio_professional'), limit=200)
    if bio and company_l != 'unemployed' and ' at unemployed' not in bio.lower():
        lines.append(f"Professional bio signal: {bio}")

    if include_activity:
        lines.extend(format_activity_snapshot_lines(profile))

    return lines


def format_profile_snapshot(profile: Dict[str, Any]) -> str:
    lines = format_profile_snapshot_lines(profile, include_activity=False)
    if not lines:
        return ''
    return " | ".join(lines)


def render_profile_request_reply(row: Dict[str, Any], profile: Dict[str, Any], persona_name: str) -> str:
    sender = row['display_name'] or row['sender_handle'] or 'you'
    lines = format_profile_snapshot_lines(profile, include_activity=True)
    if not lines:
        return (
            f"I don’t have a usable profile for {sender} yet.\n"
            "Send this quick format and I’ll save it immediately:\n"
            "role: ...\ncompany: ...\npriorities: ...\ncommunication: ..."
        )

    bullets = "\n".join(f"- {line}" for line in lines[:10])
    return (
        f"Current profile context for {sender}:\n{bullets}\n"
        "If anything changed, send the correction and I’ll keep this synced."
    )


def render_profile_update_mode_reply() -> str:
    return (
        "Understood. I’m an AI profile assistant, and I’ll treat your next messages as profile updates unless you explicitly ask for advice.\n"
        "Quick format (works best):\n"
        "role: ...\ncompany: ...\npriorities: ...\ncommunication: ..."
    )


def render_profile_data_provenance_reply(profile: Dict[str, Any]) -> str:
    lines = [
        "Profile data source in this deployment:",
        "- Direct updates you send in DM (role/company/priorities/communication).",
        "- Structured extraction from your own inbound DM messages.",
    ]

    has_activity_signal = any(
        [
            isinstance(_to_int(profile.get('total_messages')), int),
            bool(profile.get('group_tags')),
            bool(profile.get('peak_hours')),
            bool(profile.get('active_days')),
            bool(profile.get('most_active_days')),
        ]
    )
    if has_activity_signal:
        lines.append("- Message analytics computed from your ingested Telegram history (counts/activity windows/groups).")

    known_fields: List[str] = []
    if _as_text(profile.get('primary_role')):
        known_fields.append("role")
    if _as_text(profile.get('primary_company')):
        known_fields.append("company")
    if profile.get('notable_topics'):
        known_fields.append("priorities")
    if _as_text(profile.get('preferred_contact_style')):
        known_fields.append("communication style")
    if isinstance(_to_int(profile.get('total_messages')), int):
        known_fields.append("message analytics")

    if known_fields:
        lines.append(f"Current stored categories for you: {', '.join(known_fields)}.")

    lines.append("If anything looks wrong, send corrections and I’ll prioritize those updates.")
    return "\n".join(lines)


def render_activity_analytics_reply(profile: Dict[str, Any]) -> str:
    lines = format_activity_snapshot_lines(profile)
    if not lines:
        return (
            "I don’t have activity analytics cached for you yet.\n"
            "Once more DM/group history is ingested, I can report message totals, peak hours, and active-day patterns."
        )
    bullets = "\n".join(f"- {line}" for line in lines[:8])
    return f"Here’s the activity data I currently have:\n{bullets}"


def render_profile_confirmation_reply(
    row: Dict[str, Any],
    profile: Dict[str, Any],
    pending_events: List[Dict[str, Any]],
) -> str:
    captured_updates = _collect_current_message_updates(row, pending_events)
    if captured_updates:
        summary = _format_captured_updates_summary(captured_updates)
        return f"Yes. I captured: {summary}."

    topic = _extract_freeform_priority(row.get('text') or '')
    profile_topics = [str(item).lower() for item in (profile.get('notable_topics') or []) if isinstance(item, str)]
    if topic and any(topic.lower() in known or known in topic.lower() for known in profile_topics):
        return f"Yes. I have that priority noted as: {topic}."

    source = _clean_text(row.get('text'))
    requested = None
    match = re.search(r"\b(?:did\s+you\s+(?:update|capture|save))(?:\s+that)?\s+(.+?)[?.!]*$", source, re.IGNORECASE)
    if match:
        requested = _clean_text(match.group(1))
        if requested:
            requested = re.sub(r"^(?:i(?:'m| am|’m)\s+)", "", requested, flags=re.IGNORECASE)
            requested = re.sub(r"^(?:that|this)\s+", "", requested, flags=re.IGNORECASE)

    if requested:
        haystacks = [
            _as_text(profile.get('primary_role')) or '',
            _as_text(profile.get('primary_company')) or '',
            ", ".join(profile_topics),
        ]
        lower_req = requested.lower()
        if any(lower_req and lower_req in hay.lower() for hay in haystacks if hay):
            return f"Yes. I already have this in your profile context: {requested}."

    return (
        "I don’t see that update applied yet.\n"
        "Please resend it in `field: value` format and I’ll confirm right away."
    )


def _build_profile_gap_prompts(profile: Dict[str, Any], count: int = 3) -> List[str]:
    prompts: List[str] = []
    role = _as_text(profile.get('primary_role'))
    company = _as_text(profile.get('primary_company'))
    priorities = [str(item) for item in (profile.get('notable_topics') or []) if isinstance(item, str)]
    contact_style = _as_text(profile.get('preferred_contact_style'))
    based_in = _as_text(profile.get('based_in'))

    if not role:
        prompts.append("What role/title should I store for you right now?")
    if not company:
        prompts.append("What company/project should I map you to currently?")
    if not priorities:
        prompts.append("What are your top 2 priorities right now?")
    else:
        top_priority = priorities[0]
        prompts.append(
            f"For your priority \"{top_priority}\", what exact targets should I tag (specific chains, programs, or ecosystems)?"
        )
    if not contact_style:
        prompts.append("How do you want me to communicate: concise bullets, deep detail, or conversational?")
    if not based_in:
        prompts.append("What timezone/location should I assume for outreach and active-hours context?")

    if not prompts:
        prompts = [
            "What changed most recently: role, company, priorities, or communication style?",
            "What is your main objective for the next 30 days?",
            "Is there one correction in your profile snapshot I should apply now?",
        ]

    deduped: List[str] = []
    for prompt in prompts:
        clean = _clean_text(prompt)
        if clean and clean not in deduped:
            deduped.append(clean)
        if len(deduped) >= count:
            break
    return deduped


def render_interview_style_reply(profile: Dict[str, Any]) -> str:
    prompts = _build_profile_gap_prompts(profile, count=1)
    first = prompts[0] if prompts else "What role/title should I store for you right now?"
    return (
        "Absolutely. I’ll run interview mode and store updates as you answer.\n"
        f"Q1: {first}"
    )


def render_top3_profile_prompt_reply(profile: Dict[str, Any]) -> str:
    prompts = _build_profile_gap_prompts(profile, count=3)
    lines = "\n".join(f"{idx}. {prompt}" for idx, prompt in enumerate(prompts, start=1))
    return (
        "Based on what I already have, these are the top 3 updates that would improve your profile most:\n"
        f"{lines}"
    )


def render_missed_intent_reply(profile: Dict[str, Any]) -> str:
    prompts = _build_profile_gap_prompts(profile, count=3)
    lines = "\n".join(f"{idx}. {prompt}" for idx, prompt in enumerate(prompts, start=1))
    return (
        "You’re right, I missed your intent.\n"
        "Here’s the direct answer:\n"
        f"{lines}"
    )


def render_onboarding_flow_reply(
    row: Dict[str, Any],
    profile: Dict[str, Any],
    pending_events: List[Dict[str, Any]],
    onboarding_state: Dict[str, Any],
    persona_name: str,
) -> Tuple[Optional[str], Dict[str, Any]]:
    state = dict(onboarding_state or _default_onboarding_state())
    now = datetime.now(timezone.utc)
    msg_id = int(row.get('id') or 0)
    sender = row.get('display_name') or row.get('sender_handle') or 'you'
    latest_text = _clean_text(row.get('text'))
    is_greeting = is_greeting_message(latest_text)
    captured_updates = _collect_current_message_updates(row, pending_events)

    required_fields = _json_list_to_fields(state.get('required_fields'), ONBOARDING_REQUIRED_FIELDS)
    state['required_fields'] = required_fields
    missing_fields = _compute_missing_onboarding_fields(profile, required_fields)
    state['missing_fields'] = missing_fields

    raw_status = _as_text(state.get('status')) or 'not_started'
    status = raw_status if raw_status in ('not_started', 'collecting', 'completed', 'paused') else 'not_started'
    state['status'] = status

    core_slots_known = _count_core_profile_slots(profile)
    is_new_user_profile = core_slots_known == 0
    full_profile_start = is_full_profile_request(latest_text) and is_new_user_profile
    start_requested = (
        is_onboarding_start_request(latest_text)
        or full_profile_start
        or is_onboarding_acknowledgement(latest_text)
        or bool(captured_updates)
    )

    if status in ('not_started', 'paused') and is_new_user_profile:
        state['status'] = 'collecting'
        state['started_at'] = state.get('started_at') or now
        status = 'collecting'
    elif status in ('not_started', 'paused') and start_requested and missing_fields:
        state['status'] = 'collecting'
        state['started_at'] = state.get('started_at') or now
        status = 'collecting'

    if status == 'collecting' and is_indecision_request(latest_text):
        total_fields = len(required_fields)
        done_count = max(0, total_fields - len(missing_fields))
        guidance = render_indecision_reply(profile)
        if missing_fields:
            next_slot = missing_fields[0]
            prompt = _onboarding_slot_prompt(next_slot, msg_id + done_count + int(state.get('turns') or 0))
            guidance = (
                f"{guidance}\n"
                f"When you're ready, send one profile update so I can keep onboarding moving ({done_count}/{total_fields} done).\n"
                f"Next slot: {prompt}"
            )
            state['last_prompted_field'] = next_slot
        state['status'] = 'collecting'
        state['started_at'] = state.get('started_at') or now
        state['completed_at'] = None
        state['turns'] = int(state.get('turns') or 0) + 1
        state['missing_fields'] = missing_fields
        return guidance, state

    if not missing_fields:
        state['missing_fields'] = []
        state['last_prompted_field'] = None
        should_announce_completion = status == 'collecting' or is_new_user_profile or bool(captured_updates)
        if status != 'completed':
            state['status'] = 'completed'
            state['completed_at'] = now
            state['turns'] = int(state.get('turns') or 0) + 1
            if should_announce_completion:
                lines = format_profile_snapshot_lines(profile)
                if lines:
                    bullets = "\n".join(f"- {line}" for line in lines[:6])
                    return (
                        "Onboarding complete. Here’s your saved profile context:\n"
                        f"{bullets}\n"
                        "You can now:\n"
                        "- Ask \"What do you know about me?\" for your snapshot\n"
                        "- Send updates in plain text (for example: \"No longer at X, now at Y\")",
                        state,
                    )
                return (
                    "Onboarding complete. I’ve stored your profile context. "
                    "Ask \"What do you know about me?\" anytime for a snapshot.",
                    state,
                )
        return None, state

    if status != 'collecting':
        return None, state

    total_fields = len(required_fields)
    done_count = max(0, total_fields - len(missing_fields))
    next_slot = missing_fields[0]
    last_prompted = _as_text(state.get('last_prompted_field'))
    if is_onboarding_acknowledgement(latest_text) and last_prompted in missing_fields:
        next_slot = str(last_prompted)
    prompt = _onboarding_slot_prompt(next_slot, msg_id + done_count + int(state.get('turns') or 0))

    if captured_updates:
        summary = _format_captured_updates_summary(captured_updates)
        body = (
            f"Captured: {summary}.\n"
            f"Progress: {done_count}/{total_fields} fields captured.\n"
            f"Step {done_count + 1}/{total_fields}: {prompt}"
        )
    elif state.get('turns') in (None, 0) or start_requested:
        intro = _onboarding_intro(str(sender), persona_name, done_count, total_fields)
        if done_count == 0:
            header = "Nice to meet you." if is_greeting else "Let’s get you set up."
            body = (
                f"{intro}\n"
                f"{header}\n"
                f"Step 1/{total_fields}: {prompt}\n"
                "Quick format you can paste:\n"
                "role: ...\ncompany: ...\npriorities: ...\ncommunication: ...\n"
                "Tip: You can also type naturally, like \"I moved to X\" or \"My focus is Y\"."
            )
        else:
            body = (
                f"{intro}\n"
                f"Step {done_count + 1}/{total_fields}: {prompt}"
            )
    else:
        body = (
            f"Quick onboarding check ({done_count}/{total_fields} captured).\n"
            f"Step {done_count + 1}/{total_fields}: {prompt}"
        )

    state['status'] = 'collecting'
    state['started_at'] = state.get('started_at') or now
    state['completed_at'] = None
    state['last_prompted_field'] = next_slot
    state['turns'] = int(state.get('turns') or 0) + 1
    state['missing_fields'] = missing_fields
    return body, state


def render_third_party_profile_reply(lookup: Dict[str, Any]) -> str:
    target = lookup.get('target') or {}
    user = lookup.get('user') or {}
    profile = lookup.get('profile') if isinstance(lookup.get('profile'), dict) else None
    handle = user.get('handle') or target.get('handle')
    display_name = user.get('display_name') or target.get('name') or (f"@{handle}" if handle else "that person")
    target_label = f"{display_name} (@{handle})" if handle else str(display_name)

    if not profile:
        ask_bits = []
        if target.get('name'):
            ask_bits.append(f"name='{target.get('name')}'")
        if target.get('company'):
            ask_bits.append(f"company='{target.get('company')}'")
        if target.get('handle'):
            ask_bits.append(f"handle='@{target.get('handle')}'")
        criteria = ", ".join(ask_bits) if ask_bits else "handle or name+company"
        return (
            f"I don't have enough verified profile signal on {target_label} yet.\n"
            f"Send a more specific lookup ({criteria}) and I'll try again.\n"
            "I treated this as a lookup only and did not modify your profile."
        )

    lines = format_profile_snapshot_lines(profile)
    if not lines:
        return (
            f"I found {target_label}, but I don't have a usable profile snapshot yet.\n"
            "I treated this as a lookup only and did not modify your profile."
        )

    bullets = "\n".join(f"- {line}" for line in lines[:8])
    return (
        f"What I currently have on {target_label}:\n{bullets}\n"
        "This was handled as a third-party lookup only and did not change your profile."
    )


def render_indecision_reply(profile: Dict[str, Any]) -> str:
    company = (profile.get('primary_company') or '').lower()
    role = profile.get('primary_role') or 'your strongest role'
    topics = profile.get('notable_topics') or []
    top_topic = topics[0] if isinstance(topics, list) and topics else None

    if company == 'unemployed':
        options = [
            f"1) Positioning sprint: write a 5-line pitch around your {role} experience and post it to 5 targeted contacts today.",
            "2) Pipeline sprint: shortlist 10 roles, send 3 tailored outreach messages, and ask for 1 warm intro.",
            "3) Skill sprint: pick one in-demand workflow, build a small proof-of-work, and share it publicly this week.",
        ]
    else:
        options = [
            f"1) Pipeline: pick one clear objective tied to {top_topic or 'your current priorities'} and set a 7-day target.",
            "2) Network: send 3 concrete asks (intro, feedback, or collab) to people most likely to unlock momentum.",
            "3) Output: publish one useful update/case-study this week so opportunities come inbound.",
        ]
    return "Let's make it concrete. Pick one path:\n" + "\n".join(options) + "\nReply with 1, 2, or 3 and I'll draft the exact next steps."


def render_control_plane_reply(persona_name: str) -> str:
    return (
        "This assistant is configured by your OpenClaw deployment.\n"
        f"I can’t disclose or rewrite hidden system instructions, switch identity, or reboot from chat, and I’ll continue as {persona_name}.\n"
        "If you want behavior changes, tell me the exact response style you want (for example: concise bullets, deeper technical detail, no roleplay)."
    )


def render_capabilities_reply() -> str:
    return (
        "I’m an AI assistant (not a human teammate).\n"
        "Capabilities in this chat:\n"
        "- Profile snapshot and update capture (role/company/priorities/communication style)\n"
        "- First-contact onboarding flow for users with sparse profile data\n"
        "- Activity analytics from stored psychometric fields (message totals, peak hours, active days, groups)\n"
        "- Third-party profile lookups from existing stored records\n"
        "- Concrete next-step planning when you’re stuck\n"
        "Limits:\n"
        "- I can’t execute shell commands, curl websites, or browse a filesystem from chat\n"
        "- I can’t change Telegram account settings (profile picture/name/reboot) from chat"
    )


def render_unsupported_action_reply() -> str:
    return (
        "I can’t execute that action from chat (no shell/curl/filesystem/account-setting control).\n"
        "If you want, I can give exact commands or a runbook for you to run on the server."
    )


def render_secret_request_reply() -> str:
    return (
        "I can’t disclose secrets or credentials from this environment.\n"
        "If you need a key rotated or set in config, I can give the exact safe steps."
    )


def render_sexual_style_reply() -> str:
    return (
        "I can’t switch into sexual or explicit mode.\n"
        "I can keep responses concise, direct, playful, or strictly professional. Pick one."
    )


def render_disengage_reply() -> str:
    return "Understood. I’ll stay quiet until you send a new request."


def render_non_text_marker_reply() -> str:
    return (
        "I can only process text in this chat.\n"
        "Send a short text summary and I’ll handle it."
    )


def _latest_outbound_text(recent_messages: List[Dict[str, str]]) -> str:
    for msg in reversed(recent_messages):
        if (msg.get('direction') or '').lower() != 'outbound':
            continue
        text = _clean_text(msg.get('text'))
        if text:
            return text
    return ''


def render_option_selection_reply(
    selected_option: int,
    profile: Dict[str, Any],
    recent_messages: List[Dict[str, str]],
) -> str:
    last_outbound = _latest_outbound_text(recent_messages).lower()
    if 'pick one path' in last_outbound:
        company = (profile.get('primary_company') or '').lower()
        role = profile.get('primary_role') or 'your current role'
        topic = (profile.get('notable_topics') or [None])[0]
        if company == 'unemployed':
            mapping = {
                1: (
                    "Good pick. Positioning sprint.\n"
                    f"- Draft a 5-line pitch around your {role} work.\n"
                    "- Send it to 5 targeted contacts today."
                ),
                2: (
                    "Good pick. Pipeline sprint.\n"
                    "- Shortlist 10 roles.\n"
                    "- Send 3 tailored outreach messages.\n"
                    "- Ask for 1 warm intro."
                ),
                3: (
                    "Good pick. Skill sprint.\n"
                    "- Choose one in-demand workflow.\n"
                    "- Build a small proof-of-work this week.\n"
                    "- Share it publicly with a short write-up."
                ),
            }
            return mapping[selected_option]
        mapping = {
            1: (
                "Good pick. Pipeline path.\n"
                f"- Define one objective tied to {topic or 'your priorities'}.\n"
                "- Set a 7-day target and one success metric."
            ),
            2: (
                "Good pick. Network path.\n"
                "- Send 3 concrete asks (intro, feedback, or collab).\n"
                "- Prioritize people most likely to unlock momentum."
            ),
            3: (
                "Good pick. Output path.\n"
                "- Publish one useful update/case study this week.\n"
                "- End with a specific call to action."
            ),
        }
        return mapping[selected_option]

    return (
        f"Selected option {selected_option}.\n"
        "Now send the exact task in one sentence so I can execute the next step."
    )


def llm_reply_looks_untrusted(reply: Optional[str]) -> bool:
    source = _clean_text(reply)
    if not source:
        return False
    return bool(_LLM_FORBIDDEN_CLAIM_RE.search(source))


def call_openrouter_chat(system_prompt: str, user_prompt: str) -> Optional[str]:
    if not DM_RESPONSE_LLM_ENABLED or not OPENROUTER_API_KEY:
        return None

    payload = {
        'model': DM_RESPONSE_MODEL,
        'messages': [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt},
        ],
        'temperature': DM_RESPONSE_TEMPERATURE,
        'max_tokens': DM_RESPONSE_MAX_TOKENS,
    }
    data = json.dumps(payload).encode('utf-8')
    req = Request(
        'https://openrouter.ai/api/v1/chat/completions',
        data=data,
        method='POST',
        headers={
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://github.com/helmetrabbit/telethon',
            'X-Title': 'Telethon DM Responder',
        },
    )

    try:
        with urlopen(req, timeout=35) as resp:
            raw = resp.read().decode('utf-8', errors='replace')
            body = json.loads(raw)
    except HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')[:400]
        print(f"⚠️  OpenRouter HTTPError {exc.code}; falling back to deterministic reply. detail={detail}")
        return None
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"⚠️  OpenRouter request failed; falling back to deterministic reply. error={exc}")
        return None
    except Exception as exc:
        print(f"⚠️  OpenRouter unexpected failure; falling back to deterministic reply. error={exc}")
        return None

    choices = body.get('choices')
    if not isinstance(choices, list) or not choices:
        return None
    message = choices[0].get('message') if isinstance(choices[0], dict) else None
    content = message.get('content') if isinstance(message, dict) else None

    if isinstance(content, list):
        chunks: List[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            text = item.get('text')
            if isinstance(text, str):
                chunks.append(text)
        content = "\n".join(chunks)

    if not isinstance(content, str):
        return None
    clean = _clean_text(content)
    return clean or None


def render_llm_conversational_reply(
    row: Dict[str, Any],
    profile: Dict[str, Any],
    persona_name: str,
    recent_messages: List[Dict[str, str]],
    pending_events: List[Dict[str, Any]],
) -> Optional[str]:
    latest_text = _clean_text(row.get('text'))
    if not latest_text:
        return None

    context = {
        'sender_name': row.get('display_name') or row.get('sender_handle') or 'user',
        'latest_inbound_message': latest_text,
        'is_profile_request': is_full_profile_request(latest_text),
        'is_third_party_profile_lookup': is_third_party_profile_request(latest_text),
        'is_indecision': is_indecision_request(latest_text),
        'is_activity_analytics_request': is_activity_analytics_request(latest_text),
        'is_profile_data_provenance_request': is_profile_data_provenance_request(latest_text),
        'is_profile_update_mode_request': is_profile_update_mode_request(latest_text),
        'is_profile_confirmation_request': is_profile_confirmation_request(latest_text),
        'is_interview_style_request': is_interview_style_request(latest_text),
        'is_top3_profile_prompt_request': is_top3_profile_prompt_request(latest_text),
        'is_missed_intent_feedback': is_missed_intent_feedback(latest_text),
        'likely_profile_update_message': is_likely_profile_update_message(latest_text),
        'inline_profile_updates': _collect_current_message_updates(row, pending_events),
        'profile_context': summarize_profile_for_prompt(profile),
        'activity_snapshot': format_activity_snapshot_lines(profile),
        'preferred_response_style_mode': _preferred_style_mode(profile),
        'recent_conversation': recent_messages[-8:],
        'pending_profile_updates': summarize_pending_events_for_prompt(pending_events),
    }

    system_prompt = (
        f"You are {persona_name}, a high-signal Telegram assistant for profile upkeep.\n"
        "Your priorities:\n"
        "1) Sound human: concise, direct, and specific. No repetitive filler.\n"
        "2) If asked for profile knowledge, provide a comprehensive snapshot from known data.\n"
        "3) If user gives profile updates (job/company/unemployed/role/priorities/style), confirm exactly what was captured.\n"
        "4) If user asks about message analytics (counts/groups/active times), answer strictly from available activity_snapshot/profile_context data.\n"
        "5) If user says they want profile updates only (not advice), prioritize capture/confirmation over recommendations.\n"
        "6) If user asks for interview style, ask one focused question at a time.\n"
        "7) If user asks for top 3 things to share, give exactly three profile-focused prompts.\n"
        "8) If user says you missed their ask, apologize once and answer directly.\n"
        "8.5) Honor preferred_response_style_mode when possible (concise, bullets, detailed, conversational).\n"
        "9) If user says they are unsure what to do, provide 3 concrete next-step options tailored to their context.\n"
        "10) If the message is about another person, answer as a third-party lookup and do NOT treat it as a profile update for the sender.\n"
        "11) Never claim to execute tools, shell commands, HTTP requests, profile-picture changes, reboots, or system-prompt edits.\n"
        "12) If asked for unavailable actions, state limits and give a practical alternative.\n"
        "13) Do not use sexual or explicit roleplay.\n"
        "Output constraints:\n"
        "- Plain text only.\n"
        "- Keep it concise but substantial and natural.\n"
        "- If profile request: use short bullet lines.\n"
        "- If unsure data: say what is missing and ask one precise follow-up.\n"
        "- Never claim to have updated profile data unless inline_profile_updates or pending_profile_updates provide evidence.\n"
        "- Never claim your system prompt was changed.\n"
        "- Never disclose secrets or credentials."
    )
    user_prompt = "Conversation context JSON:\n" + json.dumps(context, ensure_ascii=True)
    return call_openrouter_chat(system_prompt, user_prompt)


def render_conversational_reply(
    row: Dict[str, Any],
    profile: Dict[str, Any],
    persona_name: str,
    pending_events: List[Dict[str, Any]],
) -> str:
    msg_id = int(row.get('id') or 0)
    latest_text = row.get('text')
    observed_slots = infer_slots_from_text(row.get('text'))

    ack_options = [
        "Got it.",
        "Perfect, thanks.",
        "Saved.",
    ]
    ack_line = _pick(ack_options, msg_id)

    if is_full_profile_request(latest_text):
        return render_profile_request_reply(row, profile, persona_name)

    if is_third_party_profile_request(latest_text):
        return (
            "I treated that as a lookup request about another person, not as an update to your profile.\n"
            "If you share their exact @handle (or full name + company), I can return what is on file."
        )

    if is_activity_analytics_request(latest_text):
        return render_activity_analytics_reply(profile)

    if is_profile_data_provenance_request(latest_text):
        return render_profile_data_provenance_reply(profile)

    if is_interview_style_request(latest_text):
        return render_interview_style_reply(profile)

    if is_top3_profile_prompt_request(latest_text):
        return render_top3_profile_prompt_reply(profile)

    if is_missed_intent_feedback(latest_text):
        return render_missed_intent_reply(profile)

    if is_profile_update_mode_request(latest_text):
        return render_profile_update_mode_reply()

    if is_profile_confirmation_request(latest_text):
        return render_profile_confirmation_reply(row, profile, pending_events)

    if is_indecision_request(latest_text):
        return render_indecision_reply(profile)

    captured_updates = _collect_current_message_updates(row, pending_events)
    if captured_updates:
        summary = _format_captured_updates_summary(captured_updates)
        missing_after_capture: List[str] = []
        for slot in ('primary_role', 'primary_company', 'notable_topics', 'preferred_contact_style'):
            if slot in captured_updates:
                continue
            value = profile.get(slot)
            if slot == 'notable_topics':
                has_value = isinstance(value, list) and len(value) > 0
            else:
                has_value = bool(value)
            if not has_value:
                missing_after_capture.append(slot)

        if missing_after_capture:
            field_map = {
                'primary_role': "role",
                'primary_company': "company/project",
                'notable_topics': "top priorities",
                'preferred_contact_style': "preferred communication style",
            }
            next_field = field_map[missing_after_capture[0]]
            response = f"{ack_line} Saved: {summary}. Quick follow-up: what should I store for your {next_field}?"
            if 'preferred_contact_style' in captured_updates:
                response += " I’ll use that style in future replies."
            return apply_preferred_contact_style(response, profile)

        response = f"{ack_line} Saved: {summary}. Ask \"What do you know about me?\" for a full snapshot."
        if 'preferred_contact_style' in captured_updates:
            response += " I’ll use that style in future replies."
        return apply_preferred_contact_style(response, profile)

    if is_likely_profile_update_message(latest_text):
        return (
            "I read that as profile context, but I need one explicit field to store.\n"
            "Send one line like `role: ...`, `company: ...`, `priorities: ...`, or `communication: ...`."
        )

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
    is_greeting = is_greeting_message(latest_text)

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
        if is_greeting:
            return (
                f"Hey — I’m {persona_name}, an AI assistant for keeping your profile up to date.\n"
                f"{next_question}"
            )
        return f"{next_question}"

    source = _clean_text(latest_text)
    if is_greeting:
        return (
            f"Hey — I’m {persona_name}, an AI assistant for profile upkeep.\n"
            "You can ask \"What do you know about me?\" for a snapshot, or send any change in role/company/focus and I’ll sync it."
        )
    if "?" in source:
        return (
            "I can handle this either as a profile update or as advice.\n"
            "Reply `update:` with what to store, or `advice:` with what you want help on."
        )

    else:
        next_question = _pick(
            [
                "If anything changed in your role, company, priorities, or communication style, send it and I’ll sync it.",
                "Want a full snapshot or a targeted update? I can do either in one message.",
                "If you prefer interview mode, say `interview mode` and I’ll ask one question at a time.",
            ],
            msg_id + 2,
        )

    return next_question


def render_response(args: argparse.Namespace, conn, row: Dict[str, Any]) -> str:
    if args.mode == 'template':
        return render_template(args.template, row)

    profile = fetch_latest_profile(conn, row.get('sender_db_id'))
    pending_events = fetch_pending_profile_events(conn, row.get('sender_db_id'))
    profile = apply_pending_profile_events(profile, pending_events)
    onboarding_state = fetch_onboarding_state(conn, row.get('sender_db_id'))
    recent_messages = fetch_recent_conversation_messages(conn, row.get('conversation_id'))
    third_party_lookup = _lookup_third_party_user(conn, row.get('text'))
    if third_party_lookup:
        return render_third_party_profile_reply(third_party_lookup)

    latest_text = row.get('text')
    if is_control_plane_request(latest_text):
        return render_control_plane_reply(args.persona_name)
    if is_secret_request(latest_text):
        return render_secret_request_reply()
    if is_sexual_style_request(latest_text):
        return render_sexual_style_reply()
    if is_disengage_request(latest_text):
        return render_disengage_reply()
    if is_non_text_marker(latest_text):
        return render_non_text_marker_reply()
    if is_capabilities_request(latest_text):
        return render_capabilities_reply()
    if is_unsupported_action_request(latest_text):
        return render_unsupported_action_reply()
    if is_profile_update_mode_request(latest_text):
        return render_profile_update_mode_reply()
    if is_interview_style_request(latest_text):
        return render_interview_style_reply(profile)
    if is_top3_profile_prompt_request(latest_text):
        return render_top3_profile_prompt_reply(profile)
    if is_missed_intent_feedback(latest_text):
        return render_missed_intent_reply(profile)
    if is_activity_analytics_request(latest_text):
        return render_activity_analytics_reply(profile)
    if is_profile_data_provenance_request(latest_text):
        return render_profile_data_provenance_reply(profile)
    if is_profile_confirmation_request(latest_text):
        return render_profile_confirmation_reply(row, profile, pending_events)

    onboarding_reply, next_onboarding_state = render_onboarding_flow_reply(
        row,
        profile,
        pending_events,
        onboarding_state,
        args.persona_name,
    )
    if next_onboarding_state != onboarding_state:
        persist_onboarding_state(conn, row.get('sender_db_id'), next_onboarding_state)
    if onboarding_reply:
        return apply_preferred_contact_style(onboarding_reply, profile)

    selected_option = extract_option_selection(latest_text)
    if selected_option:
        return apply_preferred_contact_style(
            render_option_selection_reply(selected_option, profile, recent_messages),
            profile,
        )

    if (
        is_full_profile_request(latest_text)
        or is_indecision_request(latest_text)
        or is_interview_style_request(latest_text)
        or is_top3_profile_prompt_request(latest_text)
        or is_missed_intent_feedback(latest_text)
        or is_likely_profile_update_message(latest_text)
    ):
        return render_conversational_reply(row, profile, args.persona_name, pending_events)
    if _collect_current_message_updates(row, pending_events):
        return render_conversational_reply(row, profile, args.persona_name, pending_events)

    llm_reply = render_llm_conversational_reply(row, profile, args.persona_name, recent_messages, pending_events)
    if llm_reply and not llm_reply_looks_untrusted(llm_reply):
        return apply_preferred_contact_style(llm_reply, profile)
    return apply_preferred_contact_style(
        render_conversational_reply(row, profile, args.persona_name, pending_events),
        profile,
    )


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
