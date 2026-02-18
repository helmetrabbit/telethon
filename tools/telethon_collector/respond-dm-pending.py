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
import time
from datetime import datetime, timedelta, timezone
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
_raw_dm_response_model = os.getenv('DM_RESPONSE_MODEL', 'deepseek/deepseek-chat').strip() or 'deepseek/deepseek-chat'
_dm_response_model_allowlist = [
    item.strip()
    for item in (os.getenv('DM_RESPONSE_MODEL_ALLOWLIST') or 'deepseek/deepseek-chat').split(',')
    if item.strip()
]
DM_ALLOW_MODEL_FALLBACK = (os.getenv('DM_ALLOW_MODEL_FALLBACK', '0').strip().lower() in ('1', 'true', 'yes', 'on'))
DM_RESPONSE_MODEL_ALLOWED = _raw_dm_response_model in _dm_response_model_allowlist
if not DM_RESPONSE_MODEL_ALLOWED:
    forced = _dm_response_model_allowlist[0] if _dm_response_model_allowlist else 'deepseek/deepseek-chat'
    if DM_ALLOW_MODEL_FALLBACK:
        print(f"⚠️  DM_RESPONSE_MODEL={_raw_dm_response_model!r} not allowed; forcing {forced!r}")
        DM_RESPONSE_MODEL = forced
    else:
        print(f"🚫 DM_RESPONSE_MODEL={_raw_dm_response_model!r} not allowed; disabling DM responder LLM. Set DM_ALLOW_MODEL_FALLBACK=1 to force fallback to {forced!r}.")
        DM_RESPONSE_MODEL = forced
else:
    DM_RESPONSE_MODEL = _raw_dm_response_model
DM_RESPONSE_LLM_ENABLED = (
    DM_RESPONSE_MODEL_ALLOWED
    and (os.getenv('DM_RESPONSE_LLM_ENABLED', '1').strip().lower() not in ('0', 'false', 'no', 'off'))
)
DM_RESPONSE_LLM_STRATEGY = (os.getenv('DM_RESPONSE_LLM_STRATEGY') or 'auto').strip().lower()  # auto|always|never


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
DM_RESPONSE_LLM_AUTO_MIN_CHARS = max(20, _env_int('DM_RESPONSE_LLM_AUTO_MIN_CHARS', 120))
DM_OPENROUTER_DAILY_COST_CAP_USD = max(0.0, _env_float('DM_OPENROUTER_DAILY_COST_CAP_USD', 0.0))
DM_OPENROUTER_SPEND_STATE_FILE = (os.getenv('DM_OPENROUTER_SPEND_STATE_FILE') or str(_ROOT_DIR / 'data' / '.state' / 'openrouter_spend.json')).strip()
DM_OPENROUTER_SPEND_LOCK_FILE = (os.getenv('DM_OPENROUTER_SPEND_LOCK_FILE') or f"{DM_OPENROUTER_SPEND_STATE_FILE}.lock").strip()
DM_OPENROUTER_SPEND_LOCK_TIMEOUT_MS = max(250, _env_int('DM_OPENROUTER_SPEND_LOCK_TIMEOUT_MS', 2000))
DM_CONTACT_STYLE_TTL_DAYS = max(1, _env_int('DM_CONTACT_STYLE_TTL_DAYS', 45))
DM_CONTACT_STYLE_RECONFIRM_COOLDOWN_DAYS = max(1, _env_int('DM_CONTACT_STYLE_RECONFIRM_COOLDOWN_DAYS', 14))
DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD = min(1.0, max(0.0, _env_float('DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD', 0.8)))
_configured_confirm_threshold = _env_float('DM_CONTACT_STYLE_CONFIRM_THRESHOLD', 0.55)
DM_CONTACT_STYLE_CONFIRM_THRESHOLD = min(
    DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD,
    max(0.0, _configured_confirm_threshold),
)
# UX controls
DM_UI_GREETING_MENU_COOLDOWN_DAYS = max(0, _env_int('DM_UI_GREETING_MENU_COOLDOWN_DAYS', 7))
DM_UI_HOME_MENU_TTL_SECONDS = max(60, _env_int('DM_UI_HOME_MENU_TTL_SECONDS', 600))
STYLE_CONFLICT_RESOLUTION_RULE = 'confidence_gated_last_write_wins'

_PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")
_INDECISION_RE = re.compile(
    r"\b(?:idk|i\s+don'?t\s+know|not\s+sure\s+what\s+to\s+do|what\s+should\s+i(?:\s+do)?|any\s+advice|help\s+me\s+choose)\b",
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
    r"\b(?:what\s+skills\s+do\s+you\s+have|what\s+can\s+you\s+do|your\s+capabilities|"
    r"who\s+are\s+you|"
    r"what\s+is\s+this\s+chat\s+for|what\s+is\s+this\s+for|what\s+do\s+you\s+do|"
    r"what\s+is\s+this\s+used\s+for|for\s+what\s+purpose|"
    r"where\s+does\s+(?:this|my)\s+data\s+go|"
    r"what\s+is\s+(?:this\s+bot|this\s+assistant|this\s+ai|lobster\s+llama)|"
    r"what\s+is\s+(?:this|my)\s+profile\s+for|what\s+is\s+(?:this|my)\s+profile\s+used\s+for|"
    r"how\s+is\s+(?:this|my)\s+profile\s+(?:being\s+)?used|how\s+will\s+(?:this|my)\s+(?:profile|data)\s+be\s+used|"
    r"why\s+are\s+you\s+(?:messaging|message(?:ing)?|dm(?:ing)?|contacting)\s+me|"
    r"why\s+did\s+you\s+(?:message|dm|reach\s+out)(?:\s+to\s+me)?|"
    r"(?:is\s+this\s+a\s+scam|you\s+sound\s+like\s+(?:a\s+)?scam|u\s+sound\s+like\s+(?:a\s+)?scam)|"
    r"can\s+i\s+(?:ask\s+about|look\s+up|query)\s+other\s+(?:users|people|members)|"
    r"what\s+can\s+i\s+use\s+this\s+for|how\s+does\s+this\s+work|how\s+do\s+i\s+use\s+this|"
    r"what\s+is\s+the\s+process|interview\s+process|"
    r"what\s+is\s+the\s+purpose|purpose\s+of\s+this|"
    r"i'?m\s+confused|this\s+is\s+confusing|"
    r"no\s+idea\s+what\s+that\s+means|what\s+do\s+you\s+mean|what\s+does\s+(?:this|that)\s+mean|"
    r"why\s+am\s+i\s+doing\s+this|what\s+does\s+this\s+do)\b",
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
_QUESTION_LIKE_RE = re.compile(
    r"^\s*(?:"
    r"(?:what|who|where|when|why|how)\s+(?:is|are|was|were|do|does|did|can|could|would|should|will|may|might|about|else|other|many|much)\b"
    r"|(?:can|could|would|should|do|does|did|is|are|am|will|may|might)\b"
    r"|tell\s+me\b"
    r"|explain\b"
    r")",
    re.IGNORECASE,
)
_HELP_RE = re.compile(
    r"\b(?:help|menu|commands?|"
    r"start\s+here|get\s+started|getting\s+started|"
    r"how\s+do\s+i\s+(?:use|start)|how\s+to\s+(?:use|start)|"
    r"what\s+can\s+i\s+(?:say|ask)|"
    r"list\s+of\s+(?:commands?|starting\s+commands?))\b",
    re.IGNORECASE,
)
_HOME_RE = re.compile(
    r"^\s*(?:home|quick\s*start|quickstart)\s*$",
    re.IGNORECASE,
)
_FEEDBACK_RE = re.compile(
    r"^\s*(feedback|idea|feature|bug|request)\s*:\s*(.{3,2000})\s*$",
    re.IGNORECASE,
)
_IMPLICIT_FEEDBACK_RE = re.compile(
    r"\b(?:"
    r"confusing|confused|clunky|fragmented|breaking\s+the\s+flow|"
    r"you\s+asked\s+me\s+the\s+same|same\s+question|same\s+questions|again|repeat|repeating|"
    r"feature\s+request|feature|bug|broken|"
    r"wasn'?t\s+explained|no\s+explan(?:ation|ations?)|"
    r"cut\s+off|ends?\s+abruptly|"
    r"it\s+would\s+be\s+helpful|would\s+be\s+helpful|"
    r"it\s+would\s+be\s+neat|would\s+be\s+neat|"
    r"we\s+should|we\s+want|should\s+probably|"
    r"add\s+(?:a\s+)?function|collect\s+feedback"
    r")\b",
    re.IGNORECASE,
)
_THIRD_PARTY_EDIT_POLICY_RE = re.compile(
    r"\b(?:am\s+i\s+allowed\s+to|can\s+i|allowed\s+to)\s+(?:modify|edit|change|update)\s+"
    r"(?:another|other|someone\s+else(?:'s)?|a)\s+(?:users?|people|persons?)\b.{0,20}\bprofiles?\b|"
    r"\b(?:modify|edit|change|update)\s+(?:another|other|someone\s+else(?:'s)?|a)\s+(?:users?|people|persons?)\b.{0,20}\bprofiles?\b",
    re.IGNORECASE,
)
_THIRD_PARTY_LOOKUP_STORAGE_RE = re.compile(
    r"\b(?:do\s+you\s+(?:store|save|keep)|are\s+you\s+storing|does\s+this\s+store)\b.{0,80}\b(?:ask\s+about|look\s+up|query)\b.{0,40}\b(?:another|other)\b.{0,20}\b(?:users?|people|persons?)\b|"
    r"\b(?:do\s+you\s+store|does\s+it\s+store)\b.{0,40}\b(?:info|information|data)\b.{0,40}\b(?:about|on)\b.{0,12}\b(?:them|another\s+user|other\s+users?)\b",
    re.IGNORECASE,
)
_MORE_PROFILE_INFO_RE = re.compile(
    r"^\s*(?:what\s+else|anything\s+else|more)\s*[?.!]*\s*$",
    re.IGNORECASE,
)
_GROUP_POPULAR_TIME_RE = re.compile(
    r"\b(?:most\s+popular\s+time|peak\s+time|peak\s+hours?)\b.{0,120}\b(?:in|inside)\b.{0,60}\b(?:group|chat)\b",
    re.IGNORECASE,
)
_ONBOARDING_START_RE = re.compile(
    r"\b(?:onboard|onboarding|set\s+up\s+my\s+profile|setup\s+my\s+profile|initialize\s+my\s+profile|update\s+my\s+profile|"
    r"pretend\s+it'?s\s+my\s+first\s+message|pretend\s+this\s+is\s+my\s+first\s+message|"
    r"first\s+message|new\s+here|start\s+from\s+scratch|reset\s+onboarding)\b",
    re.IGNORECASE,
)
_ONBOARDING_ACK_RE = re.compile(
    r"^\s*(?:yes|yep|yeah|sure|ok|okay|start|go\s+ahead|lets\s+go|let's\s+go)\s*[.!?]*\s*$",
    re.IGNORECASE,
)
_STYLE_CONFIRM_YES_RE = re.compile(
    r"^\s*(?:yes|yep|yeah|sure|ok|okay|do\s+it|switch|confirm|go\s+ahead|sounds\s+good|works)\s*[.!?]*\s*$",
    re.IGNORECASE,
)
_STYLE_CONFIRM_NO_RE = re.compile(
    r"^\s*(?:no|nah|nope|don'?t|do\s+not|keep(?:\s+(?:it|current|current\s+style))?)\s*[.!?]*\s*$",
    re.IGNORECASE,
)
_GREETING_RE = re.compile(
    r"^\s*(?:hi|hello|hey|yo|gm|gn|good\s+(?:morning|afternoon|evening)|what'?s\s+up|sup)"
    r"(?:\s+\w{1,16}){0,2}\b[!. ]*$",
    re.IGNORECASE,
)
_PROFILE_UPDATE_MODE_RE = re.compile(
    r"\b(?:i\s+was\s+giving\s+you\s+info\s+to\s+update\s+my\s+profile|focus\s+(?:only|solely)\s+on\s+profile\s+updates?|"
    r"not\s+for\s+(?:advice|recommendations?)|no\s+advice\s+unless\s+i\s+ask|just\s+update\s+my\s+profile|"
    r"not\s+my\s+personal\s+assistant|don'?t\s+be\s+my\s+personal\s+assistant|"
    r"just\s+be\s+the\s+data\s+layer|data\s+layer\s+only|just\s+the\s+data|"
    r"only\s+capture\s+updates|only\s+store\s+updates|profile\s+only|no\s+action\s+support)\b",
    re.IGNORECASE,
)
_PROFILE_DATA_PROVENANCE_RE = re.compile(
    r"\b(?:where\s+(?:does|did)\s+(?:this|the|my)\s+data\s+come\s+from|"
    r"where\s+did\s+(?:this|the|my)\s+(?:info|information)\s+come\s+from|"
    r"where\s+did\s+you\s+get\s+(?:this|that)\s+(?:information|info|data)\s+from|"
    r"where\s+did\s+you\s+get\s+this\s+from|"
    r"how\s+did\s+you\s+get\s+this\s+(?:information|info|data)|"
    r"how\s+do\s+you\s+know\s+(?:this|that)|"
    r"data\s+source(?:s)?|source\s+of\s+this)\b",
    re.IGNORECASE,
)
_PROFILE_DATA_INVENTORY_RE = re.compile(
    r"\b(?:what\s+(?:other\s+)?data\s+do\s+you\s+have(?:\s+on\s+me)?|"
    r"is\s+there\s+(?:any\s+)?other\s+data\s+(?:on|about)\s+me|"
    r"do\s+you\s+have\s+(?:any\s+)?other\s+data\s+(?:on|about)\s+me|"
    r"what\s+else\s+do\s+you\s+have\s+(?:on|about)\s+me)\b",
    re.IGNORECASE,
)
_ACTIVITY_ANALYTICS_RE = re.compile(
    r"\b(?:how\s+many\s+messages\s+have\s+i\s+sent|message\s+count|total\s+messages?|most\s+active\s+(?:time|times|day|days)|"
    r"peak\s+hours?|active\s+hours?|popular\s+times?|when\s+am\s+i\s+most\s+active|what\s+groups?\b|which\s+groups?\b|"
    r"what\s+groups?\s+am\s+i\s+in|groups?\s+i'?m\s+in|"
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
    r"i(?:'m| am|’m)\s+pursuing|currently\s+pursuing|right\s+now\s+i(?:'m| am|’m)\s+pursuing|"
    r"i(?:'m| am|’m)\s+focused\s+on|currently\s+focused\s+on|my\s+current\s+focus\s+is|current\s+focus\s+is|"
    r"my\s+priorities?\s+(?:are|is))\s+([^.!?\n]{3,180})",
    re.IGNORECASE,
)
_FEEDBACKY_TOPIC_RE = re.compile(
    r"\b(?:"
    r"onboarding|"
    r"you\s+asked|asked\s+me|same\s+question|same\s+questions|again|repeat|repeating|stop\s+asking|"
    r"no\s+idea\s+what\s+that\s+means|i\s+want\s+to\s+know\s+what|what\s+do\s+you\s+mean|what\s+does\s+(?:this|that)\s+mean|"
    r"i'?m\s+confused|this\s+is\s+confusing|fragmented|clunky|"
    r"what\s+is\s+the\s+process|why\s+am\s+i\s+doing\s+this|what\s+does\s+this\s+do"
    r")\b",
    re.IGNORECASE,
)
_CONTACT_STYLE_KEYWORD_RE = re.compile(
    r"\b(?:concise|short|brief|detailed|long|deep|bullet(?:s)?|quick\s+back-and-forth|back-and-forth|"
    r"conversational|casual|chatty|normal|direct|formal|professional|playful|technical)\b",
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


def looks_like_feedback_topic(value: str) -> bool:
    clean = _clean_text(value).strip(" .,!?:;\"'`")
    if not clean:
        return False
    if "?" in clean:
        return True
    if _FEEDBACKY_TOPIC_RE.search(clean):
        return True
    # Long sentence-like blobs without separators are usually UX/intent feedback, not a topic list.
    has_sep = bool(re.search(r",|;|\n|\band\b|&", clean, flags=re.IGNORECASE))
    if not has_sep and len(clean.split()) > 14:
        return True
    return False


def sanitize_notable_topics(value: Any, max_items: int = 10) -> List[str]:
    raw = _to_string_list(value, max_items=max_items * 3)
    out: List[str] = []
    seen: Set[str] = set()
    for item in raw:
        clean = _clean_text(item).strip(" .,!?:;\"'`")
        if not clean:
            continue
        if looks_like_feedback_topic(clean):
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(clean)
        if len(out) >= max_items:
            break
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


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    if isinstance(value, str):
        clean = value.strip()
        if not clean:
            return None
        try:
            return float(clean)
        except Exception:
            return None
    return None


def _to_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith('Z'):
        raw = raw[:-1] + '+00:00'
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


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
_DM_FEEDBACK_TABLE_AVAILABLE: Optional[bool] = None
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


def _default_contact_style_state() -> Dict[str, Any]:
    return {
        'preferred_contact_style': None,
        'updated_at': None,
        'confidence': None,
        'source': None,
        'source_message_id': None,
        'resolution_rule': STYLE_CONFLICT_RESOLUTION_RULE,
        'reconfirm_prompted_at': None,
        'pending_candidate': None,
        'history': [],
    }


def _parse_contact_style_state_from_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    state = _default_contact_style_state()
    if not isinstance(snapshot, dict):
        return state
    style = snapshot.get('style_preference')
    if not isinstance(style, dict):
        return state

    state['preferred_contact_style'] = _as_text(style.get('value'))
    state['updated_at'] = _to_datetime(style.get('updated_at'))
    state['confidence'] = _to_float(style.get('confidence'))
    state['source'] = _as_text(style.get('source'))
    state['source_message_id'] = _to_int(style.get('source_message_id'))
    state['reconfirm_prompted_at'] = _to_datetime(style.get('reconfirm_prompted_at'))
    resolution_rule = _as_text(style.get('resolution_rule'))
    if resolution_rule:
        state['resolution_rule'] = resolution_rule

    pending = style.get('pending_candidate')
    if isinstance(pending, dict):
        pending_value = _as_text(pending.get('value'))
        if pending_value:
            state['pending_candidate'] = {
                'value': pending_value,
                'confidence': _to_float(pending.get('confidence')),
                'source': _as_text(pending.get('source')),
                'source_message_id': _to_int(pending.get('source_message_id')),
                'source_event_id': _as_text(pending.get('source_event_id')),
                'proposed_at': _to_datetime(pending.get('proposed_at')),
            }

    history = style.get('history')
    if isinstance(history, list):
        cleaned_history: List[Dict[str, Any]] = []
        for item in history[-12:]:
            if not isinstance(item, dict):
                continue
            value = _as_text(item.get('value'))
            if not value:
                continue
            cleaned_history.append(
                {
                    'value': value,
                    'updated_at': _to_datetime(item.get('updated_at')),
                    'confidence': _to_float(item.get('confidence')),
                    'source': _as_text(item.get('source')),
                    'source_message_id': _to_int(item.get('source_message_id')),
                }
            )
        state['history'] = cleaned_history
    return state


def _default_ui_preferences() -> Dict[str, Any]:
    # greeting_menu: quickstart|help|off
    return {
        'greeting_menu': 'quickstart',
        'greeting_menu_cooldown_days': DM_UI_GREETING_MENU_COOLDOWN_DAYS,
    }


def _parse_ui_preferences_from_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    prefs = _default_ui_preferences()
    if not isinstance(snapshot, dict):
        return prefs
    raw = snapshot.get('ui_preferences')
    if not isinstance(raw, dict):
        return prefs

    greeting_menu = _as_text(raw.get('greeting_menu'))
    if greeting_menu in ('quickstart', 'help', 'off'):
        prefs['greeting_menu'] = greeting_menu

    cooldown_days = _to_int(raw.get('greeting_menu_cooldown_days'))
    if cooldown_days is not None:
        prefs['greeting_menu_cooldown_days'] = max(0, min(30, cooldown_days))

    return prefs


def persist_ui_preferences(conn, sender_db_id: Optional[int], patch: Dict[str, Any]) -> Dict[str, Any]:
    if not sender_db_id:
        return _default_ui_preferences()
    snapshot = _fetch_profile_snapshot(conn, sender_db_id)
    current = _parse_ui_preferences_from_snapshot(snapshot)
    merged = dict(current)
    for key in ('greeting_menu', 'greeting_menu_cooldown_days'):
        if key not in patch:
            continue
        if key == 'greeting_menu':
            value = _as_text(patch.get(key))
            if value in ('quickstart', 'help', 'off'):
                merged[key] = value
        else:
            value = _to_int(patch.get(key))
            if value is not None:
                merged[key] = max(0, min(30, int(value)))

    snapshot['ui_preferences'] = merged
    _persist_profile_snapshot(conn, sender_db_id, snapshot)
    return merged


def _default_ui_state() -> Dict[str, Any]:
    return {
        'menu': None,  # {'type': 'home', 'sent_at': iso, 'expires_at': iso}
        'last_greeting_menu_at': None,
    }


def _parse_ui_state_from_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    state = _default_ui_state()
    if not isinstance(snapshot, dict):
        return state
    raw = snapshot.get('ui_state')
    if not isinstance(raw, dict):
        return state

    menu = raw.get('menu')
    if isinstance(menu, dict):
        menu_type = _as_text(menu.get('type'))
        sent_at = _to_datetime(menu.get('sent_at'))
        expires_at = _to_datetime(menu.get('expires_at'))
        if menu_type and isinstance(sent_at, datetime) and isinstance(expires_at, datetime):
            state['menu'] = {'type': menu_type, 'sent_at': sent_at, 'expires_at': expires_at}

    state['last_greeting_menu_at'] = _to_datetime(raw.get('last_greeting_menu_at'))
    return state


def persist_ui_state(conn, sender_db_id: Optional[int], ui_state_payload: Dict[str, Any]) -> Dict[str, Any]:
    if not sender_db_id:
        return _default_ui_state()
    snapshot = _fetch_profile_snapshot(conn, sender_db_id)
    snapshot['ui_state'] = ui_state_payload
    _persist_profile_snapshot(conn, sender_db_id, snapshot)
    return _parse_ui_state_from_snapshot(snapshot)


def clear_ui_menu(conn, sender_db_id: Optional[int], ui_state: Dict[str, Any]) -> Dict[str, Any]:
    if not sender_db_id:
        return ui_state
    payload = {
        'menu': None,
        'last_greeting_menu_at': (
            ui_state.get('last_greeting_menu_at').isoformat()
            if isinstance(ui_state.get('last_greeting_menu_at'), datetime)
            else None
        ),
    }
    return persist_ui_state(conn, sender_db_id, payload)


def _ui_menu_payload(menu_type: str, *, now: datetime) -> Dict[str, Any]:
    return {
        'type': menu_type,
        'sent_at': now.isoformat(),
        'expires_at': (now + timedelta(seconds=DM_UI_HOME_MENU_TTL_SECONDS)).isoformat(),
    }


def ui_menu_is_active(ui_state: Dict[str, Any], *, expected_type: str) -> bool:
    menu = ui_state.get('menu')
    if not isinstance(menu, dict):
        return False
    if _as_text(menu.get('type')) != expected_type:
        return False
    expires_at = menu.get('expires_at')
    if not isinstance(expires_at, datetime):
        expires_at = _to_datetime(expires_at)
    if not isinstance(expires_at, datetime):
        return False
    return datetime.now(timezone.utc) < expires_at


def _fetch_profile_snapshot(conn, sender_db_id: Optional[int]) -> Dict[str, Any]:
    if not sender_db_id:
        return {}
    available_columns = _fetch_dm_profile_state_columns(conn)
    if 'snapshot' not in available_columns:
        return {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT snapshot
            FROM dm_profile_state
            WHERE user_id = %s
            LIMIT 1
            """,
            [sender_db_id],
        )
        row = cur.fetchone()
    snapshot = row.get('snapshot') if row else None
    return snapshot if isinstance(snapshot, dict) else {}


def _persist_profile_snapshot(conn, sender_db_id: Optional[int], snapshot: Dict[str, Any]) -> None:
    if not sender_db_id:
        return
    available_columns = _fetch_dm_profile_state_columns(conn)
    if 'snapshot' not in available_columns:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dm_profile_state (user_id, snapshot)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (user_id)
            DO UPDATE SET
              snapshot = EXCLUDED.snapshot,
              updated_at = now()
            """,
            [sender_db_id, json.dumps(snapshot, ensure_ascii=True)],
        )


def dm_feedback_table_available(conn) -> bool:
    global _DM_FEEDBACK_TABLE_AVAILABLE
    if _DM_FEEDBACK_TABLE_AVAILABLE is not None:
        return bool(_DM_FEEDBACK_TABLE_AVAILABLE)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = 'dm_feedback'
                LIMIT 1
                """,
            )
            _DM_FEEDBACK_TABLE_AVAILABLE = cur.fetchone() is not None
    except Exception:
        _DM_FEEDBACK_TABLE_AVAILABLE = False
    return bool(_DM_FEEDBACK_TABLE_AVAILABLE)


def persist_feedback(
    conn,
    *,
    row: Dict[str, Any],
    sender_db_id: Optional[int],
    kind: str,
    body: str,
) -> None:
    if not sender_db_id:
        return

    conversation_id = row.get('conversation_id')
    source_message_id = _to_int(row.get('id'))
    source_external_message_id = _as_text(row.get('external_message_id'))

    if dm_feedback_table_available(conn):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dm_feedback (
                  user_id,
                  conversation_id,
                  source_message_id,
                  source_external_message_id,
                  kind,
                  text
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    sender_db_id,
                    conversation_id,
                    source_message_id,
                    source_external_message_id,
                    kind[:32],
                    body[:4000],
                ],
            )
        return

    # Fallback: append to a local log file (works even before DB migration is applied).
    log_path = _ROOT_DIR / 'data' / 'logs' / 'dm-feedback.log'
    log_path.parent.mkdir(parents=True, exist_ok=True)
    event = {
        'ts': datetime.now(timezone.utc).isoformat(),
        'user_id': sender_db_id,
        'conversation_id': conversation_id,
        'source_message_id': source_message_id,
        'source_external_message_id': source_external_message_id,
        'kind': kind,
        'text': body,
    }
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(event, ensure_ascii=True) + "\n")


def fetch_contact_style_state(conn, sender_db_id: Optional[int]) -> Dict[str, Any]:
    snapshot = _fetch_profile_snapshot(conn, sender_db_id)
    return _parse_contact_style_state_from_snapshot(snapshot)


def persist_contact_style_state(
    conn,
    sender_db_id: Optional[int],
    style_value: Optional[str],
    *,
    source: str,
    confidence: Optional[float],
    source_message_id: Optional[int],
    observed_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    normalized_style = _as_text(style_value)
    if not sender_db_id or not normalized_style:
        return fetch_contact_style_state(conn, sender_db_id)

    snapshot = _fetch_profile_snapshot(conn, sender_db_id)
    style = snapshot.get('style_preference') if isinstance(snapshot.get('style_preference'), dict) else {}
    previous_value = _as_text(style.get('value'))
    now = observed_at or datetime.now(timezone.utc)
    now_iso = now.isoformat()

    history = style.get('history') if isinstance(style.get('history'), list) else []
    entry = {
        'value': normalized_style,
        'updated_at': now_iso,
        'confidence': float(confidence) if confidence is not None else None,
        'source': source,
        'source_message_id': source_message_id,
    }
    history.append(entry)
    style['history'] = history[-12:]

    style['value'] = normalized_style
    style['updated_at'] = now_iso
    style['confidence'] = float(confidence) if confidence is not None else style.get('confidence')
    style['source'] = source
    style['source_message_id'] = source_message_id
    style['resolution_rule'] = STYLE_CONFLICT_RESOLUTION_RULE
    style['pending_candidate'] = None
    if previous_value and previous_value.lower() != normalized_style.lower():
        # Style changed, so prompt can be asked again in the future after TTL.
        style['reconfirm_prompted_at'] = None

    snapshot['style_preference'] = style
    _persist_profile_snapshot(conn, sender_db_id, snapshot)
    return _parse_contact_style_state_from_snapshot(snapshot)


def mark_contact_style_reconfirm_prompted(
    conn,
    sender_db_id: Optional[int],
    prompted_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    if not sender_db_id:
        return _default_contact_style_state()
    snapshot = _fetch_profile_snapshot(conn, sender_db_id)
    style = snapshot.get('style_preference')
    if not isinstance(style, dict):
        return _parse_contact_style_state_from_snapshot(snapshot)
    style['reconfirm_prompted_at'] = (prompted_at or datetime.now(timezone.utc)).isoformat()
    snapshot['style_preference'] = style
    _persist_profile_snapshot(conn, sender_db_id, snapshot)
    return _parse_contact_style_state_from_snapshot(snapshot)


def persist_pending_contact_style_candidate(
    conn,
    sender_db_id: Optional[int],
    style_value: Optional[str],
    *,
    source: str,
    confidence: Optional[float],
    source_message_id: Optional[int],
    proposed_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    normalized_style = _as_text(style_value)
    if not sender_db_id or not normalized_style:
        return fetch_contact_style_state(conn, sender_db_id)

    snapshot = _fetch_profile_snapshot(conn, sender_db_id)
    style = snapshot.get('style_preference') if isinstance(snapshot.get('style_preference'), dict) else {}
    style['resolution_rule'] = STYLE_CONFLICT_RESOLUTION_RULE
    style['pending_candidate'] = {
        'value': normalized_style,
        'confidence': float(confidence) if confidence is not None else None,
        'source': source,
        'source_message_id': source_message_id,
        'proposed_at': (proposed_at or datetime.now(timezone.utc)).isoformat(),
    }
    snapshot['style_preference'] = style
    _persist_profile_snapshot(conn, sender_db_id, snapshot)
    return _parse_contact_style_state_from_snapshot(snapshot)


def clear_pending_contact_style_candidate(
    conn,
    sender_db_id: Optional[int],
) -> Dict[str, Any]:
    if not sender_db_id:
        return _default_contact_style_state()
    snapshot = _fetch_profile_snapshot(conn, sender_db_id)
    style = snapshot.get('style_preference')
    if not isinstance(style, dict):
        return _parse_contact_style_state_from_snapshot(snapshot)
    style['pending_candidate'] = None
    snapshot['style_preference'] = style
    _persist_profile_snapshot(conn, sender_db_id, snapshot)
    return _parse_contact_style_state_from_snapshot(snapshot)


def merge_contact_style_state_into_profile(profile: Dict[str, Any], style_state: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(profile)
    preferred_style = _as_text(style_state.get('preferred_contact_style'))
    if preferred_style:
        merged['preferred_contact_style'] = preferred_style
    merged['contact_style_updated_at'] = style_state.get('updated_at')
    merged['contact_style_confidence'] = style_state.get('confidence')
    merged['contact_style_source'] = style_state.get('source')
    merged['contact_style_resolution_rule'] = style_state.get('resolution_rule') or STYLE_CONFLICT_RESOLUTION_RULE
    merged['contact_style_reconfirm_prompted_at'] = style_state.get('reconfirm_prompted_at')
    merged['contact_style_pending_candidate'] = style_state.get('pending_candidate')
    return merged


def _parse_profile_overrides_from_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    raw = snapshot.get('profile_overrides')
    if not isinstance(raw, dict):
        return {}
    overrides: Dict[str, Any] = {}
    role = _as_text(raw.get('primary_role'))
    company = _as_text(raw.get('primary_company'))
    topics = sanitize_notable_topics(raw.get('notable_topics'), max_items=10)
    if role:
        overrides['primary_role'] = role
    if company:
        overrides['primary_company'] = company
    if topics:
        overrides['notable_topics'] = topics
    return overrides


def merge_profile_overrides_into_profile(profile: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    if not overrides:
        return profile
    merged = dict(profile)
    role = _as_text(overrides.get('primary_role'))
    company = _as_text(overrides.get('primary_company'))
    topics = overrides.get('notable_topics')
    if role:
        merged['primary_role'] = role
    if company:
        merged['primary_company'] = company
    override_topics = sanitize_notable_topics(topics, max_items=10)
    if override_topics:
        base = sanitize_notable_topics(merged.get('notable_topics'), max_items=10)
        base_set = {item.lower() for item in base}
        for t in override_topics:
            key = t.lower()
            if key in base_set:
                continue
            base.append(t[:80])
            base_set.add(key)
            if len(base) >= 10:
                break
        merged['notable_topics'] = base[:10]
    return merged


def style_confidence_band(confidence: Optional[float]) -> str:
    value = 0.0 if confidence is None else float(confidence)
    if value >= DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD:
        return 'high'
    if value >= DM_CONTACT_STYLE_CONFIRM_THRESHOLD:
        return 'medium'
    return 'low'


def is_style_confirmation_yes(text: Optional[str]) -> bool:
    return bool(_STYLE_CONFIRM_YES_RE.search(text or ''))


def is_style_confirmation_no(text: Optional[str]) -> bool:
    return bool(_STYLE_CONFIRM_NO_RE.search(text or ''))


def render_style_confirmation_prompt(style_value: str, confidence: Optional[float]) -> str:
    band = style_confidence_band(confidence)
    if band == 'medium':
        prefix = f"Quick note: I can remember how you want me to write. I detected \"{style_value}\"."
    else:
        prefix = f"Quick note: I might have misread this as a style request (\"{style_value}\")."
    return f"{prefix} Want me to switch to that style? Reply yes or no."


def maybe_append_contact_style_reconfirm(
    reply: str,
    profile: Dict[str, Any],
    style_state: Dict[str, Any],
) -> Tuple[str, bool]:
    pending_candidate = style_state.get('pending_candidate')
    if isinstance(pending_candidate, dict) and _as_text(pending_candidate.get('value')):
        return reply, False

    preferred_style = _as_text(profile.get('preferred_contact_style'))
    if not preferred_style:
        return reply, False

    updated_at = style_state.get('updated_at')
    if not isinstance(updated_at, datetime):
        updated_at = _to_datetime(profile.get('contact_style_updated_at'))
    if not isinstance(updated_at, datetime):
        return reply, False

    now = datetime.now(timezone.utc)
    if now - updated_at < timedelta(days=DM_CONTACT_STYLE_TTL_DAYS):
        return reply, False

    reconfirm_prompted_at = style_state.get('reconfirm_prompted_at')
    if not isinstance(reconfirm_prompted_at, datetime):
        reconfirm_prompted_at = _to_datetime(profile.get('contact_style_reconfirm_prompted_at'))
    if isinstance(reconfirm_prompted_at, datetime) and (now - reconfirm_prompted_at < timedelta(days=DM_CONTACT_STYLE_RECONFIRM_COOLDOWN_DAYS)):
        return reply, False

    if "Quick style check:" in reply or "Reply yes or no." in reply:
        return reply, False

    appended = (
        f"{reply}\n"
        f"Quick style check: I have your preference as \"{preferred_style}\". "
        "Still good, or want to change it?"
    )
    return appended, True


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
        'contact_style_updated_at': None,
        'contact_style_confidence': None,
        'contact_style_source': None,
        'contact_style_resolution_rule': STYLE_CONFLICT_RESOLUTION_RULE,
        'contact_style_reconfirm_prompted_at': None,
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
        "pursuing",
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

    # Freeform self-updates (natural language) for role/company.
    if 'primary_role' not in updates or 'primary_company' not in updates:
        role_company_match = re.search(
            r"\bmy\s+(?:current\s+)?(?:role|title|job\s+title|position)\s+is\s+([^.!?\n]{2,120}?)"
            r"(?:\s+(?:and|&)\s+(?:the\s+)?(?:company|project|employer|org|organization)\s+is\s+([^.!?\n]{2,120}?))?"
            r"(?:[.!?\n]|$)",
            clean_source,
            re.IGNORECASE,
        )
        if role_company_match:
            role_value = _clean_text(role_company_match.group(1)).strip(" \"'`").strip(" .,!?:;")
            company_value = _clean_text(role_company_match.group(2) or '').strip(" \"'`").strip(" .,!?:;")
            if role_value and 'primary_role' not in updates:
                updates['primary_role'] = role_value[:120]
            if company_value and 'primary_company' not in updates:
                updates['primary_company'] = 'unemployed' if 'unemployed' in company_value.lower() else company_value[:120]

    if 'primary_company' not in updates:
        company_only_match = re.search(
            r"^(?:update\s*:\s*)?(?:my\s+)?(?:current\s+)?(?:company|project|employer|org|organization)\s+is\s+([^.!?\n]{2,120}?)(?:[.!?\n]|$)",
            clean_source,
            re.IGNORECASE,
        )
        if company_only_match:
            company_value = _clean_text(company_only_match.group(1)).strip(" \"'`").strip(" .,!?:;")
            if company_value and not re.match(r"^(?:launching|building|making|doing|working|growing|scaling|helping|assisting|supporting)\b", company_value, re.IGNORECASE):
                updates['primary_company'] = 'unemployed' if 'unemployed' in company_value.lower() else company_value[:120]

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
    if 'bullet' in source:
        return 'concise bullets'
    if any(token in source for token in ('concise', 'short', 'brief')):
        return 'concise'
    if any(token in source for token in ('detailed', 'long', 'deep')):
        return 'detailed'
    if any(token in source for token in ('quick back-and-forth', 'back-and-forth', 'conversational', 'casual', 'chatty', 'normal')):
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


def is_profile_data_inventory_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_PROFILE_DATA_INVENTORY_RE.search(source))


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
    # Prefer the "real" psychographic profile rows as a base.
    # DM reconciler rows can be sparse and should act like an overlay (see dm_profile_state.snapshot.profile_overrides).
    query = f"""
        SELECT {select_sql}
        FROM user_psychographics
        WHERE user_id = %s
          AND model_name != 'dm-event-reconciler'
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, [sender_db_id])
        row = cur.fetchone()

    if not row:
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
    profile['notable_topics'] = sanitize_notable_topics(row.get('notable_topics'), max_items=10)
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


def fetch_latest_dm_reconciler_overrides(conn, sender_db_id: Optional[int]) -> Dict[str, Any]:
    """Back-compat overlay for older deployments that wrote sparse DM reconciler rows into user_psychographics."""
    if not sender_db_id:
        return {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT primary_role, primary_company, preferred_contact_style, notable_topics
            FROM user_psychographics
            WHERE user_id = %s
              AND model_name = 'dm-event-reconciler'
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            [sender_db_id],
        )
        row = cur.fetchone()
    if not row:
        return {}
    overrides: Dict[str, Any] = {}
    role = _as_text(row.get('primary_role'))
    company = _as_text(row.get('primary_company'))
    if role:
        overrides['primary_role'] = role
    if company:
        overrides['primary_company'] = company
    topics = sanitize_notable_topics(row.get('notable_topics'), max_items=10)
    if topics:
        overrides['notable_topics'] = topics
    # preferred_contact_style is handled via dm_profile_state.snapshot.style_preference (confirmation-gated).
    return overrides


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
    topics = sanitize_notable_topics(merged.get('notable_topics'), max_items=10)
    topic_set = {item.lower() for item in topics}

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
                # Contact style is confirmation-gated via dm_profile_state.snapshot.style_preference.
                # Do not apply raw extracted facts here, or we can accidentally flip style before user confirms.
                continue
            elif field == 'notable_topics':
                if looks_like_feedback_topic(new_value):
                    continue
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
    def trim_list(values: Any, *, take: int, item_limit: int = 120) -> List[str]:
        if not isinstance(values, list):
            return []
        out: List[str] = []
        for raw in values:
            if raw is None:
                continue
            s = _truncate(str(raw), item_limit)
            if not s:
                continue
            out.append(s)
            if len(out) >= take:
                break
        return out

    return {
        'primary_role': _truncate(profile.get('primary_role'), 120),
        'primary_company': _truncate(profile.get('primary_company'), 120),
        'preferred_contact_style': _truncate(profile.get('preferred_contact_style'), 140),
        'notable_topics': trim_list(profile.get('notable_topics'), take=6, item_limit=80),
        'generated_bio_professional': _truncate(profile.get('generated_bio_professional'), 220),
        'generated_bio_personal': _truncate(profile.get('generated_bio_personal'), 220),
        'tone': _truncate(profile.get('tone'), 80),
        'professionalism': _truncate(profile.get('professionalism'), 80),
        'verbosity': _truncate(profile.get('verbosity'), 80),
        'decision_style': _truncate(profile.get('decision_style'), 80),
        'seniority_signal': _truncate(profile.get('seniority_signal'), 120),
        'based_in': _truncate(profile.get('based_in'), 120),
        'attended_events': trim_list(profile.get('attended_events'), take=4, item_limit=80),
        'driving_values': trim_list(profile.get('driving_values'), take=4, item_limit=80),
        'pain_points': trim_list(profile.get('pain_points'), take=4, item_limit=100),
        'deep_skills': trim_list(profile.get('deep_skills'), take=6, item_limit=90),
        'technical_specifics': trim_list(profile.get('technical_specifics'), take=6, item_limit=90),
        'affiliations': trim_list(profile.get('affiliations'), take=5, item_limit=90),
        'connection_requests': trim_list(profile.get('connection_requests'), take=4, item_limit=90),
        'commercial_archetype': _truncate(profile.get('commercial_archetype'), 120),
        'group_tags': trim_list(profile.get('group_tags'), take=8, item_limit=50),
        'peak_hours': trim_list(profile.get('peak_hours'), take=6, item_limit=16),
        'active_days': trim_list(profile.get('active_days'), take=6, item_limit=16),
        'most_active_days': trim_list(profile.get('most_active_days'), take=6, item_limit=16),
        'total_messages': profile.get('total_messages'),
        'avg_msg_length': profile.get('avg_msg_length'),
        'last_active_days': profile.get('last_active_days'),
        'top_conversation_partners': trim_list(profile.get('top_conversation_partners'), take=5, item_limit=80),
        'fifo': profile.get('fifo'),
        'contact_style_updated_at': (
            profile.get('contact_style_updated_at').isoformat()
            if isinstance(profile.get('contact_style_updated_at'), datetime)
            else profile.get('contact_style_updated_at')
        ),
        'contact_style_confidence': profile.get('contact_style_confidence'),
        'contact_style_source': profile.get('contact_style_source'),
        'contact_style_resolution_rule': profile.get('contact_style_resolution_rule'),
    }


def summarize_pending_events_for_prompt(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for evt in events[-8:]:
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


def is_help_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_HELP_RE.search(source))


def is_home_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_HOME_RE.search(source))


def parse_feedback_message(text: Optional[str]) -> Optional[Dict[str, str]]:
    source = _clean_text(text)
    if not source:
        return None
    match = _FEEDBACK_RE.match(source)
    if not match:
        return None
    kind = _clean_text(match.group(1)).lower()
    body = _clean_text(match.group(2))
    if not body:
        return None
    return {'kind': kind, 'body': body}


def parse_implicit_feedback_message(text: Optional[str]) -> Optional[Dict[str, str]]:
    source = _clean_text(text)
    if not source:
        return None
    if parse_feedback_message(source):
        return None
    if is_third_party_profile_request(source):
        return None
    # Don't treat real profile updates as feedback.
    if is_likely_profile_update_message(source):
        return None
    if _extract_inline_profile_updates(source):
        return None

    if not _IMPLICIT_FEEDBACK_RE.search(source):
        return None

    kind = 'feedback'
    if re.search(r"\b(bug|broken|repeat|repeating|again|same\s+question|cut\s+off|ends?\s+abruptly)\b", source, re.IGNORECASE):
        kind = 'bug'
    elif re.search(r"\b(feature|request|would\s+be\s+(?:helpful|neat)|it\s+would\s+be\s+(?:helpful|neat)|add\s+(?:a\s+)?function)\b", source, re.IGNORECASE):
        kind = 'feature'

    return {'kind': kind, 'body': source[:2000]}


def is_third_party_edit_policy_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_THIRD_PARTY_EDIT_POLICY_RE.search(source))


def is_third_party_lookup_storage_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_THIRD_PARTY_LOOKUP_STORAGE_RE.search(source))


def is_more_profile_info_request(text: Optional[str], recent_messages: List[Dict[str, str]]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    if not _MORE_PROFILE_INFO_RE.search(source):
        return False
    # Only treat this as "show more" if they recently requested their profile snapshot.
    for msg in reversed(recent_messages[-6:]):
        if msg.get('direction') == 'inbound' and is_full_profile_request(msg.get('text')):
            return True
    return False


def extract_group_query(text: Optional[str]) -> Optional[str]:
    source = _clean_text(text)
    if not source:
        return None
    # Prefer quoted group names.
    m = re.search(r"[\"“”']([^\"“”']{2,120})[\"“”']", source)
    if m:
        return _clean_text(m.group(1)).strip(" .,!?:;\"'`")[:120]
    # Fallback: take whatever comes after "group" if present.
    m = re.search(r"\bgroup\b\s*(?:named\s+)?(.{2,140})$", source, re.IGNORECASE)
    if m:
        return _clean_text(m.group(1)).strip(" .,!?:;\"'`")[:120]
    return None


def is_group_popular_time_request(text: Optional[str]) -> bool:
    source = _clean_text(text)
    if not source:
        return False
    return bool(_GROUP_POPULAR_TIME_RE.search(source))


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


def persist_inline_profile_updates_as_events(
    conn,
    *,
    row: Dict[str, Any],
    sender_db_id: Optional[int],
    inline_updates: Dict[str, str],
) -> None:
    """Backup ingestion path: if we can confidently extract updates inline, persist them as DM events.

    This prevents UX lies like "Saved" when the ingest worker is lagging or misconfigured.
    """
    if not sender_db_id or not isinstance(sender_db_id, int):
        return
    source_message_id = _to_int(row.get('id'))
    if not source_message_id:
        return

    role = _as_text(inline_updates.get('primary_role'))
    company = _as_text(inline_updates.get('primary_company'))
    raw_topics = _as_text(inline_updates.get('notable_topics'))

    conversation_id = row.get('conversation_id')
    source_external_message_id = _as_text(row.get('external_message_id'))
    raw_text = row.get('text') or ''

    if role or company:
        extracted_facts: List[Dict[str, Any]] = []
        if role:
            extracted_facts.append(
                {'field': 'primary_role', 'old_value': None, 'new_value': role, 'confidence': 0.9}
            )
        if company:
            extracted_facts.append(
                {'field': 'primary_company', 'old_value': None, 'new_value': company, 'confidence': 0.9}
            )
        evt_type = (
            'profile.role_company_update' if role and company
            else 'profile.role_update' if role
            else 'profile.company_update'
        )
        insert_dm_profile_update_event_if_absent(
            conn,
            user_id=sender_db_id,
            conversation_id=conversation_id,
            source_message_id=source_message_id,
            source_external_message_id=source_external_message_id,
            event_type=evt_type,
            event_payload={
                'raw_text': raw_text,
                'trigger': 'dm_responder_inline_parse',
                'role': role,
                'company': company,
            },
            extracted_facts=extracted_facts,
            confidence=0.9,
            event_source="dm_responder_inline",
            actor_role="user",
        )

    if raw_topics:
        topics = _split_plain_topics_answer(raw_topics)
        if not topics:
            cleaned = _clean_text(raw_topics).strip(" \"'`").strip(" .,!?:;")
            if cleaned and not looks_like_feedback_topic(cleaned):
                topics = [cleaned[:80]]
        if topics:
            insert_dm_profile_update_event_if_absent(
                conn,
                user_id=sender_db_id,
                conversation_id=conversation_id,
                source_message_id=source_message_id,
                source_external_message_id=source_external_message_id,
                event_type='profile.priorities_update',
                event_payload={
                    'raw_text': raw_text,
                    'trigger': 'dm_responder_inline_parse',
                    'priorities': topics,
                },
                extracted_facts=[
                    {'field': 'notable_topics', 'old_value': None, 'new_value': topic, 'confidence': 0.85}
                    for topic in topics
                ],
                confidence=0.85,
                event_source="dm_responder_inline",
                actor_role="user",
            )


def _collect_current_message_field_confidence(
    row: Dict[str, Any],
    pending_events: List[Dict[str, Any]],
    field_name: str,
) -> Optional[float]:
    msg_id = row.get('id')
    if not msg_id:
        return None
    best: Optional[float] = None
    for evt in pending_events:
        if evt.get('source_message_id') != msg_id:
            continue
        evt_conf = _to_float(evt.get('confidence'))
        facts = evt.get('extracted_facts')
        if not isinstance(facts, list):
            continue
        for fact in facts:
            if not isinstance(fact, dict):
                continue
            field = str(fact.get('field') or '').strip()
            value = _as_text(fact.get('new_value'))
            if field != field_name or not value:
                continue
            confidence = _to_float(fact.get('confidence'))
            if confidence is None:
                confidence = evt_conf
            if confidence is None:
                continue
            best = confidence if best is None else max(best, confidence)
    return best


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


def _split_plain_topics_answer(text: str) -> List[str]:
    """Parse a short list response like: "grants, partnerships, ecosystem growth"."""
    source = _clean_text(text).strip()
    if not source:
        return []
    if len(source) > 220:
        return []
    # Avoid treating questions/confusion/UX feedback as topic lists.
    if looks_like_feedback_topic(source):
        return []
    has_sep = bool(re.search(r",|;|\n|\band\b|&", source, flags=re.IGNORECASE))
    if not has_sep and len(source.split()) > 8:
        return []
    lowered = source.lower().strip(" .,!?:;\"'`")
    if lowered in ("yes", "yep", "yeah", "no", "nah", "nope", "ok", "okay", "sure", "k", "cool"):
        return []

    parts = re.split(r",|;|\n|\band\b|&", source, flags=re.IGNORECASE)
    out: List[str] = []
    seen: Set[str] = set()
    for part in parts:
        clean = _clean_text(part).strip(" .,!?:;\"'`")
        if not clean:
            continue
        clean = re.sub(
            r"^(?:my\s+)?(?:priorities?|focus|topics?)\s*(?:are|is)\s+",
            "",
            clean,
            flags=re.IGNORECASE,
        ).strip()
        if len(clean) < 2:
            continue
        if looks_like_feedback_topic(clean):
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(clean[:80])
        if len(out) >= 6:
            break
    return out


def upsert_dm_profile_update_event(
    conn,
    *,
    user_id: int,
    conversation_id: Optional[int],
    source_message_id: Optional[int],
    source_external_message_id: Optional[str],
    event_type: str,
    event_payload: Dict[str, Any],
    extracted_facts: List[Dict[str, Any]],
    confidence: float,
    event_source: str = "dm_responder",
    actor_role: str = "user",
) -> None:
    if not user_id or not source_message_id or not event_type:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dm_profile_update_events (
              user_id,
              conversation_id,
              source_message_id,
              source_external_message_id,
              event_type,
              event_source,
              actor_role,
              event_payload,
              extracted_facts,
              confidence
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
            ON CONFLICT (source_message_id, event_type) WHERE source_message_id IS NOT NULL DO UPDATE SET
              actor_role = EXCLUDED.actor_role,
              event_source = EXCLUDED.event_source,
              event_payload = EXCLUDED.event_payload,
              extracted_facts = EXCLUDED.extracted_facts,
              confidence = EXCLUDED.confidence,
              created_at = now()
            """,
            [
                user_id,
                conversation_id,
                source_message_id,
                source_external_message_id,
                event_type,
                event_source,
                actor_role,
                json.dumps(event_payload, ensure_ascii=True),
                json.dumps(extracted_facts, ensure_ascii=True),
                float(confidence),
            ],
        )


def insert_dm_profile_update_event_if_absent(
    conn,
    *,
    user_id: int,
    conversation_id: Optional[int],
    source_message_id: Optional[int],
    source_external_message_id: Optional[str],
    event_type: str,
    event_payload: Dict[str, Any],
    extracted_facts: List[Dict[str, Any]],
    confidence: float,
    event_source: str = "dm_responder",
    actor_role: str = "user",
) -> None:
    """Insert a DM profile update event without clobbering existing ingestion rows.

    This is used for responder "backup ingestion": we want to persist inline-extracted
    updates when the ingest worker is lagging, but we must NOT overwrite a dm_listener
    event_source if ingestion already wrote the row.
    """
    if not user_id or not source_message_id or not event_type:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dm_profile_update_events (
              user_id,
              conversation_id,
              source_message_id,
              source_external_message_id,
              event_type,
              event_source,
              actor_role,
              event_payload,
              extracted_facts,
              confidence
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
            ON CONFLICT (source_message_id, event_type) WHERE source_message_id IS NOT NULL DO NOTHING
            """,
            [
                user_id,
                conversation_id,
                source_message_id,
                source_external_message_id,
                event_type,
                event_source,
                actor_role,
                json.dumps(event_payload, ensure_ascii=True),
                json.dumps(extracted_facts, ensure_ascii=True),
                float(confidence),
            ],
        )


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
    cut = clean[: max(0, limit - 3)].rstrip()
    # Avoid hard mid-word truncation when we can.
    if cut and cut[-1].isalnum():
        last_space = cut.rfind(' ')
        if last_space >= 0 and last_space >= max(0, len(cut) - 24):
            cut = cut[:last_space].rstrip()
    return cut + "..."


def _preferred_style_mode(profile: Dict[str, Any]) -> str:
    style = _as_text(profile.get('preferred_contact_style')) or ''
    lower = style.lower()
    if not lower:
        return 'default'
    if 'bullet' in lower:
        return 'bullets'
    if 'direct' in lower:
        return 'direct'
    if any(token in lower for token in ('concise', 'short', 'brief')):
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
            return _truncate(text, limit=280) or text[:280]
        compact = "\n".join(lines[:3])
        if len(compact) > 320:
            cut = compact[:317].rstrip()
            if cut and cut[-1].isalnum():
                last_space = cut.rfind(' ')
                if last_space >= 0 and last_space >= max(0, len(cut) - 24):
                    cut = cut[:last_space].rstrip()
            compact = cut + "..."
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

    topics = sanitize_notable_topics(profile.get('notable_topics'), max_items=10)
    if topics:
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

    preferred_style = _as_text(profile.get('preferred_contact_style'))
    style_rule = _as_text(profile.get('contact_style_resolution_rule')) or STYLE_CONFLICT_RESOLUTION_RULE
    style_updated_at = profile.get('contact_style_updated_at')
    if preferred_style:
        updated_label = (
            style_updated_at.isoformat()
            if isinstance(style_updated_at, datetime)
            else _as_text(style_updated_at)
        )
        if updated_label:
            lines.append(
                f"Communication-style rule: {style_rule}. Current style=\"{preferred_style}\", last updated={updated_label}."
            )
        else:
            lines.append(f"Communication-style rule: {style_rule}. Current style=\"{preferred_style}\".")

    lines.append("If anything looks wrong, send corrections and I’ll prioritize those updates.")
    return "\n".join(lines)


def render_profile_data_inventory_reply(profile: Dict[str, Any]) -> str:
    """Explain what buckets of data exist for the user, and what is currently populated."""
    role = _as_text(profile.get('primary_role'))
    company = _as_text(profile.get('primary_company'))
    priorities = sanitize_notable_topics(profile.get('notable_topics'), max_items=10)
    contact_style = _as_text(profile.get('preferred_contact_style'))

    derived_bits: List[str] = []
    if _as_text(profile.get('generated_bio_professional')):
        derived_bits.append("bio signal")
    if isinstance(profile.get('deep_skills'), list) and profile.get('deep_skills'):
        derived_bits.append("skills")
    if isinstance(profile.get('driving_values'), list) and profile.get('driving_values'):
        derived_bits.append("values")
    if isinstance(profile.get('pain_points'), list) and profile.get('pain_points'):
        derived_bits.append("pain points")
    if isinstance(profile.get('affiliations'), list) and profile.get('affiliations'):
        derived_bits.append("affiliations")
    if isinstance(profile.get('attended_events'), list) and profile.get('attended_events'):
        derived_bits.append("events")

    analytics_bits: List[str] = []
    if isinstance(_to_int(profile.get('total_messages')), int):
        analytics_bits.append("message count")
    if isinstance(profile.get('peak_hours'), list) and profile.get('peak_hours'):
        analytics_bits.append("peak hours")
    if isinstance(profile.get('most_active_days'), list) and profile.get('most_active_days'):
        analytics_bits.append("active days")
    if isinstance(profile.get('group_tags'), list) and profile.get('group_tags'):
        analytics_bits.append("groups")
    if isinstance(profile.get('top_conversation_partners'), list) and profile.get('top_conversation_partners'):
        analytics_bits.append("top conversation partners")

    core_have: List[str] = []
    core_missing: List[str] = []
    if role:
        core_have.append("role")
    else:
        core_missing.append("role")
    if company:
        core_have.append("company/project")
    else:
        core_missing.append("company/project")
    if priorities:
        core_have.append("priorities/topics")
    else:
        core_missing.append("priorities/topics")
    if contact_style:
        core_have.append("communication style")
    else:
        core_missing.append("communication style")

    lines = [
        "Yes. In this deployment I can have 3 buckets about you:",
        "- Profile facts: role, company/project, priorities/topics, communication style.",
        "- Derived profile: bio signal, skills, values, affiliations, events (only if there’s enough signal).",
        "- Activity analytics: message counts, peak hours, groups, top partners (only if your history is ingested).",
        "",
        f"Right now, your core profile has: {', '.join(core_have) if core_have else 'nothing yet'}.",
    ]
    if derived_bits:
        lines.append(f"Derived fields present: {', '.join(derived_bits[:6])}.")
    if analytics_bits:
        lines.append(f"Analytics present: {', '.join(analytics_bits[:6])}.")
    if core_missing:
        lines.append(f"Missing core fields: {', '.join(core_missing)}.")
        lines.append("Send one update like: `role: ...` / `company: ...` / `priorities: ...` / `communication: ...`.")
    else:
        lines.append("If you want a specific slice, ask: snapshot, analytics, skills, or groups.")
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
    priorities = sanitize_notable_topics(profile.get('notable_topics'), max_items=10)
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
    conn=None,
) -> Tuple[Optional[str], Dict[str, Any]]:
    state = dict(onboarding_state or _default_onboarding_state())
    now = datetime.now(timezone.utc)
    msg_id = int(row.get('id') or 0)
    sender = row.get('display_name') or row.get('sender_handle') or 'you'
    latest_text = _clean_text(row.get('text'))
    is_greeting = is_greeting_message(latest_text)
    captured_updates = _collect_current_message_updates(row, pending_events)

    # Apply any captured updates to the in-memory profile view so onboarding progress doesn't loop
    # when ingestion/reconcile hasn't run yet.
    if captured_updates:
        role = _as_text(captured_updates.get('primary_role'))
        company = _as_text(captured_updates.get('primary_company'))
        topics_raw = _as_text(captured_updates.get('notable_topics'))
        if role:
            profile['primary_role'] = role
        if company:
            profile['primary_company'] = company
        if topics_raw:
            topics = _split_plain_topics_answer(topics_raw)
            if not topics:
                cleaned = _clean_text(topics_raw).strip(" \"'`").strip(" .,!?:;")
                if cleaned and not looks_like_feedback_topic(cleaned):
                    topics = [cleaned[:80]]
            if topics:
                existing = sanitize_notable_topics(profile.get('notable_topics'), max_items=10)
                existing_set = {item.lower() for item in existing}
                for topic in topics:
                    if topic.lower() not in existing_set:
                        existing.append(topic)
                        existing_set.add(topic.lower())
                profile['notable_topics'] = existing[:10]

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
    explicit_onboarding_requested = is_onboarding_start_request(latest_text) or full_profile_start
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

    # If we're mid-onboarding and we asked for a specific missing slot, treat the next plain reply as the answer.
    last_prompted = _as_text(state.get('last_prompted_field'))
    target_slot: Optional[str] = None
    if last_prompted and last_prompted in required_fields and last_prompted in missing_fields:
        target_slot = last_prompted
    elif len(missing_fields) == 1 and missing_fields[0] in required_fields:
        # Fallback: if only one slot is missing, treat the next reply as that answer even if last_prompted_field drifted.
        target_slot = missing_fields[0]
    if (
        status == 'collecting'
        and target_slot
        and not captured_updates
        and latest_text
        and not is_onboarding_acknowledgement(latest_text)
        and not is_greeting
    ):
        inferred: Dict[str, str] = {}
        user_id = row.get('sender_db_id')
        conversation_id = row.get('conversation_id')
        source_message_id = _to_int(row.get('id'))
        source_external_message_id = _as_text(row.get('external_message_id'))

        if target_slot == 'notable_topics':
            topics = _split_plain_topics_answer(latest_text)
            if topics:
                inferred['notable_topics'] = ", ".join(topics)
                if conn is not None and isinstance(user_id, int):
                    upsert_dm_profile_update_event(
                        conn,
                        user_id=user_id,
                        conversation_id=conversation_id,
                        source_message_id=source_message_id,
                        source_external_message_id=source_external_message_id,
                        event_type='profile.priorities_update',
                        event_payload={
                            'raw_text': row.get('text') or '',
                            'trigger': 'onboarding_answer_topics',
                            'priorities': topics,
                        },
                        extracted_facts=[
                            {
                                'field': 'notable_topics',
                                'old_value': None,
                                'new_value': topic,
                                'confidence': 0.88,
                            }
                            for topic in topics
                        ],
                        confidence=0.88,
                    )
                # Update the in-memory profile view so onboarding progresses immediately.
                existing = sanitize_notable_topics(profile.get('notable_topics'), max_items=10)
                existing_set = {item.lower() for item in existing}
                for topic in topics:
                    if topic.lower() not in existing_set:
                        existing.append(topic)
                        existing_set.add(topic.lower())
                profile['notable_topics'] = existing[:10]

        elif target_slot == 'primary_company':
            company_value = latest_text.strip(" \"'`").strip(" .,!?:;")
            if company_value and not re.match(
                r"^(?:launching|building|making|doing|working|growing|scaling|helping|assisting|supporting|pursuing|seeking|discovering)\b",
                company_value,
                re.IGNORECASE,
            ):
                normalized = 'unemployed' if 'unemployed' in company_value.lower() else company_value[:120]
                inferred['primary_company'] = normalized
                if conn is not None and isinstance(user_id, int):
                    upsert_dm_profile_update_event(
                        conn,
                        user_id=user_id,
                        conversation_id=conversation_id,
                        source_message_id=source_message_id,
                        source_external_message_id=source_external_message_id,
                        event_type='profile.company_update',
                        event_payload={
                            'raw_text': row.get('text') or '',
                            'trigger': 'onboarding_answer_company',
                            'new_company': normalized,
                        },
                        extracted_facts=[
                            {
                                'field': 'primary_company',
                                'old_value': None,
                                'new_value': normalized,
                                'confidence': 0.9,
                            }
                        ],
                        confidence=0.9,
                    )
                profile['primary_company'] = normalized

        elif target_slot == 'primary_role':
            role_value = latest_text.strip(" \"'`").strip(" .,!?:;")
            if role_value and not re.match(
                r"^(?:currently\s+)?(?:discovering|exploring|pursuing|seeking|finding|researching|learning|helping|assisting|supporting|driving|building|working|growing|scaling)\b",
                role_value,
                re.IGNORECASE,
            ):
                role = role_value[:120]
                company = None
                role_company_match = re.match(r"^(.{2,80}?)\s+(?:at|with|for)\s+(.{2,120})$", role_value, re.IGNORECASE)
                if role_company_match and 'primary_company' in missing_fields:
                    role = _clean_text(role_company_match.group(1))[:120]
                    company = _clean_text(role_company_match.group(2))[:120]
                inferred['primary_role'] = role
                if company and 'primary_company' not in inferred:
                    inferred['primary_company'] = company
                if conn is not None and isinstance(user_id, int):
                    extracted: List[Dict[str, Any]] = [
                        {'field': 'primary_role', 'old_value': None, 'new_value': role, 'confidence': 0.9}
                    ]
                    payload: Dict[str, Any] = {
                        'raw_text': row.get('text') or '',
                        'trigger': 'onboarding_answer_role',
                        'role': role,
                    }
                    evt_type = 'profile.role_update'
                    if company:
                        extracted.append({'field': 'primary_company', 'old_value': None, 'new_value': company, 'confidence': 0.9})
                        payload['company'] = company
                        evt_type = 'profile.role_company_update'
                    upsert_dm_profile_update_event(
                        conn,
                        user_id=user_id,
                        conversation_id=conversation_id,
                        source_message_id=source_message_id,
                        source_external_message_id=source_external_message_id,
                        event_type=evt_type,
                        event_payload=payload,
                        extracted_facts=extracted,
                        confidence=0.9,
                    )
                profile['primary_role'] = role
                if company:
                    profile['primary_company'] = company

        if inferred:
            captured_updates.update(inferred)
            missing_fields = _compute_missing_onboarding_fields(profile, required_fields)
            state['missing_fields'] = missing_fields

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
        should_announce_completion = explicit_onboarding_requested or status == 'collecting' or is_new_user_profile
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
        if should_announce_completion:
            lines = format_profile_snapshot_lines(profile)
            if lines:
                bullets = "\n".join(f"- {line}" for line in lines[:6])
                return (
                    "You’re already set up. Here’s your saved profile context:\n"
                    f"{bullets}\n"
                    "You can:\n"
                    "- Ask \"What do you know about me?\" for a snapshot\n"
                    "- Send updates (plain English or `field: value`)\n"
                    "- Say \"interview mode\" for one question at a time",
                    state,
                )
            return (
                "You’re already set up. Ask \"What do you know about me?\" for a snapshot, "
                "or send updates in `field: value` format.",
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
    return (
        "Totally fine — this chat is just for keeping your profile up to date.\n"
        "Send one quick answer and I’ll store it. You can also paste:\n"
        "role: ...\ncompany: ...\npriorities: ...\ncommunication: ..."
    )


def render_control_plane_reply(persona_name: str) -> str:
    return (
        "This assistant is configured by your OpenClaw deployment.\n"
        f"I can’t disclose or rewrite hidden system instructions, switch identity, or reboot from chat, and I’ll continue as {persona_name}.\n"
        "If you want behavior changes, tell me the exact response style you want (for example: concise bullets, deeper technical detail, no roleplay)."
    )


def render_capabilities_reply(profile: Dict[str, Any], persona_name: str) -> str:
    # Keep this response "UI-like": clear menu, no surprise follow-up questions.
    # Onboarding prompts are handled by the onboarding flow state machine.
    return "\n".join(
        [
            f"Hey — I’m {persona_name}. I’m an AI (not a human).",
            "This chat is for keeping your profile dataset current and letting you query it.",
            "Quick start (things you can say):",
            "- Home: \"home\" (opens the 1/2/3 menu)",
            "- Snapshot: \"What do you know about me?\"",
            "- Update (fast): `role: ...` / `company: ...` / `priorities: ...` / `communication: ...`",
            "- Guided setup: \"interview mode\" (one question at a time)",
            "- Lookup: \"What do you know about @handle?\" (read-only)",
            "- Analytics: \"What groups am I in?\" / \"When am I most active?\"",
            "- Feedback: `feedback: ...` (logs a bug/feature idea)",
            "Style note: if you ask for \"bullets\" or \"be brief\", I may ask for a quick yes/no to save that preference.",
            "Safety: I won’t ask you for money, seed phrases, or API keys.",
        ]
    )


def render_home_menu(row: Dict[str, Any], profile: Dict[str, Any], persona_name: str) -> str:
    sender = row.get('display_name') or row.get('sender_handle') or 'there'
    role = _as_text(profile.get('primary_role'))
    company = _as_text(profile.get('primary_company'))
    on_file = None
    if role and company:
        on_file = f"On file: {role} @ {company}."
    elif role:
        on_file = f"On file: {role}."
    elif company:
        on_file = f"On file: {company}."

    lines = [
        f"Hey {sender} — I’m {persona_name} (AI).",
    ]
    if on_file:
        lines.append(on_file)
    lines.extend(
        [
            "What do you want to do?",
            "1) Snapshot (what I know about you)",
            "2) Update (store a change)",
            "3) Analytics (groups + peak hours)",
            "Reply 1, 2, or 3. Or type \"help\" for everything I can do.",
        ]
    )
    return "\n".join(lines)


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


def render_help_reply(profile: Dict[str, Any], persona_name: str) -> str:
    # Currently identical to capabilities; keep a dedicated entry-point so it can diverge later without touching routing.
    return render_capabilities_reply(profile, persona_name)


def render_third_party_edit_policy_reply() -> str:
    return (
        "No — you can’t directly edit another person’s profile through this chat.\n"
        "When you ask about @handle, I treat it as a read-only lookup (it won’t change your profile or theirs).\n"
        "If you have a correction, the clean path is: ask them to DM me the update (or have them send `role: ...` / `company: ...`)."
    )


def render_third_party_lookup_storage_reply() -> str:
    return (
        "Third-party lookups are read-only.\n"
        "I don’t store new facts about them just because you asked.\n"
        "I may log that a lookup happened for debugging, but it won’t modify profiles."
    )


def render_feedback_ack_reply(kind: str) -> str:
    kind_label = kind.strip().lower() or 'feedback'
    if kind_label == 'bug':
        header = "Thanks — logged as a bug report."
    elif kind_label in ('feature', 'request'):
        header = "Thanks — logged as a feature request."
    elif kind_label == 'idea':
        header = "Thanks — logged as a product idea."
    else:
        header = "Thanks — logged as feedback."
    return (
        f"{header}\n"
        "If you want to make it actionable, add:\n"
        "- expected behavior\n"
        "- what happened instead"
    )


def render_more_profile_context_reply(profile: Dict[str, Any], persona_name: str) -> str:
    lines = format_profile_snapshot_lines(profile, include_activity=True)
    if not lines:
        return (
            f"I don’t have a usable profile for {persona_name}'s view of you yet.\n"
            "Send this quick format and I’ll save it immediately:\n"
            "role: ...\ncompany: ...\npriorities: ...\ncommunication: ..."
        )
    if len(lines) <= 10:
        bullets = "\n".join(f"- {line}" for line in lines[:10])
        return f"That’s everything I have right now:\n{bullets}"
    extra = lines[10:18]
    bullets = "\n".join(f"- {line}" for line in extra)
    return (
        "More profile context I have:\n"
        f"{bullets}\n"
        "If you want a specific slice, ask: activity, groups, skills, or communication style."
    )


def render_group_popular_time_reply(conn, group_query: str) -> str:
    q = _clean_text(group_query).strip(" .,!?:;\"'`")
    if not q:
        return "Which group? Send the exact group name in quotes (example: \"BTC Connect ⚡️🚀\")."

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, title
            FROM groups
            WHERE title ILIKE %s
            ORDER BY (CASE WHEN lower(title) = lower(%s) THEN 0 ELSE 1 END), updated_at DESC, id DESC
            LIMIT 5
            """,
            [f"%{q}%", q],
        )
        matches = list(cur.fetchall())

    if not matches:
        return (
            f"I couldn’t find a group matching: {q!r}.\n"
            "Try the exact group title (copy/paste) or put it in quotes."
        )

    if len(matches) > 1 and all(_clean_text(m.get('title') or '').lower() != q.lower() for m in matches):
        options = "\n".join(f"- {m.get('title')}" for m in matches if m.get('title'))
        return (
            "I found multiple matching groups. Reply with the exact one:\n"
            f"{options}"
        )

    group_id = matches[0]['id']
    group_title = matches[0].get('title') or q

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
              EXTRACT(HOUR FROM (sent_at AT TIME ZONE 'UTC'))::int AS hour_utc,
              COUNT(*)::bigint AS msg_count
            FROM messages
            WHERE group_id = %s
            GROUP BY 1
            ORDER BY msg_count DESC
            LIMIT 5
            """,
            [group_id],
        )
        rows = list(cur.fetchall())
        cur.execute(
            "SELECT MIN(sent_at) AS first_seen, MAX(sent_at) AS last_seen, COUNT(*)::bigint AS total FROM messages WHERE group_id = %s",
            [group_id],
        )
        window = cur.fetchone() or {}

    if not rows:
        return f"I found {group_title!r}, but I don’t have any message history indexed for it yet."

    labels = [f"{int(r['hour_utc']):02d}:00 ({int(r['msg_count'])})" for r in rows if r.get('hour_utc') is not None]
    first_seen = window.get('first_seen')
    last_seen = window.get('last_seen')
    total = window.get('total')
    window_line = None
    if first_seen and last_seen and total:
        window_line = f"Window: {str(first_seen)[:10]} -> {str(last_seen)[:10]} (total msgs: {int(total)})"

    lines = [
        f"Most active hours in {group_title} (UTC):",
        "- " + ", ".join(labels),
    ]
    if window_line:
        lines.append(window_line)
    return "\n".join(lines)


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
    # Keep this bot as a profile/data layer. If the user sends "1/2/3" without a menu context,
    # we should not drift into work-planning flows.
    return (
        f"I saw \"{selected_option}\", but I don’t have a menu open right now.\n"
        "Say \"help\" to see options, or send an update (role/company/priorities/communication)."
    )


def llm_reply_looks_untrusted(reply: Optional[str]) -> bool:
    source = _clean_text(reply)
    if not source:
        return False
    return bool(_LLM_FORBIDDEN_CLAIM_RE.search(source))


def should_use_llm_for_reply(latest_text: Optional[str]) -> bool:
    if not DM_RESPONSE_LLM_ENABLED or not OPENROUTER_API_KEY:
        return False
    if not openrouter_spend_fuse_allows_call():
        return False
    strategy = (DM_RESPONSE_LLM_STRATEGY or 'auto').strip().lower()
    if strategy in ('0', 'false', 'no', 'off', 'never'):
        return False
    if strategy in ('1', 'true', 'yes', 'on', 'always'):
        return True

    text = _clean_text(latest_text)
    if not text:
        return False
    # Auto-mode: only allow LLM replies when the user explicitly requests advice/help
    # outside the profile/data layer.
    return bool(re.match(r"^advice\\s*:\\s*", text, flags=re.IGNORECASE))


def _utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _read_openrouter_spend_state_unlocked() -> Dict[str, Any]:
    path = DM_OPENROUTER_SPEND_STATE_FILE
    try:
        raw = Path(path).read_text('utf-8')
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _write_openrouter_spend_state_unlocked(state: Dict[str, Any]) -> None:
    path = Path(DM_OPENROUTER_SPEND_STATE_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    state['updated_at'] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(state, ensure_ascii=True, indent=2) + "\n", 'utf-8')


def _with_openrouter_spend_lock(fn):
    if DM_OPENROUTER_DAILY_COST_CAP_USD <= 0:
        return fn()

    lock_path = Path(DM_OPENROUTER_SPEND_LOCK_FILE)
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    deadline = time.time() + (DM_OPENROUTER_SPEND_LOCK_TIMEOUT_MS / 1000.0)
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, json.dumps({'pid': os.getpid(), 'ts': time.time()}, ensure_ascii=True).encode('utf-8'))
            finally:
                os.close(fd)
            try:
                return fn()
            finally:
                try:
                    lock_path.unlink(missing_ok=True)  # py>=3.8
                except Exception:
                    try:
                        os.unlink(str(lock_path))
                    except Exception:
                        pass
        except FileExistsError:
            try:
                st = lock_path.stat()
                if time.time() - st.st_mtime > 30:
                    lock_path.unlink(missing_ok=True)
                    continue
            except FileNotFoundError:
                continue
            if time.time() > deadline:
                # Can't safely coordinate spend. Fail closed and skip LLM calls.
                return None
            time.sleep(0.05)


def _normalized_spend_state(state: Dict[str, Any]) -> Dict[str, Any]:
    today = _utc_day()
    if state.get('date') != today:
        return {'date': today, 'total_cost_usd': 0.0, 'by_component': {}, 'by_model': {}}
    if not isinstance(state.get('total_cost_usd'), (int, float)):
        state['total_cost_usd'] = 0.0
    if not isinstance(state.get('by_component'), dict):
        state['by_component'] = {}
    if not isinstance(state.get('by_model'), dict):
        state['by_model'] = {}
    return state


def openrouter_spend_fuse_allows_call() -> bool:
    if DM_OPENROUTER_DAILY_COST_CAP_USD <= 0:
        return True

    def check() -> bool:
        state = _normalized_spend_state(_read_openrouter_spend_state_unlocked())
        total = float(state.get('total_cost_usd') or 0.0)
        allowed = total < DM_OPENROUTER_DAILY_COST_CAP_USD
        if not allowed:
            print(
                f"🚫 OpenRouter spend fuse tripped: total_cost_usd={total:.6f} cap_usd={DM_OPENROUTER_DAILY_COST_CAP_USD:.6f}. "
                "Skipping LLM calls until next UTC day."
            )
        return allowed

    out = _with_openrouter_spend_lock(check)
    return bool(out)


def record_openrouter_cost(cost_usd: float, *, component: str, model: str) -> None:
    if DM_OPENROUTER_DAILY_COST_CAP_USD <= 0:
        return
    if cost_usd <= 0:
        return

    def update() -> None:
        state = _normalized_spend_state(_read_openrouter_spend_state_unlocked())
        total = float(state.get('total_cost_usd') or 0.0)
        state['total_cost_usd'] = total + float(cost_usd)
        by_component = state.get('by_component') or {}
        if isinstance(by_component, dict):
            by_component[component] = float(by_component.get(component) or 0.0) + float(cost_usd)
        state['by_component'] = by_component
        by_model = state.get('by_model') or {}
        if isinstance(by_model, dict):
            by_model[model] = float(by_model.get(model) or 0.0) + float(cost_usd)
        state['by_model'] = by_model
        _write_openrouter_spend_state_unlocked(state)

    _with_openrouter_spend_lock(update)


def call_openrouter_chat(system_prompt: str, user_prompt: str) -> Optional[str]:
    if not DM_RESPONSE_LLM_ENABLED or not OPENROUTER_API_KEY:
        return None
    if not openrouter_spend_fuse_allows_call():
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
            'X-Title': f'Telethon DM Responder ({DM_RESPONSE_MODEL})',
        },
    )

    try:
        start = time.monotonic()
        with urlopen(req, timeout=35) as resp:
            request_id = resp.headers.get('x-request-id') or resp.headers.get('x-openrouter-request-id') or ''
            raw = resp.read().decode('utf-8', errors='replace')
            body = json.loads(raw)
        latency_ms = int((time.monotonic() - start) * 1000)
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
    usage = body.get('usage') if isinstance(body, dict) else None
    if isinstance(usage, dict):
        prompt_tokens = usage.get('prompt_tokens')
        completion_tokens = usage.get('completion_tokens')
        total_tokens = usage.get('total_tokens')
        cost = usage.get('cost')
        model_used = body.get('model') if isinstance(body.get('model'), str) else DM_RESPONSE_MODEL
        if total_tokens is not None:
            rid = request_id if 'request_id' in locals() else ''
            rid_part = f" request_id={rid}" if rid else ""
            print(
                f"🧾 openrouter model={model_used} prompt_tokens={prompt_tokens} completion_tokens={completion_tokens} "
                f"total_tokens={total_tokens} cost={cost} max_tokens={DM_RESPONSE_MAX_TOKENS} latency_ms={latency_ms}{rid_part}"
            )
        try:
            cost_val = float(cost)
        except Exception:
            cost_val = 0.0
        if cost_val > 0:
            record_openrouter_cost(cost_val, component='dm_responder', model=str(model_used))
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
    if not should_use_llm_for_reply(latest_text):
        return None
    # Strip explicit "advice:" prefix so the model sees the actual request.
    advice_text = re.sub(r"^advice\s*:\s*", "", latest_text, flags=re.IGNORECASE).strip() or latest_text

    context = {
        'sender_name': row.get('display_name') or row.get('sender_handle') or 'user',
        'latest_inbound_message': advice_text,
        'explicit_advice_prefix': advice_text != latest_text,
        'is_profile_request': is_full_profile_request(advice_text),
        'is_third_party_profile_lookup': is_third_party_profile_request(advice_text),
        'is_indecision': is_indecision_request(advice_text),
        'is_activity_analytics_request': is_activity_analytics_request(advice_text),
        'is_profile_data_provenance_request': is_profile_data_provenance_request(advice_text),
        'is_profile_update_mode_request': is_profile_update_mode_request(advice_text),
        'is_profile_confirmation_request': is_profile_confirmation_request(advice_text),
        'is_interview_style_request': is_interview_style_request(advice_text),
        'is_top3_profile_prompt_request': is_top3_profile_prompt_request(advice_text),
        'is_missed_intent_feedback': is_missed_intent_feedback(advice_text),
        'likely_profile_update_message': is_likely_profile_update_message(advice_text),
        'inline_profile_updates': _collect_current_message_updates({**row, 'text': advice_text}, pending_events),
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
        "9) If user says they are unsure what to do, keep it inside profile-upkeep: ask one onboarding question or suggest 3 profile fields to update (role/company/priorities/style).\n"
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

    if is_profile_data_inventory_request(latest_text):
        return render_profile_data_inventory_reply(profile)

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
            return response

        response = f"{ack_line} Saved: {summary}. Ask \"What do you know about me?\" for a full snapshot."
        if 'preferred_contact_style' in captured_updates:
            response += " I’ll use that style in future replies."
        return response

    if is_likely_profile_update_message(latest_text):
        return (
            "I read that as a profile update, but I couldn’t confidently extract a specific field.\n"
            "You can say it in plain English (example: “My role is X and my company is Y”), or use:\n"
            "role: ...\ncompany: ...\npriorities: ...\ncommunication: ..."
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
            "Quick start: ask \"What do you know about me?\" (snapshot), send `role: ...` / `company: ...` (update), "
            "or say \"help\" for the full menu."
        )
    if "?" in source or _QUESTION_LIKE_RE.search(source):
        return (
            "I can answer questions about your stored profile + activity analytics, and I can capture profile updates.\n"
            "Try one of these:\n"
            "- \"What do you know about me?\"\n"
            "- \"Where did my data come from?\"\n"
            "- \"What groups am I in?\"\n"
            "- Or send: `role: ...` / `company: ...` / `priorities: ...` / `communication: ...`"
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

    sender_db_id = row.get('sender_db_id')
    profile = fetch_latest_profile(conn, sender_db_id)

    # DM profile state snapshot provides:
    # - confirmation-gated style preference (style_preference)
    # - durable role/company/priorities overrides (profile_overrides)
    snapshot = _fetch_profile_snapshot(conn, sender_db_id)
    style_state = _parse_contact_style_state_from_snapshot(snapshot)
    ui_state = _parse_ui_state_from_snapshot(snapshot)
    ui_preferences = _parse_ui_preferences_from_snapshot(snapshot)
    profile = merge_contact_style_state_into_profile(profile, style_state)

    profile_overrides = _parse_profile_overrides_from_snapshot(snapshot)
    if not profile_overrides:
        # Back-compat: fall back to latest dm-event-reconciler row if snapshot overlay isn't present yet.
        profile_overrides = fetch_latest_dm_reconciler_overrides(conn, sender_db_id)
    profile = merge_profile_overrides_into_profile(profile, profile_overrides)

    pending_events = fetch_pending_profile_events(conn, sender_db_id)
    profile = apply_pending_profile_events(profile, pending_events)

    current_updates = _collect_current_message_updates(row, pending_events)
    latest_text = row.get('text')
    explicit_feedback = parse_feedback_message(latest_text)
    implicit_feedback = None if explicit_feedback else parse_implicit_feedback_message(latest_text)

    # Backup ingestion path: if we can extract core profile updates inline, persist them immediately.
    # This keeps UX aligned with storage even if the ingest worker is lagging or misconfigured.
    inline_updates = _extract_inline_profile_updates(latest_text)
    if inline_updates and not explicit_feedback and not implicit_feedback:
        persist_inline_profile_updates_as_events(
            conn,
            row=row,
            sender_db_id=sender_db_id,
            inline_updates=inline_updates,
        )

    def finalize_reply(raw_reply: str) -> str:
        nonlocal style_state, profile
        styled_reply = apply_preferred_contact_style(raw_reply, profile)
        final_reply, prompted = maybe_append_contact_style_reconfirm(styled_reply, profile, style_state)
        if prompted:
            style_state = mark_contact_style_reconfirm_prompted(conn, sender_db_id)
            profile = merge_contact_style_state_into_profile(profile, style_state)
        return final_reply

    pending_candidate = style_state.get('pending_candidate') if isinstance(style_state.get('pending_candidate'), dict) else None
    if pending_candidate:
        pending_value = _as_text(pending_candidate.get('value'))
        if pending_value and is_style_confirmation_yes(latest_text):
            confirmed_confidence = _to_float(pending_candidate.get('confidence'))
            style_state = persist_contact_style_state(
                conn,
                sender_db_id,
                pending_value,
                source='user_style_confirmation',
                confidence=confirmed_confidence,
                source_message_id=_to_int(row.get('id')),
            )
            profile = merge_contact_style_state_into_profile(profile, style_state)
            confirmation_reply = f"Perfect — switched. I’ll use \"{pending_value}\" going forward."
            return finalize_reply(confirmation_reply)
        if pending_value and is_style_confirmation_no(latest_text):
            style_state = clear_pending_contact_style_candidate(conn, sender_db_id)
            profile = merge_contact_style_state_into_profile(profile, style_state)
            current_style = _as_text(profile.get('preferred_contact_style'))
            if current_style:
                decline_reply = f"Got it — keeping your current style: \"{current_style}\"."
            else:
                decline_reply = "Got it — I won’t change style yet."
            return finalize_reply(decline_reply)

    current_style_update = _as_text(current_updates.get('preferred_contact_style'))
    if current_style_update and implicit_feedback:
        # UX: suppress style prompts when the user is giving product feedback.
        current_style_update = None
    if current_style_update and (is_help_request(latest_text) or is_capabilities_request(latest_text)):
        # UX: avoid turning "help/menu/capability" chats into surprise preference changes.
        current_style_update = None
    if current_style_update:
        current_style_confidence = _collect_current_message_field_confidence(
            row,
            pending_events,
            'preferred_contact_style',
        )
        if current_style_confidence is None:
            current_style_confidence = 0.62
        if current_style_confidence >= DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD:
            style_state = persist_contact_style_state(
                conn,
                sender_db_id,
                current_style_update,
                source='dm_responder',
                confidence=current_style_confidence,
                source_message_id=_to_int(row.get('id')),
            )
        else:
            style_state = persist_pending_contact_style_candidate(
                conn,
                sender_db_id,
                current_style_update,
                source='dm_responder_low_confidence',
                confidence=current_style_confidence,
                source_message_id=_to_int(row.get('id')),
            )
            non_style_updates = {k: v for k, v in current_updates.items() if k != 'preferred_contact_style'}
            profile = merge_contact_style_state_into_profile(profile, style_state)
            prompt = render_style_confirmation_prompt(current_style_update, current_style_confidence)
            if non_style_updates:
                summary = _format_captured_updates_summary(non_style_updates)
                return finalize_reply(f"Saved: {summary}. {prompt}")
            return finalize_reply(prompt)
        profile = merge_contact_style_state_into_profile(profile, style_state)

    onboarding_state = fetch_onboarding_state(conn, sender_db_id)
    recent_messages = fetch_recent_conversation_messages(conn, row.get('conversation_id'))
    core_missing_fields = _compute_missing_onboarding_fields(profile, ONBOARDING_REQUIRED_FIELDS)
    onboarding_complete = len(core_missing_fields) == 0

    # Handle home-menu selections ("1/2/3") before onboarding/other routing.
    selected_option = extract_option_selection(latest_text)
    if (
        selected_option
        and onboarding_complete
        and not pending_candidate
        and ui_menu_is_active(ui_state, expected_type='home')
    ):
        ui_state = clear_ui_menu(conn, sender_db_id, ui_state)
        if selected_option == 1:
            return finalize_reply(render_profile_request_reply(row, profile, args.persona_name))
        if selected_option == 2:
            return finalize_reply(render_profile_update_mode_reply())
        if selected_option == 3:
            return finalize_reply(render_activity_analytics_reply(profile))

    if is_home_request(latest_text) and onboarding_complete and not pending_candidate:
        now = datetime.now(timezone.utc)
        ui_state = persist_ui_state(
            conn,
            sender_db_id,
            {
                'menu': _ui_menu_payload('home', now=now),
                # Also bump greeting cooldown so "home" doesn't immediately re-trigger another menu on "hi".
                'last_greeting_menu_at': now.isoformat(),
            },
        )
        return finalize_reply(render_home_menu(row, profile, args.persona_name))

    # Greeting UX: show a periodic quickstart menu so users don't get stuck.
    if is_greeting_message(latest_text) and onboarding_complete:
        greeting_menu = _as_text(ui_preferences.get('greeting_menu')) or 'quickstart'
        cooldown_days = _to_int(ui_preferences.get('greeting_menu_cooldown_days')) or DM_UI_GREETING_MENU_COOLDOWN_DAYS
        cooldown_days = max(0, min(30, int(cooldown_days)))
        last_shown = ui_state.get('last_greeting_menu_at')
        if not isinstance(last_shown, datetime):
            last_shown = _to_datetime(last_shown)
        now = datetime.now(timezone.utc)
        should_show = greeting_menu != 'off' and (not isinstance(last_shown, datetime) or (now - last_shown) >= timedelta(days=cooldown_days))

        if should_show and greeting_menu == 'help':
            ui_state = persist_ui_state(
                conn,
                sender_db_id,
                {
                    'menu': None,
                    'last_greeting_menu_at': now.isoformat(),
                },
            )
            return finalize_reply(render_help_reply(profile, args.persona_name))

        if should_show:
            ui_state = persist_ui_state(
                conn,
                sender_db_id,
                {
                    'menu': _ui_menu_payload('home', now=now),
                    'last_greeting_menu_at': now.isoformat(),
                },
            )
            return finalize_reply(render_home_menu(row, profile, args.persona_name))

        return finalize_reply(
            f"Hey — I’m {args.persona_name} (AI). Say \"help\" for options, or ask \"What do you know about me?\"."
        )

    third_party_lookup = _lookup_third_party_user(conn, row.get('text'))
    if third_party_lookup:
        return finalize_reply(render_third_party_profile_reply(third_party_lookup))

    if is_control_plane_request(latest_text):
        return finalize_reply(render_control_plane_reply(args.persona_name))
    if is_secret_request(latest_text):
        return finalize_reply(render_secret_request_reply())
    if is_sexual_style_request(latest_text):
        return finalize_reply(render_sexual_style_reply())
    if is_disengage_request(latest_text):
        return finalize_reply(render_disengage_reply())
    if is_non_text_marker(latest_text):
        return finalize_reply(render_non_text_marker_reply())
    if explicit_feedback:
        persist_feedback(
            conn,
            row=row,
            sender_db_id=sender_db_id,
            kind=explicit_feedback['kind'],
            body=explicit_feedback['body'],
        )
        return finalize_reply(render_feedback_ack_reply(explicit_feedback['kind']))
    if implicit_feedback:
        persist_feedback(
            conn,
            row=row,
            sender_db_id=sender_db_id,
            kind=implicit_feedback['kind'],
            body=implicit_feedback['body'],
        )
        # If the feedback explicitly asks for "hello"/"first message" onboarding help,
        # persist a per-user preference to show the full help menu on greeting.
        body = _clean_text(implicit_feedback.get('body'))
        if body and re.search(r"\b(?:when\s+i\s+say\s+hello|next\s+time|first\s+message|pretend\s+it'?s\s+my\s+first)\b", body, re.IGNORECASE):
            ui_preferences = persist_ui_preferences(conn, sender_db_id, {'greeting_menu': 'help'})
        source = _clean_text(latest_text)
        is_question = ("?" in source) or bool(_QUESTION_LIKE_RE.search(source))
        if not is_question and not is_help_request(latest_text):
            # UX: acknowledge the feedback and reset to a clear menu.
            return finalize_reply(
                f"{render_feedback_ack_reply(implicit_feedback['kind'])}\n\n"
                f"{render_help_reply(profile, args.persona_name)}"
            )
    if is_help_request(latest_text):
        return finalize_reply(render_help_reply(profile, args.persona_name))
    if is_third_party_edit_policy_request(latest_text):
        return finalize_reply(render_third_party_edit_policy_reply())
    if is_third_party_lookup_storage_request(latest_text):
        return finalize_reply(render_third_party_lookup_storage_reply())
    if is_group_popular_time_request(latest_text):
        group_q = extract_group_query(latest_text) or ''
        return finalize_reply(render_group_popular_time_reply(conn, group_q))
    if is_capabilities_request(latest_text):
        return finalize_reply(render_capabilities_reply(profile, args.persona_name))
    if is_unsupported_action_request(latest_text):
        return finalize_reply(render_unsupported_action_reply())
    if is_profile_update_mode_request(latest_text):
        return finalize_reply(render_profile_update_mode_reply())
    if is_interview_style_request(latest_text):
        return finalize_reply(render_interview_style_reply(profile))
    if is_top3_profile_prompt_request(latest_text):
        return finalize_reply(render_top3_profile_prompt_reply(profile))
    if is_missed_intent_feedback(latest_text):
        return finalize_reply(render_missed_intent_reply(profile))
    if is_activity_analytics_request(latest_text):
        return finalize_reply(render_activity_analytics_reply(profile))
    if is_profile_data_provenance_request(latest_text):
        return finalize_reply(render_profile_data_provenance_reply(profile))
    if is_profile_data_inventory_request(latest_text):
        return finalize_reply(render_profile_data_inventory_reply(profile))
    if is_profile_confirmation_request(latest_text):
        return finalize_reply(render_profile_confirmation_reply(row, profile, pending_events))
    if is_more_profile_info_request(latest_text, recent_messages):
        return finalize_reply(render_more_profile_context_reply(profile, args.persona_name))

    onboarding_reply, next_onboarding_state = render_onboarding_flow_reply(
        row,
        profile,
        pending_events,
        onboarding_state,
        args.persona_name,
        conn,
    )
    if next_onboarding_state != onboarding_state:
        persist_onboarding_state(conn, sender_db_id, next_onboarding_state)
    if onboarding_reply:
        return finalize_reply(onboarding_reply)

    selected_option = extract_option_selection(latest_text)
    if selected_option:
        return finalize_reply(
            render_option_selection_reply(selected_option, profile, recent_messages)
        )

    if (
        is_full_profile_request(latest_text)
        or is_indecision_request(latest_text)
        or is_interview_style_request(latest_text)
        or is_top3_profile_prompt_request(latest_text)
        or is_missed_intent_feedback(latest_text)
        or is_likely_profile_update_message(latest_text)
    ):
        return finalize_reply(render_conversational_reply(row, profile, args.persona_name, pending_events))
    if current_updates:
        return finalize_reply(render_conversational_reply(row, profile, args.persona_name, pending_events))

    llm_reply = render_llm_conversational_reply(row, profile, args.persona_name, recent_messages, pending_events)
    if llm_reply and not llm_reply_looks_untrusted(llm_reply):
        return finalize_reply(llm_reply)
    return finalize_reply(render_conversational_reply(row, profile, args.persona_name, pending_events))


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
