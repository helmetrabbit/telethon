#!/usr/bin/env node
import crypto from 'node:crypto';
import fs from 'node:fs';
import { promises as fsp } from 'node:fs';
import readline from 'node:readline';
import path from 'node:path';
import { db } from '../db/index.js';
import { parseArgs } from '../utils.js';
import { createLLMClient } from '../inference/llm-client.js';


interface DmEvent {
  message_id: number;
  chat_id: number | string;
  direction: 'inbound' | 'outbound';
  sender_id: string | null;
  sender_name: string | null;
  sender_username: string | null;
  peer_id: string | null;
  account_id?: string | null;
  peer_name: string | null;
  peer_username: string | null;
  text: string | null;
  text_len: number;
  date: string;
  reply_to_message_id: number | null;
  views: number;
  forwards: number;
  has_links: boolean;
  has_mentions: boolean;
}

interface ProfileFact {
  field: 'primary_company' | 'primary_role' | 'preferred_contact_style' | 'notable_topics';
  old_value: string | null;
  new_value: string | null;
  confidence: number;
}

interface ProfileEvent {
  event_type: string;
  event_payload: Record<string, unknown>;
  extracted_facts: ProfileFact[];
  confidence: number;
}

interface IngestState {
  version: 1;
  file: string;
  lastLine: number;
  fileSize: number;
  updatedAt: string;
}

const HANDLE_RE = /@([A-Za-z0-9_]+)/g;
const DM_PROFILE_LLM_ENABLED = (process.env.DM_PROFILE_LLM_EXTRACTION || '').toLowerCase() === '1' || (process.env.DM_PROFILE_LLM_EXTRACTION || '').toLowerCase() === 'true';
const DM_PROFILE_LLM_MODEL = process.env.DM_PROFILE_LLM_MODEL || 'deepseek/deepseek-chat';

const dmLlmClient = (() => {
  const key = process.env.OPENROUTER_API_KEY;
  if (!DM_PROFILE_LLM_ENABLED || !key) return null;
  return createLLMClient({
    apiKeys: [key],
    model: DM_PROFILE_LLM_MODEL,
    maxRetries: 3,
    retryDelayMs: 500,
    requestDelayMs: 100,
  });
})();

function parseLine(raw: string): DmEvent | null {
  try {
    return JSON.parse(raw) as DmEvent;
  } catch {
    return null;
  }
}

function isTruthyNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

async function loadIngestState(statePath: string | null): Promise<IngestState | null> {
  if (!statePath) return null;
  try {
    const raw = await fsp.readFile(statePath, 'utf8');
    const parsed = JSON.parse(raw) as Partial<IngestState>;
    const parsedLastLine = parsed.lastLine;
    const parsedFileSize = parsed.fileSize;

    if (
      parsed
      && typeof parsed.file === 'string'
      && typeof parsedLastLine === 'number'
      && Number.isInteger(parsedLastLine)
      && parsedLastLine >= 0
      && typeof parsedFileSize === 'number'
      && Number.isInteger(parsedFileSize)
      && parsedFileSize >= 0
      && typeof parsed.updatedAt === 'string'
    ) {
      return {
        version: 1,
        file: parsed.file,
        lastLine: parsedLastLine,
        fileSize: parsedFileSize,
        updatedAt: parsed.updatedAt,
      };
    }
  } catch {
    return null;
  }

  return null;
}

async function writeIngestState(statePath: string | null, state: IngestState): Promise<void> {
  if (!statePath) return;
  await fsp.mkdir(path.dirname(statePath), { recursive: true });
  await fsp.writeFile(statePath, JSON.stringify(state, null, 2), 'utf8');
}

function validDmDirection(v: unknown): v is 'inbound' | 'outbound' {
  return v === 'inbound' || v === 'outbound';
}

function toLowerHandle(input: string | null | undefined): string | null {
  if (!input) return null;
  return input.trim().toLowerCase();
}

function extractHandles(text: string | null): string[] {
  if (!text) return [];
  const out = new Set<string>();
  for (const m of text.matchAll(HANDLE_RE)) {
    const handle = m[1].toLowerCase();
    if (handle) out.add(handle);
  }
  return [...out];
}

function textHash(text: string | null): string {
  return crypto.createHash('sha256').update(text || '').digest('hex');
}

function sanitizeEntity(value: string | null | undefined): string | null {
  if (!value) return null;
  return value
    .replace(/[\t\n\r]+/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .replace(/["'`]+/g, '')
    .trim();
}

function cleanupCompanyName(raw: string): string {
  return raw
    .replace(/\b(?:LLC|Ltd\.?|Inc\.?|Co\.?|Corp\.?)/giu, '')
    .replace(/\s{2,}/g, ' ')
    .trim()
    .replace(/[.\s]+$/g, '')
    .trim();
}

function normalizeRole(raw: string): string {
  return raw
    .replace(/^[\s-]+/, '')
    .replace(/[\s,.!?;:]+$/g, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function normalizeContactStyle(raw: string): string {
  const clean = sanitizeEntity(raw);
  if (!clean) return '';
  const lower = clean.toLowerCase();

  if (lower.includes('email')) return 'email';
  if (lower.includes('telegram') || lower.includes('dm')) return 'telegram dm';
  if (lower.includes('text') || lower.includes('sms')) return 'text';
  if (lower.includes('call') || lower.includes('phone')) return 'call';
  if (lower.includes('voice')) return 'voice notes';
  if (lower.includes('short') || lower.includes('concise')) return 'short messages';
  if (lower.includes('detailed') || lower.includes('long')) return 'detailed messages';
  return clean.toLowerCase();
}

function parseUnemployedStatement(source: string): ProfileEvent[] {
  const events: ProfileEvent[] = [];

  const leftNowPatterns = [
    /(?:^|[\s\n])(?:i\s+(?:am|'m)\s+)?(?:no\s+longer|left)\s+(?:at|with|for)\s+([A-Za-z0-9 .,'-]{2,120})[^.!?\n]{0,80}?(?:and|,|\s+now)\s+(?:i\s+(?:am|'m)\s+)?(?:unemployed|not\s+working)/giu,
    /(?:^|[\s\n])(?:i\s+(?:am|'m)\s+)?(?:formerly\s+at|previously\s+at)\s+([A-Za-z0-9 .,'-]{2,120})[^.!?\n]{0,80}?(?:and\s+now|now)\s+(?:i\s+(?:am|'m)\s+)?unemployed/giu,
  ];
  for (const pattern of leftNowPatterns) {
    for (const m of source.matchAll(pattern)) {
      const oldCompany = sanitizeEntity(m[1]);
      if (!oldCompany) continue;
      const cleaned = cleanupCompanyName(oldCompany);
      if (!cleaned) continue;
      events.push({
        event_type: 'profile.company_update',
        confidence: 0.88,
        event_payload: {
          raw_text: source,
          old_company: cleaned,
          new_company: 'unemployed',
          trigger: 'left_and_unemployed',
        },
        extracted_facts: [
          {
            field: 'primary_company',
            old_value: cleaned,
            new_value: 'unemployed',
            confidence: 0.88,
          },
        ],
      });
    }
  }

  const directUnemployed = /(?:i\s+(?:am|'m)\s+(?:currently\s+)?(?:not\s+working|unemployed)|currently\s+unemployed|now\s+unemployed)\b/giu;
  if (directUnemployed.test(source) && !/(?:working|now\s+at)/iu.test(source)) {
    events.push({
      event_type: 'profile.company_update',
      confidence: 0.78,
      event_payload: {
        raw_text: source,
        new_company: 'unemployed',
        trigger: 'unemployed_direct',
      },
      extracted_facts: [
        {
          field: 'primary_company',
          old_value: null,
          new_value: 'unemployed',
          confidence: 0.78,
        },
      ],
    });
  }

  return events;
}


function shouldTryLLMForProfileExtraction(source: string): boolean {
  const s = source.toLowerCase();
  return /company|work|working|work at|work with|join|joined|left|unemployed|role|title|priorit|contact|how can i reach|reach me|my role|i am/.test(s);
}

async function extractProfileEventsByLLM(source: string): Promise<ProfileEvent[]> {
  if (!dmLlmClient || !DM_PROFILE_LLM_ENABLED || !shouldTryLLMForProfileExtraction(source)) return [];

  const prompt = `
You are a profile fact extractor for one DM message.
Return ONLY JSON in this exact shape: {"events":[...]}
Each event: {"event_type": string, "confidence": number, "event_payload": object, "extracted_facts": [{"field":"primary_company|primary_role|preferred_contact_style|notable_topics","old_value": string|null, "new_value": string|null, "confidence": number}]}
Rules:
- Use valid field values only.
- For explicit unemployed status, set primary_company new_value="unemployed".
- Do not return duplicate or empty events.
- If no facts found, return {"events":[]}.
Message: ${source}
`.trim();

  try {
    const completion = await dmLlmClient.complete(prompt);
    const parsed = JSON.parse(completion.content);
    const list = Array.isArray((parsed as any).events) ? (parsed as any).events : [];
    const out: ProfileEvent[] = [];
    for (const evt of list) {
      if (!evt || typeof evt.event_type !== 'string' || !Array.isArray(evt.extracted_facts)) continue;
      const facts = evt.extracted_facts
        .map((f: any) => {
          if (!f || !['primary_company', 'primary_role', 'preferred_contact_style', 'notable_topics'].includes(f.field)) return null;
          return {
            field: f.field as ProfileFact['field'],
            old_value: (typeof f.old_value === 'string' ? f.old_value : f.old_value == null ? null : null),
            new_value: (typeof f.new_value === 'string' ? f.new_value : null),
            confidence: Number(f.confidence ?? evt.confidence ?? 0.7),
          } as ProfileFact;
        })
        .filter((f: ProfileFact | null): f is ProfileFact => f !== null)
        .filter((f: ProfileFact) => Boolean(f.new_value));

      if (!facts.length) continue;
      out.push({
        event_type: evt.event_type,
        confidence: Number(evt.confidence ?? 0.75),
        event_payload: typeof evt.event_payload === 'object' && evt.event_payload ? evt.event_payload : { raw_text: source },
        extracted_facts: facts,
      });
    }
    return out;
  } catch {
    return [];
  }
}

function splitPriorityTopics(raw: string): string[] {
  return raw
    .split(/,|;|\band\b|\&/gi)
    .map((value) => sanitizeEntity(value))
    .filter((value): value is string => Boolean(value))
    .map((value) => value.toLowerCase())
    .filter((value) => value.length >= 2)
    .slice(0, 6);
}

async function extractProfileEventsFromText(text: string | null): Promise<ProfileEvent[]> {
  if (!text) return [];
  const source = text;
  const events: ProfileEvent[] = [];

  // Pattern: "I no longer work at X and now I'm at Y"
  const entityToken = "[A-Za-z0-9 .,'-]{1,120}";
  const twoStep = new RegExp(
    `(?:i(?:'m| am)|i|just)?\s*(?:no longer|not|never|no|didn't)?\s*(?:work|worked|work(ed)?|work for|work at|work with|work there)?\s*(?:at|with|for)?\s*(${entityToken})(?:\.|,)?\s*(?:and|then|and now|now)\s+(?:i(?:'m| am))\s+(?:just\s+)?(?:left|moved|moving|joined|working|work|start(ed)?|switch(?:ed)?|shifted)\s+(?:at|with)?\s*(${entityToken})`,
    'giu',
  );
  for (const m of source.matchAll(twoStep)) {
    const oldCompany = sanitizeEntity(m[1]);
    const newCompany = sanitizeEntity(m[2]);
    if (!newCompany) continue;
    if (oldCompany === newCompany) continue;
    const cleaned = cleanupCompanyName(newCompany);
    events.push({
      event_type: 'profile.company_update',
      confidence: 0.82,
      event_payload: {
        raw_text: text,
        old_company: oldCompany,
        new_company: cleaned,
        trigger: 'left_and_now_working_at',
      },
      extracted_facts: [
        {
          field: 'primary_company',
          old_value: oldCompany,
          new_value: cleaned,
          confidence: 0.82,
        },
      ],
    });
  }

  // Pattern: "now I'm working at X"
  const nowAt = /(?:^|[\s\n])(?:currently|now)\s+(?:i(?:'m| am)\s+)?working\s+at\s+([A-Za-z0-9 .,'-]{1,100})/giu;
  for (const m of source.matchAll(nowAt)) {
    const target = sanitizeEntity(m[1]);
    if (!target) continue;
    const cleaned = cleanupCompanyName(target);
    events.push({
      event_type: 'profile.company_update',
      confidence: 0.78,
      event_payload: {
        raw_text: text,
        new_company: cleaned,
        trigger: 'currently_working_at',
      },
      extracted_facts: [
        {
          field: 'primary_company',
          old_value: null,
          new_value: cleaned,
          confidence: 0.78,
        },
      ],
    });
  }

  // Pattern: "I joined X"
  const joined = /(?:^|[\s\n])(?:i\s+just\s+|i\s+have\s+just\s+|i\s+recently\s+)?joined\s+([A-Za-z0-9 .,'-]{1,100})/giu;
  for (const m of source.matchAll(joined)) {
    const target = sanitizeEntity(m[1]);
    if (!target) continue;
    const cleaned = cleanupCompanyName(target);
    events.push({
      event_type: 'profile.company_update',
      confidence: 0.76,
      event_payload: {
        raw_text: text,
        new_company: cleaned,
        trigger: 'joined',
      },
      extracted_facts: [
        {
          field: 'primary_company',
          old_value: null,
          new_value: cleaned,
          confidence: 0.76,
        },
      ],
    });
  }

  // Pattern: role/company declarations
  const roleAndCompany = /(?:^|[\s\n])(?:i(?:'m|’m| am)|my role(?:\s+is|[’']s)?|my title(?:\s+is|[’']s)?|i work as)\s+(?:a|an|the)?\s*([A-Za-z0-9/&+().,' -]{2,80}?)(?:\s+(?:at|with|for)\s+([A-Za-z0-9 .,'&-]{2,120}?))?(?:[.;!?]|$)/giu;
  for (const m of source.matchAll(roleAndCompany)) {
    const role = normalizeRole(m[1] || '');
    const maybeCompany = sanitizeEntity(m[2] || '');
    const company = maybeCompany ? cleanupCompanyName(maybeCompany) : null;
    const facts: ProfileFact[] = [];

    if (role) {
      facts.push({
        field: 'primary_role',
        old_value: null,
        new_value: role,
        confidence: 0.74,
      });
    }
    if (company) {
      facts.push({
        field: 'primary_company',
        old_value: null,
        new_value: company,
        confidence: 0.74,
      });
    }
    if (facts.length === 0) continue;

    events.push({
      event_type: company ? 'profile.role_company_update' : 'profile.role_update',
      confidence: 0.74,
      event_payload: {
        raw_text: text,
        trigger: 'role_company_statement',
        role: role || null,
        company,
      },
      extracted_facts: facts,
    });
  }

  // Pattern: explicit communication preference
  const commPrefs = /(?:best way to (?:reach|contact) me(?:\s+is)?|i prefer(?:\s+to)? communicate(?:\s+via|\s+on)?|contact me(?:\s+via|\s+on)?|reach me(?:\s+via|\s+on)?|prefer(?:\s+short|\s+concise|\s+detailed)?(?:\s+messages)?)\s+([^.!?\n]{3,100})/giu;
  for (const m of source.matchAll(commPrefs)) {
    const style = normalizeContactStyle(m[1] || '');
    if (!style) continue;
    events.push({
      event_type: 'profile.contact_style_update',
      confidence: 0.69,
      event_payload: {
        raw_text: text,
        trigger: 'contact_style_statement',
        preferred_contact_style: style,
      },
      extracted_facts: [
        {
          field: 'preferred_contact_style',
          old_value: null,
          new_value: style,
          confidence: 0.69,
        },
      ],
    });
  }

  // Parse explicit unemployment statements
  events.push(...parseUnemployedStatement(source));

  // Pattern: priority statements
  const priorities = /(?:my priorities are|i(?:'m| am)\s+focused on|currently focused on|right now\s+i(?:'m| am)\s+focused on)\s+([^.!?\n]{3,180})/giu;
  for (const m of source.matchAll(priorities)) {
    const topics = splitPriorityTopics(m[1] || '');
    if (topics.length === 0) continue;
    events.push({
      event_type: 'profile.priorities_update',
      confidence: 0.66,
      event_payload: {
        raw_text: text,
        trigger: 'priority_statement',
        priorities: topics,
      },
      extracted_facts: topics.map((topic) => ({
        field: 'notable_topics',
        old_value: null,
        new_value: topic,
        confidence: 0.66,
      })),
    });
  }

  const llmEvents = await extractProfileEventsByLLM(source);
  events.push(...llmEvents);


  // dedupe by event type + fact values
  const keySet = new Set<string>();
  const deduped: ProfileEvent[] = [];
  for (const evt of events) {
    const factKey = evt.extracted_facts
      .map((fact) => `${fact.field}:${fact.new_value || ''}:${fact.old_value || ''}`)
      .sort()
      .join('|');
    const k = `${evt.event_type}|${factKey}`;
    if (keySet.has(k)) continue;
    keySet.add(k);
    deduped.push(evt);
  }

  return deduped;
}

async function upsertUser(
  client: any,
  externalId: string,
  handle: string | null,
  displayName: string | null,
): Promise<number> {
  const handleNormalized = toLowerHandle(handle);
  const display = displayName?.trim() || handleNormalized;

  const res = await client.query(
    `INSERT INTO users (platform, external_id, handle, display_name)
     VALUES ('telegram', $1, $2::text, $3::text)
     ON CONFLICT (platform, external_id) DO UPDATE SET
       handle = COALESCE(NULLIF(EXCLUDED.handle, ''), users.handle),
       display_name = COALESCE(NULLIF(EXCLUDED.display_name, ''), users.display_name)
     RETURNING id`,
    [externalId, handleNormalized, display],
  );

  return res.rows[0].id as number;
}

async function addProfileEvents(
  client: any,
  userId: number,
  conversationId: number,
  messageDbId: number,
  messageId: string,
  events: ProfileEvent[],
): Promise<void> {
  for (const evt of events) {
    await client.query(
      `INSERT INTO dm_profile_update_events (
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
       ) VALUES ($1, $2, $3, $4, $5, 'dm_listener', 'user', $6::jsonb, $7::jsonb, $8)
       ON CONFLICT (source_message_id, event_type) WHERE source_message_id IS NOT NULL DO UPDATE SET
         actor_role = EXCLUDED.actor_role,
         event_payload = EXCLUDED.event_payload,
         extracted_facts = EXCLUDED.extracted_facts,
         confidence = EXCLUDED.confidence,
         created_at = now()
      `,
      [
        userId,
        conversationId,
        messageDbId,
        messageId,
        evt.event_type,
        JSON.stringify(evt.event_payload),
        JSON.stringify(evt.extracted_facts),
        evt.confidence,
      ],
    );
  }
}

async function ingestBatch(client: any, events: string[]): Promise<{
  inserted: number;
  skipped: number;
  malformed: number;
  users: number;
  psychEvents: number;
}> {
  let inserted = 0;
  let skipped = 0;
  let malformed = 0;
  let psychEvents = 0;
  const userCache = new Map<string, number>();

  for (const line of events) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const row = parseLine(trimmed);
    if (!row || !validDmDirection(row.direction) || !row.sender_id || !row.peer_id) {
      malformed += 1;
      continue;
    }

    const senderExt = row.sender_id;
    const peerExt = row.peer_id;
    const accountExtFromRow = row.account_id || null;

    const getUserId = async (extId: string, handle: string | null, name: string | null): Promise<number> => {
      const key = extId.toLowerCase();
      const hit = userCache.get(key);
      if (hit) return hit;
      const id = await upsertUser(client, extId, handle, name);
      userCache.set(key, id);
      return id;
    };

    const senderUserId = await getUserId(senderExt, row.sender_username, row.sender_name);
    const peerUserId = await getUserId(peerExt, row.peer_username, row.peer_name);

    const sentAt = new Date(row.date || Date.now()).toISOString();
    const chatId = String(row.chat_id);

    const accountUserExt = accountExtFromRow || (row.direction === 'outbound' ? senderExt : peerExt);
    let subjectUserExt = row.direction === 'outbound' ? peerExt : senderExt;

    if (subjectUserExt === accountUserExt) {
      // Fallback when source payload doesn't include an explicit account_id.
      subjectUserExt = row.direction === 'outbound'
        ? senderExt
        : peerExt;
    }

    const accountUserId = await getUserId(accountUserExt, row.sender_username, row.sender_name);
    let subjectUserId = await getUserId(subjectUserExt, row.sender_username, row.sender_name);

    if (subjectUserId === accountUserId) {
      subjectUserId = accountUserId === senderUserId ? peerUserId : senderUserId;
    }

    const convRes = await client.query(
      `INSERT INTO dm_conversations (
         platform, external_chat_id, user_a_id, user_b_id, last_message_at
       )
       VALUES ('telegram', $1, $2, $3, $4::timestamptz)
       ON CONFLICT (platform, external_chat_id)
       DO UPDATE SET
         user_a_id = EXCLUDED.user_a_id,
         user_b_id = EXCLUDED.user_b_id,
         last_message_at = GREATEST(dm_conversations.last_message_at, EXCLUDED.last_message_at),
         updated_at = now()
       RETURNING id`,
      [
        chatId,
        accountUserId,
        subjectUserId,
        sentAt,
      ],
    );

    if (!convRes.rows.length) {
      skipped += 1;
      continue;
    }

    const convId = convRes.rows[0].id as number;
    const msgText = row.text ?? null;

    const msgRes = await client.query(
      `INSERT INTO dm_messages (
         conversation_id, external_message_id, sender_id, direction, text,
         text_len, sent_at, reply_to_external_message_id,
         views, forwards, has_links, has_mentions, response_status,
         raw_payload
       ) VALUES ($1, $2, $3, $4::text, $5, $6, $7::timestamptz, $8, $9, $10, $11, $12, $13::text, $14::jsonb)
       ON CONFLICT (conversation_id, external_message_id) DO NOTHING`,
      [
        convId,
        String(row.message_id),
        senderUserId,
        row.direction,
        msgText,
        row.text_len ?? 0,
        sentAt,
        row.reply_to_message_id != null ? String(row.reply_to_message_id) : null,
        Number(row.views || 0),
        Number(row.forwards || 0),
        Boolean(row.has_links),
        Boolean(row.has_mentions),
        row.direction === 'inbound' ? 'pending' : 'not_applicable',
        JSON.stringify(row),
      ],
    );

    if (msgRes.rowCount === 1) {
      const msgDbId = await client.query(
        `SELECT id FROM dm_messages WHERE conversation_id=$1 AND external_message_id=$2`,
        [convId, String(row.message_id)],
      );
      const messageDbId = msgDbId.rows[0]?.id as number | undefined;

      // Extract profile-correction signals only for inbound user messages.
      const profileEvents =
        row.direction === 'inbound' ? await extractProfileEventsFromText(msgText) : [];
      if (profileEvents.length > 0 && messageDbId) {
        // subjectUserId is the inbound sender in private DM context
        const targetUserId = row.direction === 'inbound' ? senderUserId : subjectUserId;
        await addProfileEvents(
          client,
          targetUserId,
          convId,
          messageDbId,
          String(row.message_id),
          profileEvents,
        );
        psychEvents += profileEvents.length;
      }

      inserted += 1;
    } else {
      skipped += 1;
    }
  }

  return { inserted, skipped, malformed, users: userCache.size, psychEvents };
}

async function main() {
  const args = parseArgs();
  const filePath = args['file'];

  if (!filePath) {
    console.error(
      'Usage: npm run ingest-dm-jsonl -- --file <data/exports/telethon_dms_live.jsonl> [--state-file <path>]',
    );
    process.exit(1);
  }

  const abs = path.resolve(process.cwd(), filePath);
  const statePath = args['state-file']
    ? path.resolve(process.cwd(), args['state-file'])
    : `${abs}.checkpoint.json`;

  const existingState = await loadIngestState(statePath);
  const fileStats = await fsp.stat(abs);

  const shouldResetState = !(
    existingState
    && existingState.file === abs
    && existingState.lastLine >= 0
    && existingState.fileSize <= fileStats.size
  );

  const startLine = shouldResetState ? 0 : existingState?.lastLine || 0;

  if (existingState && shouldResetState) {
    console.log(
      `⚠️  DM state reset for ${abs}; detected file rollover/size reset (checkpoint at ${existingState.fileSize} bytes, current ${fileStats.size} bytes).`,
    );
  }

  console.log(`📥 Ingesting DM JSONL from: ${abs}`);
  if (startLine > 0) {
    console.log(`   Resuming from line ${startLine + 1} (state: ${statePath}).`);
  } else {
    console.log('   Full scan mode (no resumable state).');
  }

  const readStream = fs.createReadStream(abs, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: readStream });

  const start = Date.now();
  const rows: string[] = [];
  let lineNo = 0;

  for await (const line of rl) {
    lineNo += 1;
    if (lineNo <= startLine) continue;
    rows.push(line);
  }

  const { inserted, skipped, malformed, users, psychEvents } = await db.transaction(async (client) => {
    return ingestBatch(client, rows);
  });

  const elapsedSec = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\n✅ DM JSONL ingest finished`);
  console.log(`   File lines processed: ${lineNo - startLine}`);
  console.log(`   Inserted messages: ${inserted}`);
  console.log(`   Skipped/Duplicates: ${skipped}`);
  console.log(`   Malformed lines: ${malformed}`);
  console.log(`   Profile signal events: ${psychEvents}`);
  console.log(`   Upserted users: ${users}`);
  console.log(`   Time: ${elapsedSec}s`);

  await writeIngestState(statePath, {
    version: 1,
    file: abs,
    lastLine: lineNo,
    fileSize: fileStats.size,
    updatedAt: new Date().toISOString(),
  });

  await db.close();
}

main().catch(async (err) => {
  console.error('❌ DM JSONL ingest failed:', err);
  await db.close();
  process.exit(1);
});
