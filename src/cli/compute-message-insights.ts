#!/usr/bin/env node
/**
 * Pass B: Compute message_insights for distilled candidate messages.
 *
 * Usage:
 *   npm run compute-message-insights
 *   npm run compute-message-insights -- --user-id 123
 *   npm run compute-message-insights -- --user-ids 954,2011
 *   npm run compute-message-insights -- --limit 50
 *   npm run compute-message-insights -- --llm true --model deepseek/deepseek-chat
 */

import { db } from '../db/index.js';
import { parseArgs } from '../utils.js';
import { buildCandidateSet, type MessageRow } from '../analysis/message-distill.js';
import { createHash } from 'node:crypto';
import { createLLMClient } from '../inference/llm-client.js';

const URL_RE = /https?:\/\/[^\s)>\]]+/gi;
const HANDLE_RE = /@([a-zA-Z0-9_]{3,32})/g;

const IDENTITY_RE = /\b(my name is|i am|i'm|im )\b/i;
const EMPLOYMENT_RE = /\b(i work at|i work for|i'm at|im at|joining|joined|starting at|now at|currently at)\b/i;
const ROLE_RE = /\b(founder|cofounder|head of|lead|manager|director|partner(?:ship)?|ceo|cto|cmo|cfo|engineer|developer|dev|product|growth|marketing|bd|sales|investor|analyst)\b/i;
const HIRING_RE = /\b(we are hiring|looking for|send me your cv|recruit)\b/i;
const CONTACT_RE = /\b(dm me|reach out|intro|contact me|email me|ping me)\b/i;
const EVENTS_RE = /\b(devcon|ethdenver|token2049|denver|conference|summit|hackathon)\b/i;
const TECH_RE = /\b(solidity|rust|zk|rollup|protocol|node|react|python|typescript|postgres)\b/i;
const OPINION_RE = /\b(i think|imo|opinion|hot take|bullish|bearish)\b/i;
const MOD_RE = /\b(ban|mute|admin|moderator|mod|rules)\b/i;

const GREETING_ONLY = new Set(['gm', 'gn', 'lol', 'lfg', 'ty', 'thx', 'thanks', 'ok', 'kk', 'haha', 'yo', 'sup', 'hi', 'hey']);
const EMOJI_PUNCT_ONLY_RE = /^[\p{Emoji}\p{P}\p{S}\s]+$/u;
const EMAIL_RE = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i;
const ENS_RE = /\b[a-z0-9-]+\.eth\b/i;
const DOMAIN_RE = /\b[a-z0-9.-]+\.[a-z]{2,}\b/i;
const LANGUAGE_RE = /\b(english|spanish|french|german|chinese|mandarin|cantonese|korean|japanese|russian|arabic|portuguese|hindi)\b/i;
const CITY_COUNTRY_RE = /\b(new york|london|paris|berlin|singapore|tokyo|seoul|dubai|nyc|sf|san francisco|los angeles|austin|miami)\b/i;
const FACT_TOKEN_RE = /^[A-Z][A-Za-z0-9&.\-]{2,}$/;

const CLASSIFIER_VERSION = 'v2';

function textHash(text: string): string {
  return createHash('sha256').update(text).digest('hex');
}

function extractUrls(text: string): string[] {
  URL_RE.lastIndex = 0;
  const matches = text.match(URL_RE) ?? [];
  return [...new Set(matches)];
}

function extractHandles(text: string): string[] {
  HANDLE_RE.lastIndex = 0;
  const matches = [...text.matchAll(HANDLE_RE)].map((m) => m[1].toLowerCase());
  return [...new Set(matches)];
}

function hasUrl(text: string): boolean {
  URL_RE.lastIndex = 0;
  return URL_RE.test(text);
}

function hasHandle(text: string): boolean {
  HANDLE_RE.lastIndex = 0;
  return HANDLE_RE.test(text);
}

function isNoise(text: string, hasCritical: boolean): boolean {
  const t = text.trim().toLowerCase();
  if (!t) return true;
  if (hasCritical) return false;
  if (isFactToken(text)) return false;
  if (GREETING_ONLY.has(t)) return true;
  if (t.length <= 4 && GREETING_ONLY.has(t.replace(/[^\w]/g, ''))) return true;
  if (EMOJI_PUNCT_ONLY_RE.test(t)) return true;
  return false;
}

function firstPersonScore(text: string): number {
  const words = text.split(/\s+/).filter(Boolean);
  if (words.length === 0) return 0;
  const fp = text.match(/\b(i|my|me|i'm|im|i am)\b/gi)?.length ?? 0;
  return Math.min(1, fp / Math.max(5, words.length));
}

function extractOrgs(text: string): string[] {
  const orgs: string[] = [];
  const re = /\b(at|from|with|joining|joined|work(?:ing)? at|work(?:ing)? for|now at|currently at)\s+([A-Z][\w&.\-]{1,40}(?:\s+[A-Z][\w&.\-]{1,40}){0,4})/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    orgs.push(m[2].trim());
  }
  return [...new Set(orgs)];
}

function extractRoles(text: string): string[] {
  const roles: string[] = [];
  const re = /\b(founder|cofounder|ceo|cto|cmo|cfo|head of [a-z ]+|lead [a-z ]+|director|manager|engineer|developer|bd|sales|investor|analyst)\b/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    roles.push(m[0].trim());
  }
  return [...new Set(roles.map((r) => r.toLowerCase()))];
}

function signalTypes(text: string): string[] {
  const types = new Set<string>();
  if (IDENTITY_RE.test(text)) types.add('identity');
  if (EMPLOYMENT_RE.test(text)) types.add('career');
  if (ROLE_RE.test(text)) types.add('career');
  if (HIRING_RE.test(text)) types.add('hiring');
  if (CONTACT_RE.test(text) || hasUrl(text) || hasHandle(text)) types.add('contact');
  if (EVENTS_RE.test(text)) types.add('events');
  if (TECH_RE.test(text)) types.add('technical');
  if (OPINION_RE.test(text)) types.add('opinion');
  if (MOD_RE.test(text)) types.add('moderation');
  if (isFactToken(text)) types.add('identity');
  return [...types];
}

function isFactToken(text: string): boolean {
  const t = text.trim();
  if (EMAIL_RE.test(t) || ENS_RE.test(t) || DOMAIN_RE.test(t)) return true;
  if (LANGUAGE_RE.test(t) || CITY_COUNTRY_RE.test(t)) return true;
  if (FACT_TOKEN_RE.test(t)) return true;
  return false;
}

async function classifyWithLLM(messages: MessageRow[], model: string) {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) throw new Error('Missing OPENROUTER_API_KEY for LLM classification.');

  const llm = createLLMClient({
    apiKeys: [apiKey],
    model,
    maxRetries: 3,
    retryDelayMs: 500,
    requestDelayMs: 100,
  });

  const payload = messages.map((m) => ({
    message_id: m.id,
    sent_at: m.sent_at,
    group_title: m.group_title ?? '',
    text: m.text ?? '',
  }));

  const prompt = `You are a message classifier. Return ONLY JSON.
Input: array of {message_id, sent_at, group_title, text}
Output: array of {
  message_id: number,
  is_noise: boolean,
  signal_types: string[],
  extracted_orgs: string[],
  extracted_roles: string[],
  extracted_social_urls: string[],
  confidence: number
}
Valid signal_types: identity, career, contact, hiring, events, technical, opinion, moderation, social.
Return [] if no messages.`;

  const response = await llm.complete(`${prompt}\n\nINPUT:\n${JSON.stringify(payload)}`);
  try {
    const parsed = JSON.parse(response.content);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function main(): Promise<void> {
  const args = parseArgs();
  const userId = args['user-id'] ? parseInt(args['user-id'], 10) : null;
  const userIds = parseUserIds(args['user-ids']);
  if (userId && !userIds.includes(userId)) userIds.push(userId);
  const limit = args['limit'] ? parseInt(args['limit'], 10) : 0;
  const useLlm = args['llm'] === 'true';
  const model = args['model'] || 'deepseek/deepseek-chat';

  let userRows: { id: number }[] = [];
  if (userIds.length > 0) {
    const { rows } = await db.query<{ id: number }>('SELECT id FROM users WHERE id = ANY($1) ORDER BY id', [userIds]);
    userRows = rows;
  } else {
    const usersQuery = `SELECT DISTINCT user_id as id FROM messages ORDER BY user_id ${limit > 0 ? `LIMIT ${limit}` : ''}`;
    const { rows } = await db.query<{ id: number }>(usersQuery);
    userRows = rows;
  }
  console.log(`\n🧠 Computing message insights for ${userRows.length} users...`);

  for (const u of userRows) {
    const candidates = await buildCandidateSet(u.id);
    if (candidates.length === 0) continue;

    const ids = candidates.map((c) => c.id);
    const { rows: existing } = await db.query<{ message_id: number; text_hash: string; classifier_version: string }>(
      'SELECT message_id, text_hash, classifier_version FROM message_insights WHERE message_id = ANY($1)',
      [ids],
    );
    const existingMap = new Map(existing.map((r) => [r.message_id, r]));

    const toProcess = candidates.filter((c) => {
      const text = c.text ?? '';
      const hash = textHash(text);
      const existingRow = existingMap.get(c.id);
      if (!existingRow) return true;
      if (existingRow.text_hash !== hash) return true;
      if (existingRow.classifier_version !== CLASSIFIER_VERSION) return true;
      return false;
    });

    let llmMap = new Map<number, any>();
    if (useLlm && toProcess.length > 0) {
      const batchSize = 50;
      for (let i = 0; i < toProcess.length; i += batchSize) {
        const batch = toProcess.slice(i, i + batchSize);
        const llmOut = await classifyWithLLM(batch, model);
        for (const r of llmOut) llmMap.set(r.message_id, r);
      }
    }

    for (const m of toProcess) {
      const text = m.text ?? '';
      const urls = extractUrls(text);
      const handles = extractHandles(text);
      const hasCritical = urls.length > 0 || handles.length > 0 || isIdentityOrCareer(text);
      const sig = signalTypes(text);
      const orgs = extractOrgs(text);
      const roles = extractRoles(text);
      const noise = isNoise(text, hasCritical);
      const fps = firstPersonScore(text);
      const hash = textHash(text);

      const llm = llmMap.get(m.id);
      const final = {
        is_noise: llm?.is_noise ?? noise,
        signal_types: Array.from(new Set([...(sig ?? []), ...(llm?.signal_types ?? [])])),
        extracted_orgs: Array.from(new Set([...(orgs ?? []), ...(llm?.extracted_orgs ?? [])])),
        extracted_roles: Array.from(new Set([...(roles ?? []), ...(llm?.extracted_roles ?? [])])),
        extracted_urls: Array.from(new Set([...(urls ?? []), ...(llm?.extracted_social_urls ?? [])])),
        extracted_handles: handles,
        llm_confidence: llm?.confidence ?? null,
      };

      await db.query(
        `INSERT INTO message_insights
           (message_id, user_id, sent_at, group_id, text_hash, classifier_version, is_noise, signal_types,
            extracted_urls, extracted_handles, extracted_orgs, extracted_roles, first_person_score, llm_confidence, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,now())
         ON CONFLICT (message_id) DO UPDATE
         SET text_hash=EXCLUDED.text_hash,
             classifier_version=EXCLUDED.classifier_version,
             is_noise=EXCLUDED.is_noise,
             signal_types=EXCLUDED.signal_types,
             extracted_urls=EXCLUDED.extracted_urls,
             extracted_handles=EXCLUDED.extracted_handles,
             extracted_orgs=EXCLUDED.extracted_orgs,
             extracted_roles=EXCLUDED.extracted_roles,
             first_person_score=EXCLUDED.first_person_score,
             llm_confidence=EXCLUDED.llm_confidence,
             updated_at=now()`,
        [
          m.id, u.id, m.sent_at, m.group_id, hash, CLASSIFIER_VERSION,
          final.is_noise, final.signal_types,
          final.extracted_urls, final.extracted_handles,
          final.extracted_orgs, final.extracted_roles,
          fps, final.llm_confidence,
        ],
      );
    }

    console.log(`   ✅ user ${u.id}: ${toProcess.length}/${candidates.length} insights updated`);
  }

  await db.close();
}

function isIdentityOrCareer(text: string): boolean {
  return IDENTITY_RE.test(text) || EMPLOYMENT_RE.test(text) || ROLE_RE.test(text) || HIRING_RE.test(text);
}

function parseUserIds(raw: string | undefined): number[] {
  if (!raw) return [];
  const ids = raw
    .split(',')
    .map((value) => parseInt(value.trim(), 10))
    .filter((id) => Number.isFinite(id) && id > 0);
  return [...new Set(ids)];
}

main().catch((err) => {
  console.error('❌ compute-message-insights failed:', err);
  process.exit(1);
});
