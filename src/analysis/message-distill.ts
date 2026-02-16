/**
 * Pass A: High-recall message selection without LLMs.
 * Ensures recent, identity/contact, and diverse samples are retained.
 */

import { db } from '../db/index.js';

export type MessageRow = {
  id: number;
  sent_at: string;
  text: string | null;
  group_id: number;
  group_title: string | null;
  reaction_count: number | null;
  reply_count: number | null;
};

const URL_RE = /https?:\/\/[^\s)>\]]+/gi;
const HANDLE_RE = /@([a-zA-Z0-9_]{3,32})/g;

const IDENTITY_RE = /\b(my name is|i am|i'm|im )\b/i;
const EMPLOYMENT_RE = /\b(i work at|i work for|i'm at|im at|joining|joined|starting at|now at|currently at)\b/i;
const ROLE_RE = /\b(founder|cofounder|head of|lead|manager|director|partner(?:ship)?|ceo|cto|cmo|cfo)\b/i;
const HIRING_RE = /\b(we are hiring|looking for|send me your cv|recruit)\b/i;

const CONTACT_RE = /\b(dm me|reach out|intro|contact me|email me|ping me)\b/i;

const GREETING_ONLY = new Set([
  'gm', 'gn', 'lol', 'lfg', 'ty', 'thx', 'thanks', 'ok', 'kk', 'haha', 'yo', 'sup', 'hi', 'hey',
]);

const EMOJI_PUNCT_ONLY_RE = /^[\p{Emoji}\p{P}\p{S}\s]+$/u;
const EMAIL_RE = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i;
const ENS_RE = /\b[a-z0-9-]+\.eth\b/i;
const DOMAIN_RE = /\b[a-z0-9.-]+\.[a-z]{2,}\b/i;
const LANGUAGE_RE = /\b(english|spanish|french|german|chinese|mandarin|cantonese|korean|japanese|russian|arabic|portuguese|hindi)\b/i;
const CITY_COUNTRY_RE = /\b(new york|london|paris|berlin|singapore|tokyo|seoul|dubai|nyc|sf|san francisco|los angeles|austin|miami)\b/i;
const FACT_TOKEN_RE = /^[A-Z][A-Za-z0-9&.\-]{2,}$/;

function hasUrl(text: string): boolean {
  URL_RE.lastIndex = 0;
  return URL_RE.test(text);
}

function hasHandle(text: string): boolean {
  HANDLE_RE.lastIndex = 0;
  return HANDLE_RE.test(text);
}

function isIdentityOrCareer(text: string): boolean {
  return IDENTITY_RE.test(text) || EMPLOYMENT_RE.test(text) || ROLE_RE.test(text) || HIRING_RE.test(text);
}

function isContact(text: string): boolean {
  return hasUrl(text) || hasHandle(text) || CONTACT_RE.test(text);
}

function isFactToken(text: string): boolean {
  const t = text.trim();
  if (EMAIL_RE.test(t) || ENS_RE.test(t) || DOMAIN_RE.test(t)) return true;
  if (LANGUAGE_RE.test(t) || CITY_COUNTRY_RE.test(t)) return true;
  if (FACT_TOKEN_RE.test(t)) return true;
  return false;
}

function isNoise(text: string): boolean {
  const t = text.trim().toLowerCase();
  if (!t) return true;
  if (isFactToken(text)) return false;
  if (GREETING_ONLY.has(t)) return true;
  if (t.length <= 4 && GREETING_ONLY.has(t.replace(/[^\w]/g, ''))) return true;
  if (EMOJI_PUNCT_ONLY_RE.test(t)) return true;
  return false;
}

function timeBucket(sentAt: Date, coarse: boolean): string {
  const y = sentAt.getUTCFullYear();
  if (coarse) {
    const q = Math.floor(sentAt.getUTCMonth() / 3) + 1;
    return `${y}-Q${q}`;
  }
  const m = String(sentAt.getUTCMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

type Candidate = MessageRow & { weight: number };

function pushCandidate(map: Map<number, Candidate>, row: MessageRow, weight: number) {
  const existing = map.get(row.id);
  if (!existing || existing.weight < weight) {
    map.set(row.id, { ...row, weight });
  }
}

export async function buildCandidateSet(userId: number, maxCandidates = 500): Promise<MessageRow[]> {
  const { rows } = await db.query<MessageRow>(`
    SELECT m.id, m.sent_at::text as sent_at, m.text, m.group_id,
           g.title as group_title,
           COALESCE(m.reaction_count, 0) as reaction_count,
           COALESCE(m.reply_count, 0) as reply_count
    FROM messages m
    JOIN groups g ON g.id = m.group_id
    WHERE m.user_id = $1
    ORDER BY m.sent_at DESC
  `, [userId]);

  if (rows.length === 0) return [];

  const map = new Map<number, Candidate>();
  const now = new Date();

  // (a) Always include recent messages
  const last50 = rows.slice(0, 50);
  for (const r of last50) pushCandidate(map, r, 5);

  const last30Days = rows.filter((r) => {
    const d = new Date(r.sent_at);
    return (now.getTime() - d.getTime()) <= 30 * 24 * 60 * 60 * 1000;
  }).slice(0, 10);
  for (const r of last30Days) pushCandidate(map, r, 4);

  // (b) Identity/career/hiring matches across all time
  for (const r of rows) {
    const text = r.text ?? '';
    if (text && isIdentityOrCareer(text)) pushCandidate(map, r, 5);
  }

  // (c) Contact patterns
  for (const r of rows) {
    const text = r.text ?? '';
    if (text && isContact(text)) pushCandidate(map, r, 5);
  }

  // (d) Diversity sampling by time bucket
  const coarse = rows.length > 2000;
  const buckets = new Map<string, MessageRow[]>();
  for (const r of rows) {
    const text = r.text ?? '';
    if (!text) continue;
    if (isNoise(text) && !isIdentityOrCareer(text) && !isContact(text) && !isFactToken(text)) continue;
    const key = timeBucket(new Date(r.sent_at), coarse);
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key)!.push(r);
  }
  for (const [, msgs] of buckets) {
    msgs.sort((a, b) => new Date(b.sent_at).getTime() - new Date(a.sent_at).getTime());
    for (const r of msgs.slice(0, 3)) pushCandidate(map, r, 2);
  }

  // (e) High engagement (cap)
  const topEngagement = [...rows]
    .filter((r) => (r.reaction_count ?? 0) + (r.reply_count ?? 0) > 0)
    .sort((a, b) => ((b.reaction_count ?? 0) + (b.reply_count ?? 0)) - ((a.reaction_count ?? 0) + (a.reply_count ?? 0)))
    .slice(0, 20);
  for (const r of topEngagement) pushCandidate(map, r, 3);

  // Final dedupe and cap
  const candidates = [...map.values()];
  candidates.sort((a, b) => (b.weight - a.weight) || (new Date(b.sent_at).getTime() - new Date(a.sent_at).getTime()));

  return candidates.slice(0, maxCandidates).map(({ weight, ...rest }) => rest);
}
