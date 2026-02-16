#!/usr/bin/env node
import crypto from 'node:crypto';
import { db } from '../db/index.js';
import { parseArgs } from '../utils.js';

interface PsychRow {
  id?: string;
  primary_company: string | null;
  reasoning: string | null;
  generated_bio_professional: string | null;
  generated_bio_personal: string | null;
  primary_role: string | null;
}

interface DbEvent {
  id: string;
  user_id: string;
  event_type: string;
  event_payload: Record<string, unknown>;
  extracted_facts: Array<{
    field: 'primary_company' | 'primary_role';
    old_value: string | null;
    new_value: string | null;
    confidence: number;
  }>;
  confidence: number;
  created_at: string;
}

function sanitizeCompany(value: string | null): string {
  return (value || '').replace(/[\t\n\r]+/g, ' ').replace(/[.]$/, '').trim();
}

function applyCompanyShift(base: string | null, value: string): string {
  return sanitizeCompany(value);
}

function rewriteBioText(text: string | null, oldCompany: string | null, newCompany: string | null): string | null {
  if (!text || !oldCompany || !newCompany) return text;
  const escaped = oldCompany.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(escaped, 'gi');
  return text.replace(re, newCompany).trim();
}

function replaceInReasoning(reasoning: string | null, oldCompany: string | null, newCompany: string | null): string {
  const normalized = reasoning ? reasoning.trim() : '';
  if (!oldCompany || !newCompany) {
    const tail = `[DM profile-correction ${new Date().toISOString()}] Applied inferred profile update: company=${sanitizeCompany(newCompany)}.`;
    return normalized ? `${normalized}\n${tail}` : tail;
  }
  const rew = rewriteBioText(normalized || null, oldCompany, newCompany) || normalized;
  const tail = `[DM profile-correction ${new Date().toISOString()}] Replaced '${oldCompany}' -> '${newCompany}'.`;
  return rew ? `${rew}\n${tail}` : tail;
}

function applyProfilePatch(base: PsychRow, events: DbEvent[]): { primary_company: string | null; reasoning: string; generated_bio_professional: string | null; generated_bio_personal: string | null; }
{
  let nextCompany = base.primary_company;
  let reasoning = base.reasoning || '';
  let prof = base.generated_bio_professional;
  let personal = base.generated_bio_personal;

  for (const evt of events) {
    for (const fact of evt.extracted_facts || []) {
      if (fact.field !== 'primary_company') continue;
      const newCompany = sanitizeCompany(fact.new_value);
      if (!newCompany) continue;
      const oldCompany = nextCompany || fact.old_value;

      if (nextCompany && nextCompany !== newCompany) {
        reasoning = replaceInReasoning(reasoning, oldCompany, newCompany);
        prof = rewriteBioText(prof, oldCompany, newCompany);
        personal = rewriteBioText(personal, oldCompany, newCompany);
      }

      nextCompany = applyCompanyShift(nextCompany, newCompany);
      reasoning = replaceInReasoning(reasoning, oldCompany, newCompany);
      const confidence = evt.confidence;
      reasoning += `\n[DM event #${evt.id}] source_confidence=${confidence.toFixed(2)} payload=${JSON.stringify(evt.event_payload)}.`;
    }
  }

  return {
    primary_company: nextCompany || base.primary_company,
    reasoning,
    generated_bio_professional: prof || base.generated_bio_professional,
    generated_bio_personal: personal || base.generated_bio_personal,
  };
}

function makePromptHash(text: string): string {
  return crypto.createHash('sha256').update(text).digest('hex');
}

async function reconcileUsers(targetUserIds: string[] = [], limit = 0): Promise<number> {
  const criteria = targetUserIds.length > 0
    ? 'e.user_id = ANY($1) AND e.processed = false'
    : 'e.processed = false';

  const params: unknown[] = targetUserIds.length > 0 ? [targetUserIds] : [];
  const usersRes = await db.query<{ user_id: string }>(
    `SELECT DISTINCT e.user_id
     FROM dm_profile_update_events e
     WHERE ${criteria}
     ORDER BY e.user_id
     ${limit > 0 ? `LIMIT ${targetUserIds.length > 0 ? '$2' : '$1'}` : ''}`,
    limit > 0
      ? [...params, limit]
      : params,
  );

  let reconciled = 0;

  for (const user of usersRes.rows) {
    const userId = user.user_id;

    const eventsRes = await db.query<DbEvent>(
      `SELECT id::text, user_id::text, event_type, event_payload::jsonb as event_payload,
              extracted_facts::jsonb as extracted_facts, confidence, created_at::text
       FROM dm_profile_update_events
       WHERE user_id = $1 AND processed = false
       ORDER BY id ASC`,
      [userId],
    );

    if (!eventsRes.rows.length) continue;

    const latestProfileRes = await db.query<PsychRow>(
      `SELECT primary_company, reasoning, generated_bio_professional, generated_bio_personal, primary_role
       FROM user_psychographics
       WHERE user_id = $1
       ORDER BY created_at DESC
       LIMIT 1`,
      [userId],
    );

    const latest = latestProfileRes.rows[0] || {
      primary_company: null,
      reasoning: null,
      generated_bio_professional: null,
      generated_bio_personal: null,
      primary_role: null,
    };

    const patch = applyProfilePatch(latest, eventsRes.rows);

    const snapshot = {
      source: 'dm_profile_reconciler',
      user_id: userId,
      event_count: eventsRes.rows.length,
      events: eventsRes.rows.map((e) => ({
        id: e.id,
        type: e.event_type,
        payload: e.event_payload,
        created_at: e.created_at,
      })),
      applied_at: new Date().toISOString(),
    };

    const reasoningText = patch.reasoning || `Auto-reconciled from ${eventsRes.rows.length} DM profile correction events.`;
    const promptHash = makePromptHash(JSON.stringify(snapshot));

    const insertedProfile = await db.query<{ id: string }>(
      `INSERT INTO user_psychographics (
         user_id, model_name, prompt_hash, primary_company, reasoning,
         generated_bio_professional, generated_bio_personal, primary_role, raw_response, latency_ms
       ) VALUES ($1, 'dm-event-reconciler', $2, $3, $4, $5, $6, $7, $8, 0)
       RETURNING id`,
      [
        userId,
        promptHash,
        patch.primary_company,
        reasoningText,
        patch.generated_bio_professional,
        patch.generated_bio_personal,
        latest.primary_role,
        `Applied ${eventsRes.rows.length} DM-derived profile correction(s).`,
      ],
    );

    const newProfileId = insertedProfile.rows[0]?.id;
    const eventIds = eventsRes.rows.map((r) => r.id);

    await db.query(
      `INSERT INTO dm_profile_state (user_id, last_profile_event_id, user_psychographics_id, snapshot)
       VALUES ($1, $2, $3, $4::jsonb)
       ON CONFLICT (user_id)
       DO UPDATE SET
         last_profile_event_id = EXCLUDED.last_profile_event_id,
         user_psychographics_id = EXCLUDED.user_psychographics_id,
         snapshot = EXCLUDED.snapshot,
         updated_at = now()`,
      [
        userId,
        eventsRes.rows[eventsRes.rows.length - 1].id,
        newProfileId,
        JSON.stringify(snapshot),
      ],
    );

    await db.query(
      `UPDATE dm_profile_update_events
       SET processed = true
       WHERE id = ANY($1::bigint[])`,
      [eventIds],
    );

    reconciled += 1;
    console.log(`reconciled user ${userId} with ${eventsRes.rows.length} events -> user_psychographics#${newProfileId}`);
  }

  return reconciled;
}

async function main(): Promise<void> {
  const args = parseArgs();
  const rawLimit = args['limit'];
  const limit = rawLimit ? parseInt(rawLimit, 10) : 0;
  const rawUsers = args['user-ids'] || args['user_ids'] || args['userIds'];
  const userIds = rawUsers ? rawUsers.split(',').map((v) => v.trim()).filter(Boolean) : [];

  console.log('\n🧠 DM psychometry reconcile started');
  const count = await reconcileUsers(userIds, Number.isFinite(limit) ? limit : 0);
  console.log(`\n✅ Reconcile complete: ${count} user(s) updated`);
  await db.close();
}

main().catch(async (err) => {
  console.error('❌ Reconcile failed:', err);
  await db.close();
  process.exit(1);
});
