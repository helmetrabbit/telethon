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
  preferred_contact_style: string | null;
  notable_topics: unknown;
}

interface DbEvent {
  id: string;
  user_id: string;
  event_type: string;
  event_payload: Record<string, unknown>;
  extracted_facts: Array<{
    field: 'primary_company' | 'primary_role' | 'preferred_contact_style' | 'notable_topics';
    old_value: string | null;
    new_value: string | null;
    confidence: number;
  }>;
  confidence: number;
  created_at: string;
}

interface ContactStyleAuditEntry {
  value: string;
  updated_at: string;
  confidence: number | null;
  source: string;
  source_event_id: string;
}

interface ContactStylePendingCandidate {
  value: string;
  confidence: number | null;
  source: string;
  source_event_id: string;
  source_message_id: number | null;
  proposed_at: string;
}

type ContactStyleResolutionRule = 'confidence_gated_last_write_wins';

interface ContactStyleAudit {
  value: string | null;
  updated_at: string | null;
  confidence: number | null;
  source: string | null;
  source_event_id: string | null;
  resolution_rule: ContactStyleResolutionRule;
  reconfirm_prompted_at: string | null;
  pending_candidate: ContactStylePendingCandidate | null;
  history: ContactStyleAuditEntry[];
}

const ONBOARDING_REQUIRED_FIELDS = ['primary_role', 'primary_company', 'notable_topics', 'preferred_contact_style'] as const;
const STYLE_CONFLICT_RESOLUTION_RULE: ContactStyleResolutionRule = 'confidence_gated_last_write_wins';

function parseEnvFloat(name: string, fallback: number): number {
  const raw = (process.env[name] || '').trim();
  if (!raw) return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function clamp01(value: number): number {
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}

const CONTACT_STYLE_AUTO_APPLY_THRESHOLD = clamp01(parseEnvFloat('DM_CONTACT_STYLE_AUTO_APPLY_THRESHOLD', 0.8));
const CONTACT_STYLE_CONFIRM_THRESHOLD = Math.min(
  CONTACT_STYLE_AUTO_APPLY_THRESHOLD,
  clamp01(parseEnvFloat('DM_CONTACT_STYLE_CONFIRM_THRESHOLD', 0.55)),
);

function sanitizeCompany(value: string | null): string {
  return (value || '').replace(/[\t\n\r]+/g, ' ').replace(/[.]$/, '').trim();
}

function sanitizeRole(value: string | null): string {
  return (value || '').replace(/[\t\n\r]+/g, ' ').replace(/[.]$/, '').trim();
}

function sanitizeContactStyle(value: string | null): string {
  return (value || '').replace(/[\t\n\r]+/g, ' ').replace(/[.]$/, '').trim();
}

function normalizeTopics(value: unknown): string[] {
  if (typeof value === 'string') {
    const raw = value.trim();
    if (!raw) return [];
    if (raw.startsWith('[')) {
      try {
        const parsed = JSON.parse(raw);
        return normalizeTopics(parsed);
      } catch {
        // fall through to split heuristic
      }
    }
    return raw
      .split(/,|;|\band\b|\&/gi)
      .map((token) => token.replace(/[\t\n\r]+/g, ' ').trim().toLowerCase())
      .filter((token) => token.length >= 2)
      .slice(0, 10);
  }

  if (!Array.isArray(value)) return [];
  const out = new Set<string>();
  for (const item of value) {
    if (typeof item !== 'string') continue;
    const clean = item.replace(/[\t\n\r]+/g, ' ').trim().toLowerCase();
    if (!clean) continue;
    out.add(clean);
  }
  return [...out];
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

function applyProfilePatch(base: PsychRow, events: DbEvent[]): {
  primary_company: string | null;
  primary_role: string | null;
  preferred_contact_style: string | null;
  notable_topics: string[];
  reasoning: string;
  generated_bio_professional: string | null;
  generated_bio_personal: string | null;
  style_events_seen: boolean;
  contact_style_audit: ContactStyleAudit;
}
{
  let nextCompany = base.primary_company;
  let nextRole = base.primary_role;
  let nextContactStyle = base.preferred_contact_style;
  const nextTopics = new Set<string>(normalizeTopics(base.notable_topics));
  let reasoning = base.reasoning || '';
  let prof = base.generated_bio_professional;
  let personal = base.generated_bio_personal;
  let styleLastUpdatedAt: string | null = null;
  let styleSourceEventId: string | null = null;
  let styleConfidence: number | null = null;
  let styleEventsSeen = false;
  let pendingCandidate: ContactStylePendingCandidate | null = null;
  const styleHistory: ContactStyleAuditEntry[] = [];

  for (const evt of events) {
    for (const fact of evt.extracted_facts || []) {
      if (fact.field === 'primary_company') {
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
      } else if (fact.field === 'primary_role') {
        const newRole = sanitizeRole(fact.new_value);
        if (!newRole) continue;
        if (nextRole !== newRole) {
          reasoning += `\n[DM profile-correction ${new Date().toISOString()}] Updated role -> '${newRole}'.`;
          nextRole = newRole;
        }
      } else if (fact.field === 'preferred_contact_style') {
        const newContactStyle = sanitizeContactStyle(fact.new_value);
        if (!newContactStyle) continue;
        styleEventsSeen = true;
        const eventConfidence = Number.isFinite(Number(fact.confidence))
          ? Number(fact.confidence)
          : Number(evt.confidence);
        styleHistory.push({
          value: newContactStyle,
          updated_at: evt.created_at || new Date().toISOString(),
          confidence: Number.isFinite(eventConfidence) ? eventConfidence : null,
          source: 'dm_profile_update_events',
          source_event_id: evt.id,
        });
        if (Number.isFinite(eventConfidence) && eventConfidence >= CONTACT_STYLE_AUTO_APPLY_THRESHOLD) {
          styleLastUpdatedAt = evt.created_at || new Date().toISOString();
          styleSourceEventId = evt.id;
          styleConfidence = eventConfidence;
          pendingCandidate = null;
          if (nextContactStyle !== newContactStyle) {
            reasoning += `\n[DM profile-correction ${new Date().toISOString()}] Updated preferred_contact_style -> '${newContactStyle}' via ${STYLE_CONFLICT_RESOLUTION_RULE} (confidence=${eventConfidence.toFixed(2)}).`;
            nextContactStyle = newContactStyle;
          }
        } else {
          const confidenceBand = Number.isFinite(eventConfidence) && eventConfidence >= CONTACT_STYLE_CONFIRM_THRESHOLD
            ? 'medium'
            : 'low';
          pendingCandidate = {
            value: newContactStyle,
            confidence: Number.isFinite(eventConfidence) ? eventConfidence : null,
            source: 'dm_profile_update_events',
            source_event_id: evt.id,
            source_message_id: null,
            proposed_at: evt.created_at || new Date().toISOString(),
          };
          const confidenceLabel = Number.isFinite(eventConfidence)
            ? eventConfidence.toFixed(2)
            : 'unknown';
          reasoning += `\n[DM profile-correction ${new Date().toISOString()}] Queued preferred_contact_style candidate '${newContactStyle}' for confirmation (${STYLE_CONFLICT_RESOLUTION_RULE}, band=${confidenceBand}, confidence=${confidenceLabel}, auto_apply_threshold=${CONTACT_STYLE_AUTO_APPLY_THRESHOLD.toFixed(2)}).`;
        }
      } else if (fact.field === 'notable_topics') {
        const topic = (fact.new_value || '').trim().toLowerCase();
        if (!topic) continue;
        if (!nextTopics.has(topic)) {
          nextTopics.add(topic);
          reasoning += `\n[DM profile-correction ${new Date().toISOString()}] Added notable topic '${topic}'.`;
        }
      }
      const confidence = evt.confidence;
      reasoning += `\n[DM event #${evt.id}] source_confidence=${confidence.toFixed(2)} payload=${JSON.stringify(evt.event_payload)}.`;
    }
  }

  return {
    primary_company: nextCompany || base.primary_company,
    primary_role: nextRole || base.primary_role,
    preferred_contact_style: nextContactStyle || base.preferred_contact_style,
    notable_topics: [...nextTopics],
    reasoning,
    generated_bio_professional: prof || base.generated_bio_professional,
    generated_bio_personal: personal || base.generated_bio_personal,
    style_events_seen: styleEventsSeen,
    contact_style_audit: {
      value: nextContactStyle || base.preferred_contact_style || null,
      updated_at: styleLastUpdatedAt,
      confidence: styleConfidence,
      source: styleSourceEventId ? 'dm_profile_update_events' : null,
      source_event_id: styleSourceEventId,
      resolution_rule: STYLE_CONFLICT_RESOLUTION_RULE,
      reconfirm_prompted_at: null,
      pending_candidate: pendingCandidate,
      history: styleHistory.slice(-12),
    },
  };
}

function makePromptHash(text: string): string {
  return crypto.createHash('sha256').update(text).digest('hex');
}

function computeMissingOnboardingFields(next: {
  primary_company: string | null;
  primary_role: string | null;
  preferred_contact_style: string | null;
  notable_topics: string[];
}): string[] {
  const missing: string[] = [];
  if (!sanitizeRole(next.primary_role)) missing.push('primary_role');
  if (!sanitizeCompany(next.primary_company)) missing.push('primary_company');
  if (!Array.isArray(next.notable_topics) || next.notable_topics.length === 0) missing.push('notable_topics');
  if (!sanitizeContactStyle(next.preferred_contact_style)) missing.push('preferred_contact_style');
  return missing;
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

    const existingStateRes = await db.query<{ snapshot: Record<string, unknown> | null }>(
      `SELECT snapshot::jsonb AS snapshot
       FROM dm_profile_state
       WHERE user_id = $1
       LIMIT 1`,
      [userId],
    );
    const existingSnapshot = (existingStateRes.rows[0]?.snapshot && typeof existingStateRes.rows[0].snapshot === 'object')
      ? existingStateRes.rows[0].snapshot as Record<string, unknown>
      : {};
    const existingStylePreference = (
      existingSnapshot.style_preference
      && typeof existingSnapshot.style_preference === 'object'
      && !Array.isArray(existingSnapshot.style_preference)
    )
      ? existingSnapshot.style_preference as Record<string, unknown>
      : {};

    const latestProfileRes = await db.query<PsychRow>(
      `SELECT id::text, primary_company, reasoning, generated_bio_professional, generated_bio_personal, primary_role, preferred_contact_style, notable_topics
       FROM user_psychographics
       WHERE user_id = $1
         AND model_name != 'dm-event-reconciler'
       ORDER BY created_at DESC, id DESC
       LIMIT 1`,
      [userId],
    );

    let latest = latestProfileRes.rows[0];
    if (!latest) {
      const fallbackRes = await db.query<PsychRow>(
        `SELECT id::text, primary_company, reasoning, generated_bio_professional, generated_bio_personal, primary_role, preferred_contact_style, notable_topics
         FROM user_psychographics
         WHERE user_id = $1
         ORDER BY created_at DESC, id DESC
         LIMIT 1`,
        [userId],
      );
      latest = fallbackRes.rows[0];
    }
    if (!latest) {
      latest = {
        primary_company: null,
        reasoning: null,
        generated_bio_professional: null,
        generated_bio_personal: null,
        primary_role: null,
        preferred_contact_style: null,
        notable_topics: [],
      };
    }

    const patch = applyProfilePatch(latest, eventsRes.rows);

    const existingStyleValue = typeof existingStylePreference.value === 'string' ? existingStylePreference.value : null;
    const existingStyleUpdatedAt = typeof existingStylePreference.updated_at === 'string' ? existingStylePreference.updated_at : null;
    const existingStyleConfidence = Number.isFinite(Number(existingStylePreference.confidence))
      ? Number(existingStylePreference.confidence)
      : null;
    const existingStyleSource = typeof existingStylePreference.source === 'string' ? existingStylePreference.source : null;
    const existingStyleSourceEventId = typeof existingStylePreference.source_event_id === 'string'
      ? existingStylePreference.source_event_id
      : null;
    const existingReconfirmPromptedAt = typeof existingStylePreference.reconfirm_prompted_at === 'string'
      ? existingStylePreference.reconfirm_prompted_at
      : null;
    const rawExistingPendingCandidate = (
      existingStylePreference.pending_candidate
      && typeof existingStylePreference.pending_candidate === 'object'
      && !Array.isArray(existingStylePreference.pending_candidate)
    )
      ? existingStylePreference.pending_candidate as Record<string, unknown>
      : null;
    let existingPendingCandidate: ContactStylePendingCandidate | null = null;
    if (rawExistingPendingCandidate) {
      const value = typeof rawExistingPendingCandidate.value === 'string'
        ? rawExistingPendingCandidate.value.trim()
        : '';
      if (value) {
        existingPendingCandidate = {
          value,
          confidence: Number.isFinite(Number(rawExistingPendingCandidate.confidence))
            ? Number(rawExistingPendingCandidate.confidence)
            : null,
          source: typeof rawExistingPendingCandidate.source === 'string'
            ? rawExistingPendingCandidate.source
            : 'dm_profile_update_events',
          source_event_id: typeof rawExistingPendingCandidate.source_event_id === 'string'
            ? rawExistingPendingCandidate.source_event_id
            : '',
          source_message_id: Number.isFinite(Number(rawExistingPendingCandidate.source_message_id))
            ? Number(rawExistingPendingCandidate.source_message_id)
            : null,
          proposed_at: typeof rawExistingPendingCandidate.proposed_at === 'string'
            ? rawExistingPendingCandidate.proposed_at
            : new Date().toISOString(),
        };
      }
    }
    const existingStyleHistory = Array.isArray(existingStylePreference.history)
      ? existingStylePreference.history
          .filter((item) => item && typeof item === 'object')
          .map((item: any) => ({
            value: typeof item.value === 'string' ? item.value : '',
            updated_at: typeof item.updated_at === 'string' ? item.updated_at : new Date().toISOString(),
            confidence: Number.isFinite(Number(item.confidence)) ? Number(item.confidence) : null,
            source: typeof item.source === 'string' ? item.source : 'dm_profile_update_events',
            source_event_id: typeof item.source_event_id === 'string' ? item.source_event_id : 'unknown',
          }))
          .filter((item) => item.value.length > 0)
      : [];
    const mergedStyleHistory = [...existingStyleHistory, ...patch.contact_style_audit.history].slice(-12);

    const styleValue = patch.contact_style_audit.value || existingStyleValue;
    const styleUpdatedAt = patch.contact_style_audit.updated_at || existingStyleUpdatedAt;
    const styleConfidence = patch.contact_style_audit.confidence ?? existingStyleConfidence;
    const styleSource = patch.contact_style_audit.source || existingStyleSource;
    const styleSourceEventId = patch.contact_style_audit.source_event_id || existingStyleSourceEventId;
    const styleChanged = Boolean(
      styleValue
      && existingStyleValue
      && styleValue.toLowerCase() !== existingStyleValue.toLowerCase(),
    );
    const styleReconfirmPromptedAt = (
      !styleChanged
    ) ? existingReconfirmPromptedAt : null;
    let pendingStyleCandidate: ContactStylePendingCandidate | null = existingPendingCandidate;
    if (patch.style_events_seen) {
      pendingStyleCandidate = patch.contact_style_audit.pending_candidate;
    }
    if (patch.style_events_seen && patch.contact_style_audit.source_event_id) {
      // Applied style updates clear any previous pending candidate.
      pendingStyleCandidate = null;
    }
    const stylePreference: ContactStyleAudit = {
      value: styleValue,
      updated_at: styleUpdatedAt,
      confidence: styleConfidence,
      source: styleSource,
      source_event_id: styleSourceEventId,
      resolution_rule: STYLE_CONFLICT_RESOLUTION_RULE,
      reconfirm_prompted_at: styleReconfirmPromptedAt,
      pending_candidate: pendingStyleCandidate,
      history: mergedStyleHistory,
    };

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
      profile_overrides: {
        primary_role: patch.primary_role,
        primary_company: patch.primary_company,
        notable_topics: patch.notable_topics,
        // preferred_contact_style is confirmation-gated (see style_preference below).
        updated_at: new Date().toISOString(),
        source_event_id: eventsRes.rows[eventsRes.rows.length - 1].id,
      },
      style_preference: stylePreference,
      applied_at: new Date().toISOString(),
    };

    const reasoningText = patch.reasoning || `Auto-reconciled from ${eventsRes.rows.length} DM profile correction events.`;
    const promptHash = makePromptHash(JSON.stringify(snapshot));
    const onboardingMissingFields = computeMissingOnboardingFields(patch);
    const onboardingStatus = onboardingMissingFields.length === 0 ? 'completed' : 'collecting';
    const onboardingCompletedAt = onboardingStatus === 'completed' ? new Date().toISOString() : null;
    const onboardingLastPromptedField = onboardingMissingFields[0] || null;

    let newProfileId = latest.id || null;
    if (newProfileId) {
      // Important: do not create a new sparse user_psychographics row here.
      // Doing so would wipe richer psychometry fields (tone, deep skills, etc.) in any "latest row" lookups.
      await db.query(
        `UPDATE user_psychographics
         SET primary_company = $2,
             reasoning = $3,
             generated_bio_professional = $4,
             generated_bio_personal = $5,
             primary_role = $6,
             preferred_contact_style = $7,
             notable_topics = $8::jsonb
         WHERE id = $1`,
        [
          newProfileId,
          patch.primary_company,
          reasoningText,
          patch.generated_bio_professional,
          patch.generated_bio_personal,
          patch.primary_role,
          patch.preferred_contact_style,
          JSON.stringify(patch.notable_topics),
        ],
      );
    } else {
      const insertedProfile = await db.query<{ id: string }>(
        `INSERT INTO user_psychographics (
           user_id, model_name, prompt_hash, primary_company, reasoning,
           generated_bio_professional, generated_bio_personal, primary_role, preferred_contact_style, notable_topics, raw_response, latency_ms
         ) VALUES ($1, 'dm-event-reconciler', $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, 0)
         RETURNING id`,
        [
          userId,
          promptHash,
          patch.primary_company,
          reasoningText,
          patch.generated_bio_professional,
          patch.generated_bio_personal,
          patch.primary_role,
          patch.preferred_contact_style,
          JSON.stringify(patch.notable_topics),
          `Applied ${eventsRes.rows.length} DM-derived profile correction(s).`,
        ],
      );
      newProfileId = insertedProfile.rows[0]?.id || null;
    }
    const eventIds = eventsRes.rows.map((r) => r.id);

    try {
      await db.query(
        `INSERT INTO dm_profile_state (
           user_id,
           last_profile_event_id,
           user_psychographics_id,
           snapshot,
           onboarding_status,
           onboarding_required_fields,
           onboarding_missing_fields,
           onboarding_last_prompted_field,
           onboarding_started_at,
           onboarding_completed_at,
           onboarding_turns
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6::jsonb, $7::jsonb, $8, now(), $9, 0)
         ON CONFLICT (user_id)
         DO UPDATE SET
           last_profile_event_id = EXCLUDED.last_profile_event_id,
           user_psychographics_id = EXCLUDED.user_psychographics_id,
           snapshot = EXCLUDED.snapshot,
           onboarding_status = EXCLUDED.onboarding_status,
           onboarding_required_fields = EXCLUDED.onboarding_required_fields,
           onboarding_missing_fields = EXCLUDED.onboarding_missing_fields,
           onboarding_last_prompted_field = EXCLUDED.onboarding_last_prompted_field,
           onboarding_started_at = COALESCE(dm_profile_state.onboarding_started_at, EXCLUDED.onboarding_started_at),
           onboarding_completed_at = CASE
             WHEN EXCLUDED.onboarding_status = 'completed' THEN COALESCE(dm_profile_state.onboarding_completed_at, EXCLUDED.onboarding_completed_at)
             ELSE NULL
           END,
           onboarding_turns = CASE
             WHEN EXCLUDED.onboarding_status = 'completed' THEN 0
             ELSE dm_profile_state.onboarding_turns
           END,
           updated_at = now()`,
        [
          userId,
          eventsRes.rows[eventsRes.rows.length - 1].id,
          newProfileId,
          JSON.stringify(snapshot),
          onboardingStatus,
          JSON.stringify(ONBOARDING_REQUIRED_FIELDS),
          JSON.stringify(onboardingMissingFields),
          onboardingLastPromptedField,
          onboardingCompletedAt,
        ],
      );
    } catch (err: any) {
      const isMissingOnboardingColumns =
        err?.code === '42703'
        || (typeof err?.message === 'string' && err.message.includes('onboarding_'));
      if (!isMissingOnboardingColumns) throw err;

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
    }

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
