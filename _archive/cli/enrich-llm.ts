/**
 * CLI: Run LLM enrichment for user profiles.
 *
 * Usage:
 *   npx tsx src/cli/enrich-llm.ts
 *   npx tsx src/cli/enrich-llm.ts --model liquid/lfm-2.5-1.2b-thinking:free
 *   npx tsx src/cli/enrich-llm.ts --limit 10 --skip-existing
 *
 * Feeds structured user briefings to an OpenRouter LLM and writes
 * the results as claims with evidence_type='llm'.
 */

import { db } from '../db/index.js';
import { parseArgs } from '../utils.js';
import { createLLMClient, promptHash } from '../inference/llm-client.js';
import { buildPrompt, type UserBriefing } from '../inference/llm-prompt.js';
import { ROLES, INTENTS, ORG_TYPES } from '../config/taxonomies.js';
import type { Role, Intent, OrgType } from '../config/taxonomies.js';

// ── Types ───────────────────────────────────────────────

interface LLMClassification {
  synthetic_bio?: string;
  roles?: string[];
  role_reasoning?: string;
  intents?: string[];
  intent_reasoning?: string;
  org_affiliation?: string | null;
  org_type?: string | null;
  confidence?: number;
}

// ── Config ──────────────────────────────────────────────

const DEFAULT_MODEL = 'liquid/lfm-2.5-1.2b-thinking:free';
const API_KEYS = [
  process.env.OPENROUTER_API_KEY || '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',
  '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',
  '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',
].filter(Boolean) as string[];

// ── Main ────────────────────────────────────────────────

async function main(): Promise<void> {
  const args = parseArgs();
  const model = args['model'] || DEFAULT_MODEL;
  const limit = parseInt(args['limit'] || '0', 10);
  const skipExisting = args['skip-existing'] === 'true';
  const modelVersion = `llm:${model}`;

  console.log(`\n🤖 LLM Enrichment — ${model}\n`);

  const llm = createLLMClient({
    apiKeys: API_KEYS,
    model,
    maxRetries: 3,
    retryDelayMs: 2000,
    requestDelayMs: 600, // ~100 req/min
  });

  // ── 1. Find users to process ──────────────────────
  let userQuery = `
    SELECT u.id, u.handle, u.display_name, u.bio
    FROM users u
    JOIN memberships m ON m.user_id = u.id
    GROUP BY u.id
    ORDER BY u.id
  `;
  if (limit > 0) userQuery += ` LIMIT ${limit}`;

  const { rows: users } = await db.query<{
    id: number; handle: string | null; display_name: string | null; bio: string | null;
  }>(userQuery);

  console.log(`   Found ${users.length} users.`);

  // Filter out already-enriched users if requested
  let toProcess = users;
  if (skipExisting) {
    const { rows: existing } = await db.query<{ user_id: number }>(
      'SELECT DISTINCT user_id FROM llm_enrichments WHERE model_name = $1',
      [model],
    );
    const existingSet = new Set(existing.map((r) => r.user_id));
    toProcess = users.filter((u) => !existingSet.has(u.id));
    console.log(`   Skipping ${users.length - toProcess.length} already enriched. Processing ${toProcess.length}.`);
  }

  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < toProcess.length; i++) {
    const user = toProcess[i];
    const pct = Math.round(((i + 1) / toProcess.length) * 100);
    console.log(`\n── [${i + 1}/${toProcess.length} ${pct}%] User ${user.id} (${user.handle || user.display_name || 'anon'}) ──`);

    try {
      // ── 2. Build briefing from existing DB data ─────
      const briefing = await loadUserBriefing(user.id, user.handle, user.display_name, user.bio);

      if (briefing.totalMessages === 0 && !briefing.bio) {
        console.log('   ⏭ Skipping — no messages and no bio.');
        continue;
      }

      // ── 3. Build prompt and call LLM ────────────────
      const prompt = buildPrompt(briefing);
      const hash = promptHash(prompt);

      // Check cache
      const { rows: cached } = await db.query(
        'SELECT parsed_json FROM llm_enrichments WHERE user_id = $1 AND model_name = $2 AND prompt_hash = $3',
        [user.id, model, hash],
      );
      let parsed: LLMClassification;
      let latencyMs = 0;

      if (cached.length > 0 && cached[0].parsed_json) {
        console.log('   📦 Using cached response.');
        parsed = cached[0].parsed_json as LLMClassification;
      } else {
        const response = await llm.complete(prompt);
        latencyMs = response.latencyMs;
        console.log(`   ⏱ LLM responded in ${latencyMs}ms`);

        // Parse response
        parsed = extractJSON(response.content);

        // Store raw response
        await db.query(
          `INSERT INTO llm_enrichments (user_id, model_name, prompt_hash, raw_response, parsed_json, latency_ms)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (user_id, model_name, prompt_hash) DO UPDATE
           SET raw_response = EXCLUDED.raw_response, parsed_json = EXCLUDED.parsed_json,
               latency_ms = EXCLUDED.latency_ms, created_at = now()`,
          [user.id, model, hash, response.content, parsed, latencyMs],
        );
      }

      // ── 4. Write claims from LLM output ─────────────
      await writeLLMClaims(user.id, modelVersion, parsed);
      successCount++;

    } catch (err) {
      failCount++;
      console.error(`   ❌ Failed: ${(err as Error).message}`);
    }
  }

  // ── Summary ─────────────────────────────────────────
  console.log('\n' + '━'.repeat(50));
  console.log(`✅ LLM enrichment complete (${model}):`);
  console.log(`   Succeeded: ${successCount}`);
  console.log(`   Failed:    ${failCount}`);
  console.log(`   Skipped:   ${toProcess.length - successCount - failCount}`);

  await db.close();
}

// ── Helpers ─────────────────────────────────────────────

async function loadUserBriefing(
  userId: number,
  handle: string | null,
  displayName: string | null,
  bio: string | null,
): Promise<UserBriefing> {
  // Groups
  const { rows: groups } = await db.query<{ title: string; kind: string; msg_count: number }>(`
    SELECT g.title, g.kind, m.msg_count
    FROM memberships m JOIN groups g ON g.id = m.group_id
    WHERE m.user_id = $1
  `, [userId]);

  // Message count
  const { rows: countRow } = await db.query<{ cnt: string }>(
    'SELECT COUNT(*)::text AS cnt FROM messages WHERE user_id = $1',
    [userId],
  );
  const totalMessages = parseInt(countRow[0]?.cnt ?? '0', 10);

  // Sample messages: 25 most recent + 25 longest (deduped)
  // Recent messages show current behavior; long messages show substantive content.
  const { rows: recentMsgs } = await db.query<{ id: number; sent_at: string; text: string }>(`
    SELECT id, sent_at, text FROM messages
    WHERE user_id = $1 AND text IS NOT NULL AND LENGTH(text) > 10
    ORDER BY sent_at DESC
    LIMIT 25
  `, [userId]);

  const { rows: longMsgs } = await db.query<{ id: number; sent_at: string; text: string }>(`
    SELECT id, sent_at, text FROM messages
    WHERE user_id = $1 AND text IS NOT NULL AND LENGTH(text) > 10
    ORDER BY LENGTH(text) DESC
    LIMIT 25
  `, [userId]);

  // Deduplicate by message id, then sort chronologically
  const seenIds = new Set<number>();
  const sampleMsgs: { sent_at: string; text: string }[] = [];
  for (const m of [...recentMsgs, ...longMsgs]) {
    if (!seenIds.has(m.id)) {
      seenIds.add(m.id);
      sampleMsgs.push({ sent_at: m.sent_at, text: m.text });
    }
  }
  sampleMsgs.sort((a, b) => new Date(a.sent_at).getTime() - new Date(b.sent_at).getTime());

  // Existing deterministic claims
  const { rows: claims } = await db.query<{
    predicate: string; object_value: string; confidence: number; status: string;
  }>(`
    SELECT predicate, object_value, confidence, status
    FROM claims WHERE subject_user_id = $1
    AND model_version NOT LIKE 'llm:%'
  `, [userId]);

  return {
    userId,
    handle,
    displayName,
    bio,
    groups: groups.map((g) => ({ title: g.title || '(untitled)', kind: g.kind, msgCount: g.msg_count })),
    totalMessages,
    sampleMessages: sampleMsgs.map((m) => ({ sent_at: new Date(m.sent_at).toISOString().slice(0, 10), text: m.text })),
    existingClaims: claims.map((c) => ({ predicate: c.predicate, value: c.object_value, confidence: c.confidence, status: c.status })),
  };
}

function extractJSON(raw: string): LLMClassification {
  // 1. Remove <think> blocks (DeepSeek/Liquid style) to avoid confusion
  const clean = raw.replace(/<think>[\s\S]*?<\/think>/gi, '').trim();

  // 2. Try to extract JSON from markdown fences
  const fenceMatch = clean.match(/```(?:json)?\s*([\s\S]*?)\s*```/);
  const jsonStr = fenceMatch ? fenceMatch[1] : clean;

  // 3. Find the outermost { ... }
  const braceStart = jsonStr.indexOf('{');
  const braceEnd = jsonStr.lastIndexOf('}');
  
  if (braceStart === -1 || braceEnd === -1) {
    if (raw.length > 0) {
        console.warn('   ⚠ No JSON object found in LLM response. Raw length:', raw.length);
    }
    return {};
  }

  try {
    const candidate = jsonStr.slice(braceStart, braceEnd + 1);
    return JSON.parse(candidate);
  } catch (e) {
    console.warn(`   ⚠ Failed to parse LLM JSON: ${(e as Error).message}`);
    // Optional: try loose JSON parsing here if needed
    return {};
  }
}

const validRoles = new Set<string>(ROLES.filter((r) => r !== 'unknown'));
const validIntents = new Set<string>(INTENTS.filter((i) => i !== 'unknown'));
const validOrgTypes = new Set<string>(ORG_TYPES.filter((o) => o !== 'unknown'));

async function writeLLMClaims(
  userId: number,
  modelVersion: string,
  parsed: LLMClassification,
): Promise<void> {
  const confidence = Math.min(1.0, Math.max(0, parsed.confidence ?? 0.5));

  // Clear previous LLM claims for this user + model version
  await db.query('DELETE FROM claims WHERE subject_user_id = $1 AND model_version = $2', [userId, modelVersion]);

  // Role claims
  const roles = (parsed.roles || []).filter((r) => validRoles.has(r));
  for (const role of roles) {
    await db.transaction(async (client) => {
      const claimRes = await client.query(
        `INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version, notes, generated_at)
         VALUES ($1, 'has_role'::predicate_label, $2, $3, $4::claim_status, $5, $6, now())
         ON CONFLICT (subject_user_id, predicate, object_value, model_version)
         DO UPDATE SET confidence = EXCLUDED.confidence, status = EXCLUDED.status, notes = EXCLUDED.notes, generated_at = now()
         RETURNING id`,
        [userId, role as any, confidence, confidence >= 0.6 ? 'supported' : 'tentative', modelVersion, parsed.role_reasoning || null],
      );
      const claimId = claimRes.rows[0].id;
      await client.query('DELETE FROM claim_evidence WHERE claim_id = $1', [claimId]);
      await client.query(
        `INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
         VALUES ($1, 'llm'::evidence_type, $2, $3)`,
        [claimId, `llm:${modelVersion}:role:${role}`, confidence * 3.0],
      );
    });
    console.log(`   ✅ role: ${role} (confidence=${confidence.toFixed(2)})`);
  }

  // Intent claims
  const intents = (parsed.intents || []).filter((i) => validIntents.has(i));
  for (const intent of intents) {
    await db.transaction(async (client) => {
      const claimRes = await client.query(
        `INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version, notes, generated_at)
         VALUES ($1, 'has_intent'::predicate_label, $2, $3, $4::claim_status, $5, $6, now())
         ON CONFLICT (subject_user_id, predicate, object_value, model_version)
         DO UPDATE SET confidence = EXCLUDED.confidence, status = EXCLUDED.status, notes = EXCLUDED.notes, generated_at = now()
         RETURNING id`,
        [userId, intent as any, confidence, confidence >= 0.6 ? 'supported' : 'tentative', modelVersion, parsed.intent_reasoning || null],
      );
      const claimId = claimRes.rows[0].id;
      await client.query('DELETE FROM claim_evidence WHERE claim_id = $1', [claimId]);
      await client.query(
        `INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
         VALUES ($1, 'llm'::evidence_type, $2, $3)`,
        [claimId, `llm:${modelVersion}:intent:${intent}`, confidence * 3.0],
      );
    });
    console.log(`   ✅ intent: ${intent} (confidence=${confidence.toFixed(2)})`);
  }

  // Affiliation
  if (parsed.org_affiliation) {
    await db.transaction(async (client) => {
      const claimRes = await client.query(
        `INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version, notes, generated_at)
         VALUES ($1, 'affiliated_with'::predicate_label, $2, $3, $4::claim_status, $5, $6, now())
         ON CONFLICT (subject_user_id, predicate, object_value, model_version)
         DO UPDATE SET confidence = EXCLUDED.confidence, status = EXCLUDED.status, generated_at = now()
         RETURNING id`,
        [userId, parsed.org_affiliation, confidence, confidence >= 0.6 ? 'supported' : 'tentative', modelVersion, parsed.synthetic_bio || null],
      );
      const claimId = claimRes.rows[0].id;
      await client.query('DELETE FROM claim_evidence WHERE claim_id = $1', [claimId]);
      await client.query(
        `INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
         VALUES ($1, 'llm'::evidence_type, $2, $3)`,
        [claimId, `llm:${modelVersion}:affiliation`, confidence * 3.0],
      );
    });
    console.log(`   ✅ affiliation: ${parsed.org_affiliation}`);
  }

  // Org type
  if (parsed.org_type && validOrgTypes.has(parsed.org_type)) {
    await db.transaction(async (client) => {
      const claimRes = await client.query(
        `INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version, notes, generated_at)
         VALUES ($1, 'has_org_type'::predicate_label, $2, $3, $4::claim_status, $5, $6, now())
         ON CONFLICT (subject_user_id, predicate, object_value, model_version)
         DO UPDATE SET confidence = EXCLUDED.confidence, status = EXCLUDED.status, generated_at = now()
         RETURNING id`,
        [userId, parsed.org_type as any, confidence, confidence >= 0.6 ? 'supported' : 'tentative', modelVersion, null],
      );
      const claimId = claimRes.rows[0].id;
      await client.query('DELETE FROM claim_evidence WHERE claim_id = $1', [claimId]);
      await client.query(
        `INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
         VALUES ($1, 'llm'::evidence_type, $2, $3)`,
        [claimId, `llm:${modelVersion}:org_type:${parsed.org_type}`, confidence * 3.0],
      );
    });
    console.log(`   ✅ org_type: ${parsed.org_type}`);
  }
}

main().catch((err) => {
  console.error('❌ enrich-llm failed:', err);
  process.exit(1);
});
