#!/usr/bin/env node
/**
 * v3 minimal claims extractor from psychographics timeline + bio.
 *
 * Usage:
 *   npm run extract-claims
 *   npm run extract-claims -- --limit 100
 *   npm run extract-claims -- --user-id 123
 */

import { db } from '../db/index.js';
import { parseArgs } from '../utils.js';

const DEFAULT_MODEL_VERSION = 'claims:v3';

const ROLE_MAP: Record<string, string> = {
  // strict mappings to role_label enum
  'bd': 'bd',
  'business development': 'bd',
  'biz dev': 'bd',
  'business developer': 'bd',
  'founder': 'founder_exec',
  'cofounder': 'founder_exec',
  'co-founder': 'founder_exec',
  'ceo': 'founder_exec',
  'cto': 'founder_exec',
  'cfo': 'founder_exec',
  'cmo': 'founder_exec',
  'executive': 'founder_exec',
  'investor': 'investor_analyst',
  'analyst': 'investor_analyst',
  'recruiter': 'recruiter',
  'agency': 'vendor_agency',
  'vendor': 'vendor_agency',
  'community': 'community',
  'media': 'media_kol',
  'kol': 'media_kol',
  'builder': 'builder',
  'developer': 'builder',
  'engineer': 'builder',
  'trader': 'market_maker',
  'market maker': 'market_maker',
};

function mapRole(role: string | null | undefined): string | null {
  if (!role) return null;
  const key = role.trim().toLowerCase();
  return ROLE_MAP[key] || null;
}

type TimelineEntry = {
  org?: string | null;
  role?: string | null;
  is_current?: boolean;
  evidence_message_ids?: number[];
  confidence?: number;
};

async function main(): Promise<void> {
  const args = parseArgs();
  const limit = args['limit'] ? parseInt(args['limit'], 10) : 0;
  const userId = args['user-id'] ? parseInt(args['user-id'], 10) : null;
  const modelVersion = args['model-version'] || DEFAULT_MODEL_VERSION;

  const psychoQuery = userId
    ? `SELECT p.user_id, p.role_company_timeline, u.bio, u.bio_updated_at
       FROM user_psychographics p JOIN users u ON u.id = p.user_id
       WHERE p.user_id = ${userId}
       ORDER BY p.created_at DESC LIMIT 1`
    : `SELECT DISTINCT ON (p.user_id) p.user_id, p.role_company_timeline, u.bio, u.bio_updated_at
       FROM user_psychographics p JOIN users u ON u.id = p.user_id
       ORDER BY p.user_id, p.created_at DESC ${limit > 0 ? `LIMIT ${limit}` : ''}`;

  const { rows } = await db.query<{ user_id: number; role_company_timeline: any; bio: string | null; bio_updated_at: string | null }>(psychoQuery);
  console.log(`\n🧾 Extracting claims from ${rows.length} users (model=${modelVersion})...`);

  for (const row of rows) {
    const timeline: TimelineEntry[] = Array.isArray(row.role_company_timeline) ? row.role_company_timeline : [];
    if (timeline.length === 0) continue;

    await db.transaction(async (client) => {
      for (const t of timeline) {
        const org = typeof t.org === 'string' ? t.org.trim() : '';
        const roleMapped = mapRole(t.role);
        const confidence = typeof t.confidence === 'number' ? t.confidence : 0;
        const msgIds = Array.isArray(t.evidence_message_ids) ? t.evidence_message_ids.filter((x) => Number.isFinite(x)) : [];

        if (org) {
          const evidence = await buildEvidence(client, row.user_id, msgIds, row.bio_updated_at);
          if (evidence.length === 0) continue; // skip if no evidence
          const claimId = await upsertClaim(client, row.user_id, 'affiliated_with', org, confidence, modelVersion);
          await upsertEvidence(client, claimId, evidence);
        }

        if (roleMapped) {
          const evidence = await buildEvidence(client, row.user_id, msgIds, row.bio_updated_at);
          if (evidence.length === 0) continue;
          const claimId = await upsertClaim(client, row.user_id, 'has_role', roleMapped, confidence, modelVersion);
          await upsertEvidence(client, claimId, evidence);
        }
      }
    });
  }

  await db.close();
}

async function upsertClaim(
  client: any,
  userId: number,
  predicate: string,
  objectValue: string,
  confidence: number,
  modelVersion: string,
): Promise<number> {
  const { rows } = await client.query(
    `INSERT INTO claims (subject_user_id, predicate, object_value, confidence, status, model_version)
     VALUES ($1, $2, $3, $4, 'tentative', $5)
     ON CONFLICT (subject_user_id, predicate, object_value, model_version)
     DO UPDATE SET confidence = EXCLUDED.confidence
     RETURNING id`,
    [userId, predicate, objectValue, confidence, modelVersion],
  );
  return rows[0].id;
}

async function buildEvidence(
  client: any,
  userId: number,
  msgIds: number[],
  bioUpdatedAt: string | null,
): Promise<{ evidence_type: string; evidence_ref: string }[]> {
  const out: { evidence_type: string; evidence_ref: string }[] = [];

  if (msgIds.length > 0) {
    const res = await client.query(
      'SELECT message_id FROM message_insights WHERE message_id = ANY($1)',
      [msgIds],
    );
    const rows = res.rows as { message_id: number }[];
    for (const r of rows) {
      out.push({ evidence_type: 'message', evidence_ref: `msg:${r.message_id}` });
    }
  }

  if (out.length === 0 && bioUpdatedAt) {
    out.push({ evidence_type: 'bio', evidence_ref: `bio:${userId}:${bioUpdatedAt}` });
  }

  return out;
}

async function upsertEvidence(
  client: any,
  claimId: number,
  evidence: { evidence_type: string; evidence_ref: string }[],
): Promise<void> {
  for (const e of evidence) {
    await client.query(
      `INSERT INTO claim_evidence (claim_id, evidence_type, evidence_ref, weight)
       VALUES ($1, $2, $3, 1.0)
       ON CONFLICT DO NOTHING`,
      [claimId, e.evidence_type, e.evidence_ref],
    );
  }
}

main().catch((err) => {
  console.error('❌ extract-claims failed:', err);
  process.exit(1);
});
