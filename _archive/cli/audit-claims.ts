
/**
 * CLI: Audit Claims
 *
 * Inspects users who have specific claims to verify precision.
 *
 * Usage:
 *   npx tsx src/cli/audit-claims.ts --predicate has_role --object builder
 */

import { db } from '../db/index.js';
import minimist from 'minimist';

async function main() {
  const args = minimist(process.argv.slice(2));
  const predicate = args.predicate || 'has_role';
  const objectValue = args.object;
  const limit = args.limit || 20;
  const model = args.model || 'v0.5.9';

  if (!objectValue) {
    console.error('Usage: npx tsx src/cli/audit-claims.ts --predicate <p> --object <o> [--limit <n>] [--model <v>]');
    process.exit(1);
  }

  console.log(`🔍 Auditing Claim: ${predicate} -> ${objectValue}`);
  console.log(`   Model: ${model}`);

  const query = `
    SELECT 
      u.id, 
      u.display_name, 
      u.bio, 
      u.handle,
      c.confidence,
      c.id as claim_id
    FROM claims c
    JOIN users u ON u.id = c.subject_user_id
    WHERE c.predicate = $1 
      AND c.object_value = $2
      AND c.model_version = $4
    ORDER BY c.confidence DESC, random()
    LIMIT $3
  `;

  const res = await db.query(query, [predicate, objectValue, limit, model]);

  console.log(`Found ${res.rows.length} users. Showing top ${limit} by confidence...\n`);

  for (const row of res.rows) {
    console.log(`User ${row.id} [${row.display_name}] (@${row.handle})`);
    if (row.bio) console.log(`Bio: "${row.bio}"`);
    console.log(`Confidence: ${row.confidence}`);

    // Fetch evidence
    const evQuery = `
      SELECT evidence_type, evidence_ref, weight 
      FROM claim_evidence 
      WHERE claim_id = $1
    `;
    const evRes = await db.query(evQuery, [row.claim_id]);
    
    for (const e of evRes.rows) {
      console.log(`   - ${e.evidence_type}: ${e.evidence_ref} (w=${e.weight})`);
    }
    console.log('-'.repeat(40));
  }

  await db.close();
}

main().catch(console.error);
