/**
 * CLI: Aggregates claims from multiple models (deterministic + varied LLMs)
 * and produces a high-confidence consensus set.
 *
 * Usage:
 *   npx tsx src/cli/resolve-ensemble.ts
 */

import { db } from '../db/index.js';
import { ROLES, INTENTS } from '../config/taxonomies.js';
import type { Role, Intent, Predicate } from '../config/taxonomies.js';
import { writeClaimWithEvidence } from '../inference/engine.js';

// ── Voting Config ───────────────────────────────────────

// Which model versions to include in the vote
// 'deterministic:v0.6.0' = the regex engine
// 'llm:...' = the AI models
const VOTING_BLOC = [
  'llm:liquid/lfm-2.5-1.2b-thinking:free',
  'llm:nvidia/nemotron-3-nano-30b-a3b:free',
  'llm:stepfun/step-3.5-flash:free',
];

// Min votes required to win 'supported' status in consensus
const MIN_VOTES = 2; // 2 out of 3 models must agree

// ── Types ───────────────────────────────────────────────

interface ClaimVote {
  model: string;
  confidence: number;
}

interface UserVotes {
  userId: number;
  roles: Map<Role, ClaimVote[]>;
  intents: Map<Intent, ClaimVote[]>;
  affiliations: Map<string, ClaimVote[]>;
}

// ── Main ────────────────────────────────────────────────

async function main() {
  const version = 'ensemble:v1';
  console.log(`\n⚖️  Running Ensemble Resolution (${version})\n`);
  console.log(`   Voting bloc: \n   - ${VOTING_BLOC.join('\n   - ')}\n`);

  // Clear previous ensemble results
  await db.query('DELETE FROM claims WHERE model_version = $1', [version]);
  
  // 1. Fetch all claims from the voting bloc
  // We fetch standard columns + model_version
  const { rows: claims } = await db.query<{
    subject_user_id: number;
    predicate: string;
    object_value: string;
    confidence: number;
    model_version: string;
  }>(`
    SELECT subject_user_id, predicate, object_value, confidence, model_version
    FROM claims
    WHERE model_version = ANY($1)
  `, [VOTING_BLOC]);

  console.log(`   Loaded ${claims.length} claims from participating models.`);

  // 2. Aggregate votes per user
  const userMap = new Map<number, UserVotes>();

  for (const c of claims) {
    if (!userMap.has(c.subject_user_id)) {
      userMap.set(c.subject_user_id, {
        userId: c.subject_user_id,
        roles: new Map(),
        intents: new Map(),
        affiliations: new Map(),
      });
    }
    const u = userMap.get(c.subject_user_id)!;
    const vote = { model: c.model_version, confidence: c.confidence };

    if (c.predicate === 'has_role') {
      const r = c.object_value as Role;
      if (!u.roles.has(r)) u.roles.set(r, []);
      u.roles.get(r)!.push(vote);
    } else if (c.predicate === 'has_intent') {
      const i = c.object_value as Intent;
      if (!u.intents.has(i)) u.intents.set(i, []);
      u.intents.get(i)!.push(vote);
    } else if (c.predicate === 'affiliated_with' || c.predicate === 'has_org_type') {
      const key = `${c.predicate}:${c.object_value}`; // composite key for map
      if (!u.affiliations.has(key)) u.affiliations.set(key, []);
      u.affiliations.get(key)!.push(vote);
    }
  }

  // 3. Resolve & Persist
  let resolvedCount = 0;
  let skippedCount = 0;

  for (const [userId, votes] of userMap) {
    // -- Resolve Roles --
    for (const [role, castVotes] of votes.roles) {
      if (castVotes.length >= MIN_VOTES) {
        const avgConf = castVotes.reduce((sum, v) => sum + v.confidence, 0) / castVotes.length;
        await persistConsensus(userId, 'has_role', role, avgConf, version, castVotes);
        resolvedCount++;
      }
    }

    // -- Resolve Intents --
    for (const [intent, castVotes] of votes.intents) {
      if (castVotes.length >= MIN_VOTES) {
        const avgConf = castVotes.reduce((sum, v) => sum + v.confidence, 0) / castVotes.length;
        await persistConsensus(userId, 'has_intent', intent, avgConf, version, castVotes);
        resolvedCount++;
      }
    }
    
    // -- Resolve Affiliations (Org & OrgType) --
    // These are trickier because string values might differ slightly ("Binance" vs "Binance Labs")
    // For now, we require exact match resolution.
    for (const [key, castVotes] of votes.affiliations) {
      if (castVotes.length >= MIN_VOTES) {
        const colonIdx = key.indexOf(':');
        const predicate = key.slice(0, colonIdx);
        const value = key.slice(colonIdx + 1);
        const avgConf = castVotes.reduce((sum, v) => sum + v.confidence, 0) / castVotes.length;
        await persistConsensus(userId, predicate as Predicate, value, avgConf, version, castVotes);
        resolvedCount++;
      }
    }
    
    if (votes.roles.size + votes.intents.size === 0) skippedCount++;
  }

  console.log(`\n✅ Resolution Complete.`);
  console.log(`   Total Users Scanned: ${userMap.size}`);
  console.log(`   Consensus Claims:    ${resolvedCount}`);
  await db.close();
}

// Write the "Winner" claim to DB
async function persistConsensus(
  userId: number, 
  predicate: Predicate, 
  value: string, 
  confidence: number, 
  version: string,
  votes: ClaimVote[]
) {
  // Construct evidence showing which models voted for this
  const evidence = votes.map(v => ({
    evidence_type: 'llm' as const, // We use 'llm' as the type for the consensus pointer
    evidence_ref: `consensus:source=${v.model}`,
    weight: v.confidence
  }));

  await db.transaction(async client => {
    await writeClaimWithEvidence(
      client,
      userId,
      predicate,
      value,
      parseFloat(confidence.toFixed(2)),
      'supported', // Consensus implies support
      evidence,
      version
    );
  });
  
  console.log(`   User ${userId} → ${predicate}: ${value} (Votes: ${votes.length}/${VOTING_BLOC.length})`);
}

main().catch(console.error);
