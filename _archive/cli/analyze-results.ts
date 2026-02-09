
import { db } from '../db/index.js';

async function main() {
  console.log("=== Phase 5: Result Analysis (v0.6.0) ===\n");

  // 1. Claim Stats
  console.log("--- Claims Summary ---");
  const claims = await db.query(`
    SELECT predicate, status, COUNT(*) as count 
    FROM claims 
    WHERE model_version = 'v0.6.0' 
    GROUP BY predicate, status
    ORDER BY predicate, status
  `);
  
  if (claims.rows.length === 0) {
      console.log("(No claims found for v0.6.0. Did you run inference?)");
  } else {
      claims.rows.forEach(r => {
        console.log(`- ${r.predicate} [${r.status}]: ${r.count}`);
      });
  }

  // 2. Abstention Stats
  console.log("\n--- Abstention Reasons ---");
  const abstentions = await db.query(`
    SELECT reason_code, count(*) as c 
    FROM abstention_log 
    WHERE model_version = 'v0.6.0' 
    GROUP BY reason_code 
    ORDER BY c DESC
  `);
  
  if (abstentions.rows.length === 0) {
      console.log("(No abstentions found)");
  } else {
      abstentions.rows.forEach(r => {
        console.log(`- ${r.reason_code}: ${r.c}`);
      });
  }

  // 3. Near Misses Analysis
  console.log("\n--- 'Near Misses' (Confidence 0.20 - 0.55) ---");
  console.log("These users have signal but were gated by confidence threshold.\n");
  
interface AbstentionRow {
    subject_user_id: number;
    details: string;
}

  const nearMisses = await db.query<AbstentionRow>(`
    SELECT subject_user_id, details 
    FROM abstention_log 
    WHERE model_version = 'v0.6.0'
    AND reason_code = 'low_confidence'
  `);

  const usersToCheck: {id: number, score: number, msg: string}[] = [];

  for (const row of nearMisses.rows) {
      const match = row.details.match(/confidence ([0-9.]+) < threshold/);
      if (match) {
          const conf = parseFloat(match[1]);
          if (conf >= 0.20) {
              usersToCheck.push({
                  id: row.subject_user_id,
                  score: conf,
                  msg: row.details
              });
          }
      }
  }

  // Sort by score descending
  usersToCheck.sort((a,b) => b.score - a.score);

  // Print top 20
  usersToCheck.slice(0, 20).forEach(u => {
      console.log(`User ${u.id}: ${u.msg}`);
  });

  console.log(`\nTotal 'Near Misses' (>0.20): ${usersToCheck.length}`);

  process.exit(0);
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});
