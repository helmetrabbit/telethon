
/**
 * CLI: Check Recall Test Results
 */
import { db } from '../db/index.js';

async function main() {
  const targetExtIds = ['user424278354', 'user1314436676', 'user1167327'];
  
  console.log('🔍 Checking Recall Test Users...\n');

  for (const extId of targetExtIds) {
    const user = await db.query('SELECT * FROM users WHERE external_id = $1', [extId]);
    if (user.rows.length === 0) {
        console.log(`❌ User ${extId} NOT FOUND in DB.`);
        continue;
    }
    const u = user.rows[0];
    const uid = u.id;
    console.log(`User ${u.id}: ${u.display_name}`);
    console.log(`Bio: "${u.bio}"`); // Confirm bio is actually there

    // Get Claims
    const claims = await db.query('SELECT * FROM claims WHERE subject_user_id = $1 AND model_version = \'v0.5.8\'', [uid]);
    if (claims.rows.length === 0) {
        console.log(`   🚫 NO CLAIMS emitted.`);
        // Check abstention log
        const abs = await db.query('SELECT * FROM abstention_log WHERE subject_user_id = $1 AND model_version = \'v0.5.8\'', [uid]);
        abs.rows.forEach(r => console.log(`      Abstention: ${r.predicate} -> ${r.reason_code} (${r.details})`));
    } else {
        for (const c of claims.rows) {
            console.log(`   ✅ Claim: ${c.predicate} -> ${c.object_value} (conf=${c.confidence})`);
            
            // Get Evidence
            const ev = await db.query('SELECT * FROM claim_evidence WHERE claim_id = $1', [c.id]);
            ev.rows.forEach(e => {
                console.log(`      - ${e.evidence_type}: ${e.evidence_ref} (w=${e.weight})`);
            });
        }
    }
    console.log('-'.repeat(40));
  }
  await db.close();
}

main().catch(console.error);
