
/**
 * CLI: Audit Bios
 *
 * Dumps bios of users who were gated (abstained), to manually inspect
 * why our bio extraction is failing.
 *
 * Usage:
 *   npm run audit-bios
 */

import { db } from '../db/index.js';
import { BIO_ROLE_KEYWORDS, BIO_INTENT_KEYWORDS, BIO_AFFILIATION_PATTERNS } from '../inference/keywords.js';

async function main() {
  console.log('🔍 Auditing Bios for Gated Users...');

  // 1. Fetch random sample of gated users who HAVE bios
  // We join abstention_log to ensure they were gated, and check bio IS NOT NULL
  const query = `
    SELECT u.id, u.display_name, u.bio, al.reason_code, al.details
    FROM users u
    JOIN abstention_log al ON al.subject_user_id = u.id
    WHERE u.bio IS NOT NULL AND u.bio <> ''
    AND al.model_version = 'v0.6.0'
    AND al.reason_code = 'low_confidence' -- Focus on near misses first
    ORDER BY random()
    LIMIT 20;
  `;

  const res = await db.query<{ id: number; display_name: string; bio: string; reason_code: string; details: string }>(query);

  console.log(`Found ${res.rows.length} gated users with bios. Analysing...\n`);

  for (const user of res.rows) {
    console.log(`User ${user.id} [${user.display_name}]`);
    console.log(`Bio: "${user.bio}"`);
    console.log(`Gated Reason: ${user.reason_code} (${user.details})`);

    // Check matches manually
    const matches: string[] = [];
    
    // Role check
    for (const kw of BIO_ROLE_KEYWORDS) {
      if (kw.pattern.test(user.bio)) {
        matches.push(`ROLE: ${kw.label} (${kw.tag})`);
      }
    }

    // Intent check
    for (const kw of BIO_INTENT_KEYWORDS) {
      if (kw.pattern.test(user.bio)) {
        matches.push(`INTENT: ${kw.label} (${kw.tag})`);
      }
    }

    // Affiliation check
    for (const aff of BIO_AFFILIATION_PATTERNS) {
      const match = user.bio.match(aff.pattern);
      if (match) {
        matches.push(`AFFILIATION: ${match[1]} (${aff.tag})`);
      }
    }

    if (matches.length > 0) {
      console.log(`⚠️  Matches found but still gated:`);
      matches.forEach(m => console.log(`   - ${m}`));
    } else {
      console.log(`❌  NO MATCHES found in bio.`);
    }

    console.log('-'.repeat(40));
  }

  await db.close();
}

main().catch(console.error);
