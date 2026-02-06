/**
 * Quick smoke test: verify DB connection + list tables.
 * Run: npx tsc && node dist/db/smoke-test.js
 */

import { db } from './index.js';

async function main() {
  console.log('Connecting to Postgres...');

  const result = await db.query<{ table_name: string }>(
    `SELECT table_name
       FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
      ORDER BY table_name`,
  );

  console.log(`\n✅ Connected! Found ${result.rows.length} tables:`);
  for (const row of result.rows) {
    console.log(`   - ${row.table_name}`);
  }

  // Test transaction helper
  const ver = await db.transaction(async (client) => {
    const r = await client.query('SELECT version()');
    return r.rows[0].version as string;
  });
  console.log(`\n✅ Transaction test passed. PG version:\n   ${ver}`);

  await db.close();
  console.log('\n✅ Pool closed cleanly.');
}

main().catch((err) => {
  console.error('❌ Smoke test failed:', err.message);
  process.exit(1);
});
