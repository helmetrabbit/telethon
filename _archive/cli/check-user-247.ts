
import { db } from '../db/index.js';

async function main() {
    const userId = 247; // The specific user reported
    console.log(`Checking User ${userId}...`);

    // 1. Check User exists
    const userRes = await db.query('SELECT * FROM users WHERE id = $1', [userId]);
    if (userRes.rows.length === 0) {
        console.log('❌ User not found.');
        process.exit(1);
    }
    const user = userRes.rows[0];
    console.log(`✅ User Found: ${user.display_name} (@${user.handle})`);
    
    // 3. Purge old "Sloppy" Data
    console.log('\n🗑️  Clearing existing profile and queuing for re-enrichment...');
    await db.query('DELETE FROM user_psychographics WHERE user_id = $1', [userId]);
    
    // 4. Flag for immediate update
    await db.query('UPDATE users SET needs_enrichment = true WHERE id = $1', [userId]);
    console.log('✅ Done. You can now run: npm run enrich-psycho -- --limit 1');
    
    await db.close();
}

main().catch(console.error);
