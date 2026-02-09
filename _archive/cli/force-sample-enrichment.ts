
import { db } from '../db/index.js';

async function main() {
    console.log("Picking 5 random users with > 10 messages to verify prompt...");

    // Find 5 users with enough stats but maybe already enriched (so we can re-evaluate)
    // We want to see how the NEW prompt handles them.
    const res = await db.query(`
        SELECT u.id, u.display_name, count(m.id) as msg_cnt
        FROM users u
        JOIN messages m ON m.user_id = u.id
        GROUP BY u.id
        HAVING count(m.id) > 10
        ORDER BY random()
        LIMIT 5
    `);

    if (res.rows.length === 0) {
        console.log("No candidates found.");
        return;
    }

    const ids = res.rows.map(r => r.id);
    console.log("Selected IDs:", ids.join(', '));

    // Clear and Flag
    await db.query(`DELETE FROM user_psychographics WHERE user_id = ANY($1)`, [ids]);
    await db.query(`UPDATE users SET needs_enrichment = true WHERE id = ANY($1)`, [ids]);

    console.log("✅ Cleared and flagged. Run 'npm run enrich-psycho -- --limit 5' now.");
    await db.close();
}

main().catch(console.error);
