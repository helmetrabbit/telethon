#!/usr/bin/env npx tsx
/**
 * scrape-bios.ts
 *
 * Backfills user bios by scraping public t.me/<username> pages.
 * No Telegram API auth required — just plain HTTP GETs.
 *
 * Usage:  npx tsx src/cli/scrape-bios.ts [--limit N]
 */

import { db } from '../db/index.js';

// ── Config ──────────────────────────────────────────────
const CONCURRENCY    = 10;       // parallel HTTP requests
const DELAY_MS       = 100;      // ms between batches (be polite)
const RETRY_DELAY_MS = 2000;     // backoff on rate-limit / transient error
const MAX_RETRIES    = 3;        // retries per user
const PROGRESS_EVERY = 100;      // log progress every N users
const MIN_PASS_YIELD = 20;       // stop looping when a pass finds fewer than this many bios
const MAX_PASSES     = 15;       // safety cap on number of passes
const USER_AGENT     = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

// ── CLI args ────────────────────────────────────────────
const limitIdx = process.argv.indexOf('--limit');
const USER_LIMIT = limitIdx !== -1 ? parseInt(process.argv[limitIdx + 1], 10) : undefined;

// ── Bio extraction ──────────────────────────────────────
const BIO_REGEX = /<div class="tgme_page_description[^"]*"[^>]*>([\s\S]*?)<\/div>/;

function extractBio(html: string): string | null {
  const match = html.match(BIO_REGEX);
  if (!match) return null;

  // Strip HTML tags and decode entities
  const bio = match[1]
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#036;/g, '$')
    .replace(/&#33;/g, '!')
    .replace(/&nbsp;/g, ' ')
    .trim();

  if (!bio || bio.length === 0) return null;

  // Filter out the Telegram default "contact" placeholder — it's NOT a real bio
  if (/^If you have Telegram, you can contact @\S+ right away\.?$/i.test(bio)) {
    return null;
  }
  // Also filter the generic "You can contact @X right away" variant
  if (/^You can contact @\S+ right away\.?$/i.test(bio)) {
    return null;
  }

  return bio;
}

type FetchResult = { bio: string | null; status: 'ok' | 'error' | 'rate-limited' | 'not-found'; httpCode?: number };

async function fetchBio(username: string, attempt = 1): Promise<FetchResult> {
  try {
    const resp = await fetch(`https://t.me/${username}`, {
      headers: { 'User-Agent': USER_AGENT },
      redirect: 'follow',
      signal: AbortSignal.timeout(10_000), // 10s timeout
    });

    if (resp.status === 429) {
      // Rate-limited — back off and retry
      if (attempt <= MAX_RETRIES) {
        const wait = RETRY_DELAY_MS * attempt;
        await new Promise((r) => setTimeout(r, wait));
        return fetchBio(username, attempt + 1);
      }
      return { bio: null, status: 'rate-limited', httpCode: 429 };
    }

    if (resp.status === 404) {
      return { bio: null, status: 'not-found', httpCode: 404 };
    }

    if (!resp.ok) {
      // Other HTTP error — retry transient 5xx
      if (resp.status >= 500 && attempt <= MAX_RETRIES) {
        const wait = RETRY_DELAY_MS * attempt;
        await new Promise((r) => setTimeout(r, wait));
        return fetchBio(username, attempt + 1);
      }
      return { bio: null, status: 'error', httpCode: resp.status };
    }

    const html = await resp.text();
    return { bio: extractBio(html), status: 'ok', httpCode: 200 };
  } catch (err: unknown) {
    // Network / timeout errors — retry
    if (attempt <= MAX_RETRIES) {
      const wait = RETRY_DELAY_MS * attempt;
      await new Promise((r) => setTimeout(r, wait));
      return fetchBio(username, attempt + 1);
    }
    const msg = err instanceof Error ? err.message : String(err);
    return { bio: null, status: 'error', httpCode: undefined };
  }
}

// ── Main ────────────────────────────────────────────────
interface UserRow { id: string; handle: string }

async function main() {
  let grandTotal = 0;
  let pass = 0;

  while (pass < MAX_PASSES) {
    pass++;

    // Re-query each pass to pick up users still without a bio
    const limitClause = USER_LIMIT ? `LIMIT ${USER_LIMIT}` : '';
    const { rows } = await db.query<UserRow>(`
      SELECT id, handle FROM users
      WHERE handle IS NOT NULL
        AND handle != ''
        AND (bio IS NULL OR bio = '')
      ORDER BY id DESC
      ${limitClause}
    `);

    if (rows.length === 0) {
      console.log(`\n🎉 All scrapable users have bios!`);
      break;
    }

    console.log(`\n${'═'.repeat(60)}`);
    console.log(`🔄 Pass ${pass} — ${rows.length} users remaining without bio`);
    console.log(`${'═'.repeat(60)}`);

    let updated     = 0;
    let empty       = 0;
    let errors      = 0;
    let rateLimited = 0;
    let notFound    = 0;
    let processed   = 0;

    // Process in concurrent batches
    for (let i = 0; i < rows.length; i += CONCURRENCY) {
      const batch = rows.slice(i, i + CONCURRENCY);

      const results = await Promise.all(
        batch.map(async (user: UserRow) => {
          const result = await fetchBio(user.handle);
          return { id: user.id, handle: user.handle, ...result };
        })
      );

      // Update DB
      for (const r of results) {
        processed++;
        if (r.bio) {
          await db.query('UPDATE users SET bio = $1 WHERE id = $2', [r.bio, r.id]);
          updated++;
          console.log(`  ✅ @${r.handle}: ${r.bio.substring(0, 80)}${r.bio.length > 80 ? '…' : ''}`);
        } else if (r.status === 'rate-limited') {
          rateLimited++;
          console.log(`  ⚠️  @${r.handle}: rate-limited (429) after ${MAX_RETRIES} retries`);
        } else if (r.status === 'error') {
          errors++;
          console.log(`  ❌ @${r.handle}: HTTP ${r.httpCode ?? 'network error'}`);
        } else if (r.status === 'not-found') {
          notFound++;
        } else {
          empty++;
        }
      }

      // Adaptive delay — slow down if we're getting rate-limited
      let batchDelay = DELAY_MS;
      if (results.some((r) => r.status === 'rate-limited')) {
        batchDelay = 3000; // 3s cooldown after rate-limit
        console.log(`  🐢 Rate-limit detected, cooling down ${batchDelay}ms...`);
      }

      // Progress
      if (processed % PROGRESS_EVERY === 0 || processed === rows.length) {
        const pct = ((processed / rows.length) * 100).toFixed(1);
        const parts = [`${updated} bios`, `${empty} empty`];
        if (errors > 0) parts.push(`${errors} errors`);
        if (rateLimited > 0) parts.push(`${rateLimited} rate-limited`);
        if (notFound > 0) parts.push(`${notFound} not-found`);
        console.log(`📊 ${processed}/${rows.length} (${pct}%) — ${parts.join(', ')}`);
      }

      // Be polite
      if (i + CONCURRENCY < rows.length) {
        await new Promise((r) => setTimeout(r, batchDelay));
      }
    }

    grandTotal += updated;
    console.log(`\n✅ Pass ${pass} complete: ${updated} bios found (${grandTotal} total across all passes)`);
    console.log(`   ${empty} empty, ${errors} errors, ${rateLimited} rate-limited, ${notFound} not-found`);

    if (updated < MIN_PASS_YIELD) {
      console.log(`\n🏁 Yield dropped below ${MIN_PASS_YIELD} — stopping.`);
      break;
    }

    // Brief pause between passes
    console.log(`   ⏳ Pausing 2s before next pass...`);
    await new Promise((r) => setTimeout(r, 2000));
  }

  const { rows: [{ count }] } = await db.query<{ count: string }>(`SELECT COUNT(*)::text AS count FROM users WHERE bio IS NOT NULL AND bio != ''`);
  console.log(`\n🎯 Grand total: ${grandTotal} new bios this session. ${count} users now have bios in DB.`);
  await db.close();
}

main().catch((err) => {
  console.error('❌ Fatal:', err);
  process.exit(1);
});
