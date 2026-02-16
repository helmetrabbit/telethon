#!/usr/bin/env npx tsx
/**
 * scrape-bios.ts
 *
 * Backfills user bios and display names by scraping public t.me/<username> pages.
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

// ── Profile extraction ──────────────────────────────────
const BIO_REGEX = /<div class="tgme_page_description[^"]*"[^>]*>([\s\S]*?)<\/div>/;
const DISPLAY_NAME_REGEX = /<div class="tgme_page_title[^"]*"[^>]*>([\s\S]*?)<\/div>/i;
const OG_TITLE_REGEX = /<meta[^>]+property=["']og:title["'][^>]+content=["']([\s\S]*?)["'][^>]*>/i;
const OG_TITLE_FALLBACK_REGEX = /<meta[^>]+content=["']([\s\S]*?)["'][^>]+property=["']og:title["'][^>]*>/i;

function decodeEntities(text: string): string {
  return text
    .replace(/&#x([0-9a-f]+);/gi, (_, hex: string) => String.fromCodePoint(parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, dec: string) => String.fromCodePoint(parseInt(dec, 10)))
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#036;/g, '$')
    .replace(/&#33;/g, '!')
    .replace(/&nbsp;/g, ' ');
}

function htmlToText(raw: string): string {
  return decodeEntities(
    raw
      .replace(/<br\s*\/?>/gi, '\n')
      .replace(/<\/p>/gi, '\n')
      .replace(/<[^>]+>/g, '')
  )
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n[ \t]+/g, '\n')
    .replace(/[ \t]{2,}/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function extractBio(html: string): string | null {
  const match = html.match(BIO_REGEX);
  if (!match) return null;

  const bio = htmlToText(match[1]);

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

function extractDisplayName(html: string): string | null {
  const titleMatch = html.match(DISPLAY_NAME_REGEX);
  const ogMatch = html.match(OG_TITLE_REGEX) || html.match(OG_TITLE_FALLBACK_REGEX);
  const raw = titleMatch?.[1] ?? ogMatch?.[1];
  if (!raw) return null;

  const name = htmlToText(raw)
    .replace(/\s*\(@[^)]+\)\s*$/, '')
    .replace(/\s*[-|]\s*Telegram$/i, '')
    .trim();

  if (!name) return null;
  if (['unknown', 'deleted account'].includes(name.toLowerCase())) return null;
  if (/^telegram:\s*contact\s*@/i.test(name)) return null;
  if (/^you can (view and join|contact)\s+/i.test(name)) return null;
  if (/^@[\w\d_]+$/.test(name)) return null;
  return name;
}

type FetchResult = {
  bio: string | null;
  displayName: string | null;
  status: 'ok' | 'error' | 'rate-limited' | 'not-found';
  httpCode?: number;
};

async function fetchProfile(username: string, attempt = 1): Promise<FetchResult> {
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
        return fetchProfile(username, attempt + 1);
      }
      return { bio: null, displayName: null, status: 'rate-limited', httpCode: 429 };
    }

    if (resp.status === 404) {
      return { bio: null, displayName: null, status: 'not-found', httpCode: 404 };
    }

    if (!resp.ok) {
      // Other HTTP error — retry transient 5xx
      if (resp.status >= 500 && attempt <= MAX_RETRIES) {
        const wait = RETRY_DELAY_MS * attempt;
        await new Promise((r) => setTimeout(r, wait));
        return fetchProfile(username, attempt + 1);
      }
      return { bio: null, displayName: null, status: 'error', httpCode: resp.status };
    }

    const html = await resp.text();
    return {
      bio: extractBio(html),
      displayName: extractDisplayName(html),
      status: 'ok',
      httpCode: 200,
    };
  } catch (err: unknown) {
    // Network / timeout errors — retry
    if (attempt <= MAX_RETRIES) {
      const wait = RETRY_DELAY_MS * attempt;
      await new Promise((r) => setTimeout(r, wait));
      return fetchProfile(username, attempt + 1);
    }
    return { bio: null, displayName: null, status: 'error', httpCode: undefined };
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
        AND btrim(handle) != ''
        AND (
          bio IS NULL
          OR btrim(bio) = ''
          OR display_name IS NULL
          OR btrim(display_name) = ''
          OR lower(btrim(display_name)) IN ('unknown', 'deleted account')
        )
      ORDER BY id DESC
      ${limitClause}
    `);

    if (rows.length === 0) {
      console.log(`\n🎉 All scrapable users have bios!`);
      break;
    }

    console.log(`\n${'═'.repeat(60)}`);
    console.log(`🔄 Pass ${pass} — ${rows.length} users remaining with missing bio/name`);
    console.log(`${'═'.repeat(60)}`);

    let biosUpdated = 0;
    let namesUpdated = 0;
    let anyUpdated  = 0;
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
          const result = await fetchProfile(user.handle);
          return { id: user.id, handle: user.handle, ...result };
        })
      );

      // Update DB
      for (const r of results) {
        processed++;
        if (r.bio || r.displayName) {
          const updateRes = await db.query<{ bio_applied: boolean; name_applied: boolean }>(`
            WITH input AS (
              SELECT
                $1::text AS bio_in,
                $2::text AS name_in,
                $3::bigint AS user_id
            ),
            prev AS (
              SELECT id, bio, display_name
              FROM users
              WHERE id = (SELECT user_id FROM input)
              FOR UPDATE
            ),
            upd AS (
              UPDATE users u
              SET
                bio = CASE
                  WHEN (SELECT bio_in FROM input) IS NOT NULL AND (prev.bio IS NULL OR btrim(prev.bio) = '') THEN (SELECT bio_in FROM input)
                  ELSE u.bio
                END,
                bio_source = CASE
                  WHEN (SELECT bio_in FROM input) IS NOT NULL AND (prev.bio IS NULL OR btrim(prev.bio) = '') THEN 'tme_scrape'
                  ELSE u.bio_source
                END,
                bio_updated_at = CASE
                  WHEN (SELECT bio_in FROM input) IS NOT NULL AND (prev.bio IS NULL OR btrim(prev.bio) = '') THEN now()
                  ELSE u.bio_updated_at
                END,
                display_name = CASE
                  WHEN (SELECT name_in FROM input) IS NOT NULL AND (
                    prev.display_name IS NULL
                    OR btrim(prev.display_name) = ''
                    OR lower(btrim(prev.display_name)) IN ('unknown', 'deleted account')
                  ) THEN (SELECT name_in FROM input)
                  ELSE u.display_name
                END,
                display_name_source = CASE
                  WHEN (SELECT name_in FROM input) IS NOT NULL AND (
                    prev.display_name IS NULL
                    OR btrim(prev.display_name) = ''
                    OR lower(btrim(prev.display_name)) IN ('unknown', 'deleted account')
                  ) THEN 'tme_scrape'
                  ELSE u.display_name_source
                END,
                display_name_updated_at = CASE
                  WHEN (SELECT name_in FROM input) IS NOT NULL AND (
                    prev.display_name IS NULL
                    OR btrim(prev.display_name) = ''
                    OR lower(btrim(prev.display_name)) IN ('unknown', 'deleted account')
                  ) THEN now()
                  ELSE u.display_name_updated_at
                END
              FROM prev
              WHERE u.id = prev.id
              RETURNING
                ((SELECT bio_in FROM input) IS NOT NULL AND (prev.bio IS NULL OR btrim(prev.bio) = '')) AS bio_applied,
                ((SELECT name_in FROM input) IS NOT NULL AND (
                  prev.display_name IS NULL
                  OR btrim(prev.display_name) = ''
                  OR lower(btrim(prev.display_name)) IN ('unknown', 'deleted account')
                )) AS name_applied
            )
            SELECT bio_applied, name_applied FROM upd
          `, [r.bio, r.displayName, r.id]);

          const flags = updateRes.rows[0];
          if (flags?.bio_applied) biosUpdated++;
          if (flags?.name_applied) namesUpdated++;
          if (flags?.bio_applied || flags?.name_applied) {
            anyUpdated++;
            const pieces: string[] = [];
            if (flags.bio_applied && r.bio) pieces.push(`bio="${r.bio.substring(0, 60)}${r.bio.length > 60 ? '…' : ''}"`);
            if (flags.name_applied && r.displayName) pieces.push(`name="${r.displayName}"`);
            console.log(`  ✅ @${r.handle}: ${pieces.join(' | ')}`);
          }
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
        const parts = [`${biosUpdated} bios`, `${namesUpdated} names`, `${empty} empty`];
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

    grandTotal += anyUpdated;
    console.log(`\n✅ Pass ${pass} complete: ${biosUpdated} bios + ${namesUpdated} names (${anyUpdated} users updated)`);
    console.log(`   Session total updates: ${grandTotal} users`);
    console.log(`   ${empty} empty, ${errors} errors, ${rateLimited} rate-limited, ${notFound} not-found`);

    if (anyUpdated < MIN_PASS_YIELD) {
      console.log(`\n🏁 Yield dropped below ${MIN_PASS_YIELD} — stopping.`);
      break;
    }

    // Brief pause between passes
    console.log(`   ⏳ Pausing 2s before next pass...`);
    await new Promise((r) => setTimeout(r, 2000));
  }

  const { rows: [coverage] } = await db.query<{ bio_count: string; name_count: string }>(`
    SELECT
      COUNT(*) FILTER (WHERE bio IS NOT NULL AND btrim(bio) != '')::text AS bio_count,
      COUNT(*) FILTER (
        WHERE display_name IS NOT NULL
          AND btrim(display_name) != ''
          AND lower(btrim(display_name)) NOT IN ('unknown', 'deleted account')
      )::text AS name_count
    FROM users
  `);
  console.log(`\n🎯 Session updated ${grandTotal} users.`);
  console.log(`   Coverage now: ${coverage.bio_count} users with bios, ${coverage.name_count} users with display names.`);
  await db.close();
}

main().catch((err) => {
  console.error('❌ Fatal:', err);
  process.exit(1);
});
