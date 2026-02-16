/**
 * Backfill missing user display names from Telegram export sender strings.
 *
 * High-precision strategy:
 * - only targets users whose display_name is missing/placeholder
 * - requires strong consensus from message sender names, or participant anchor
 * - never overwrites a non-placeholder display_name
 *
 * Usage:
 *   npm run backfill-message-names
 *   npm run backfill-message-names -- --dir data/exports --min-count 2 --min-share 0.75
 */

import fs from 'node:fs';
import path from 'node:path';
import { db } from '../db/index.js';
import { parseArgs } from '../utils.js';

type CandidateUser = {
  id: number;
  external_id: string;
};

type NameStats = {
  score: number;
  msg_count: number;
  participant_count: number;
  latest_ts: number;
};

type Choice = {
  name: string;
  reason: string;
};

const PLACEHOLDERS = new Set(['unknown', 'deleted account']);

function isMissingDisplayName(name: unknown): boolean {
  if (typeof name !== 'string') return true;
  const t = name.trim();
  if (!t) return true;
  return PLACEHOLDERS.has(t.toLowerCase());
}

function normalizeName(raw: unknown): string | null {
  if (typeof raw !== 'string') return null;
  const cleaned = raw.replace(/\s+/g, ' ').trim();
  if (!cleaned) return null;
  const lc = cleaned.toLowerCase();
  if (PLACEHOLDERS.has(lc)) return null;
  if (/^@\w{3,32}$/.test(cleaned)) return null;
  if (/^user\d+$/i.test(cleaned)) return null;
  if (/^telegram\b/i.test(cleaned)) return null;
  if (/https?:\/\//i.test(cleaned)) return null;
  if (cleaned.length > 80) return null;
  return cleaned;
}

function toTs(raw: unknown): number {
  if (typeof raw !== 'string') return 0;
  const ms = Date.parse(raw);
  return Number.isFinite(ms) ? ms : 0;
}

function getOrCreateStats(
  byUser: Map<string, Map<string, NameStats>>,
  externalId: string,
  name: string,
): NameStats {
  let names = byUser.get(externalId);
  if (!names) {
    names = new Map<string, NameStats>();
    byUser.set(externalId, names);
  }
  let stats = names.get(name);
  if (!stats) {
    stats = {
      score: 0,
      msg_count: 0,
      participant_count: 0,
      latest_ts: 0,
    };
    names.set(name, stats);
  }
  return stats;
}

function chooseName(names: Map<string, NameStats>, minCount: number, minShare: number): Choice | null {
  const ranked = Array.from(names.entries()).sort((a, b) => {
    if (b[1].score !== a[1].score) return b[1].score - a[1].score;
    if (b[1].msg_count !== a[1].msg_count) return b[1].msg_count - a[1].msg_count;
    return b[1].latest_ts - a[1].latest_ts;
  });
  if (ranked.length === 0) return null;

  const [topName, top] = ranked[0];
  const second = ranked[1]?.[1];
  const secondScore = second?.score ?? 0;

  // Participant-anchored evidence is treated as high confidence.
  if (top.participant_count > 0) {
    const gap = top.score - secondScore;
    if (gap >= 1) {
      return { name: topName, reason: `participant_anchor score=${top.score} gap=${gap}` };
    }
    return null;
  }

  const totalMsg = ranked.reduce((acc, [, s]) => acc + s.msg_count, 0);
  if (totalMsg <= 0) return null;
  const share = top.msg_count / totalMsg;
  const secondMsg = second?.msg_count ?? 0;

  if (top.msg_count >= minCount && share >= minShare && top.msg_count > secondMsg) {
    return {
      name: topName,
      reason: `message_consensus top=${top.msg_count}/${totalMsg} share=${share.toFixed(2)}`,
    };
  }

  return null;
}

function listExportFiles(dir: string, fileArg?: string): string[] {
  if (fileArg) {
    return fileArg
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map((f) => path.resolve(process.cwd(), f));
  }

  const absDir = path.resolve(process.cwd(), dir);
  const entries = fs
    .readdirSync(absDir, { withFileTypes: true })
    .filter((d) => d.isFile() && d.name.endsWith('.json'))
    .map((d) => path.join(absDir, d.name))
    .sort();

  return entries;
}

async function main(): Promise<void> {
  const args = parseArgs();
  const dir = args.dir ?? 'data/exports';
  const files = listExportFiles(dir, args.file);
  const minCount = Number.parseInt(args['min-count'] ?? '2', 10);
  const minShare = Number.parseFloat(args['min-share'] ?? '0.75');
  const onlyNoHandle = String(args['only-no-handle'] ?? 'false').toLowerCase() === 'true';

  if (!Number.isFinite(minCount) || minCount < 1) {
    throw new Error(`Invalid --min-count: ${args['min-count']}`);
  }
  if (!Number.isFinite(minShare) || minShare <= 0 || minShare > 1) {
    throw new Error(`Invalid --min-share: ${args['min-share']}`);
  }

  if (files.length === 0) {
    throw new Error('No export files found to scan.');
  }

  const noHandleClause = onlyNoHandle ? "AND (handle IS NULL OR btrim(handle) = '')" : '';
  const { rows: candidateRows } = await db.query<CandidateUser>(`
    SELECT id, external_id
    FROM users
    WHERE platform = 'telegram'
      AND (display_name IS NULL OR btrim(display_name) = '' OR lower(btrim(display_name)) IN ('unknown', 'deleted account'))
      AND external_id ~ '^user[0-9]+$'
      ${noHandleClause}
  `);

  if (candidateRows.length === 0) {
    console.log('✅ No missing-name candidates found.');
    await db.close();
    return;
  }

  const candidateByExternal = new Map<string, CandidateUser>();
  for (const c of candidateRows) candidateByExternal.set(c.external_id, c);

  console.log(`🔎 Candidates: ${candidateRows.length}`);
  console.log(`📁 Export files: ${files.length}`);
  console.log(`⚙️  Rules: min_count=${minCount}, min_share=${minShare.toFixed(2)}, only_no_handle=${onlyNoHandle}`);

  const byUser = new Map<string, Map<string, NameStats>>();
  let scannedMessages = 0;
  let scannedParticipants = 0;

  for (const file of files) {
    const raw = fs.readFileSync(file, 'utf8');
    const parsed = JSON.parse(raw) as {
      participants?: unknown[];
      messages?: unknown[];
    };

    const participants = Array.isArray(parsed.participants) ? parsed.participants : [];
    const messages = Array.isArray(parsed.messages) ? parsed.messages : [];

    for (const p of participants) {
      if (!p || typeof p !== 'object') continue;
      const row = p as Record<string, unknown>;
      const userId = row.user_id;
      if (typeof userId !== 'number') continue;
      const externalId = `user${userId}`;
      if (!candidateByExternal.has(externalId)) continue;

      const partName = normalizeName(
        row.display_name ?? [row.first_name, row.last_name].filter((x) => typeof x === 'string' && String(x).trim()).join(' '),
      );
      if (!partName) continue;

      const stats = getOrCreateStats(byUser, externalId, partName);
      stats.participant_count += 1;
      stats.score += 3;
      scannedParticipants += 1;
    }

    for (const m of messages) {
      if (!m || typeof m !== 'object') continue;
      const row = m as Record<string, unknown>;
      const type = row.type;
      if (typeof type === 'string' && type !== 'message') continue;

      const fromId = row.from_id;
      if (typeof fromId !== 'string') continue;
      if (!candidateByExternal.has(fromId)) continue;

      const name = normalizeName(row.from);
      if (!name) continue;

      const stats = getOrCreateStats(byUser, fromId, name);
      stats.msg_count += 1;
      stats.score += 1;
      const ts = toTs(row.date);
      if (ts > stats.latest_ts) stats.latest_ts = ts;
      scannedMessages += 1;
    }

    console.log(`   scanned ${path.basename(file)} (msgs=${messages.length}, participants=${participants.length})`);
  }

  const choices: Array<{ user_id: number; name: string; reason: string }> = [];
  let ambiguous = 0;

  for (const [externalId, names] of byUser.entries()) {
    const pick = chooseName(names, minCount, minShare);
    if (!pick) {
      ambiguous += 1;
      continue;
    }
    const user = candidateByExternal.get(externalId);
    if (!user) continue;
    choices.push({ user_id: user.id, name: pick.name, reason: pick.reason });
  }

  if (choices.length === 0) {
    console.log('ℹ️ No high-confidence name updates found.');
    console.log(`   scanned messages with candidate senders: ${scannedMessages}`);
    console.log(`   scanned participant anchors: ${scannedParticipants}`);
    console.log(`   ambiguous users: ${ambiguous}`);
    await db.close();
    return;
  }

  let updated = 0;
  await db.transaction(async (client) => {
    for (const c of choices) {
      const res = await client.query(
        `
        UPDATE users
        SET display_name = $1,
            display_name_source = 'message_sender_backfill',
            display_name_updated_at = now()
        WHERE id = $2
          AND (display_name IS NULL OR btrim(display_name) = '' OR lower(btrim(display_name)) IN ('unknown', 'deleted account'))
        `,
        [c.name, c.user_id],
      );
      if ((res.rowCount ?? 0) > 0) updated += 1;
    }
  });

  console.log('✅ Message-sender name backfill complete:');
  console.log(`   scanned messages with candidate senders: ${scannedMessages}`);
  console.log(`   scanned participant anchors: ${scannedParticipants}`);
  console.log(`   candidate choices: ${choices.length}`);
  console.log(`   updated users: ${updated}`);
  console.log(`   ambiguous users skipped: ${ambiguous}`);

  const preview = choices.slice(0, 10);
  if (preview.length > 0) {
    console.log('   sample choices:');
    for (const p of preview) {
      console.log(`     user_id=${p.user_id} -> "${p.name}" (${p.reason})`);
    }
  }

  await db.close();
}

main().catch(async (err) => {
  console.error('❌ backfill-message-names failed:', err);
  try {
    await db.close();
  } catch {
    // no-op
  }
  process.exit(1);
});
