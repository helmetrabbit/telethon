#!/usr/bin/env node
/**
 * Lightweight regression harness for psychographic profiles.
 *
 * Usage:
 *   npm run profile-regress
 */

import fs from 'fs';
import path from 'path';
import { db } from '../db/index.js';

type Assertion =
  | { path: string; contains?: string; contains_any?: string[]; exists?: boolean; max_length?: number };

type Case = {
  id: string;
  enabled: boolean;
  user_id: number | null;
  assert?: Assertion[];
};

async function main() {
  const filePath = path.resolve(process.cwd(), 'testcases/profile_regression.json');
  const raw = fs.readFileSync(filePath, 'utf-8');
  const data = JSON.parse(raw);
  const cases: Case[] = data.cases ?? [];

  let passed = 0;
  let failed = 0;

  for (const c of cases) {
    if (!c.enabled) {
      console.log(`- SKIP ${c.id} (disabled)`);
      continue;
    }
    if (!c.user_id) {
      console.log(`- SKIP ${c.id} (missing user_id)`);
      continue;
    }

    const { rows } = await db.query<{
      primary_company: string | null;
      role_company_timeline: any;
      created_at: string;
    }>(`
      SELECT primary_company, role_company_timeline, created_at
      FROM user_psychographics
      WHERE user_id = $1
      ORDER BY created_at DESC
      LIMIT 1
    `, [c.user_id]);

    if (rows.length === 0) {
      console.log(`- FAIL ${c.id}: no psychographic profile found for user_id=${c.user_id}`);
      failed++;
      continue;
    }

    const row = rows[0];
    const profile = {
      primary_company: row.primary_company,
      role_company_timeline: Array.isArray(row.role_company_timeline) ? row.role_company_timeline : [],
      conflict_notes: (row as any).conflict_notes || null,
    };

    let ok = true;
    const asserts = c.assert ?? [];
    for (const a of asserts) {
      const value = a.path.split('.').reduce((acc: any, key) => (acc ? acc[key] : undefined), profile);
      if (a.exists !== undefined) {
        if (a.exists && (value === undefined || value === null)) ok = false;
        if (!a.exists && value !== undefined && value !== null) ok = false;
      }
      if (a.contains) {
        const v = Array.isArray(value) ? JSON.stringify(value) : String(value ?? '');
        if (!v.includes(a.contains)) ok = false;
      }
      if (a.contains_any) {
        const v = Array.isArray(value) ? JSON.stringify(value) : String(value ?? '');
        if (!a.contains_any.some((s) => v.includes(s))) ok = false;
      }
      if (a.max_length !== undefined) {
        const v = String(value ?? '');
        if (v.length > a.max_length) ok = false;
      }
    }

    if (ok) {
      console.log(`✅ PASS ${c.id}`);
      passed++;
    } else {
      console.log(`❌ FAIL ${c.id}`);
      console.log(`   assertions failed: ${JSON.stringify(asserts)}`);
      failed++;
    }
  }

  console.log(`\nSummary: ${passed} passed, ${failed} failed.`);
  await db.close();
}

main().catch((err) => {
  console.error('❌ profile-regress failed:', err);
  process.exit(1);
});
