#!/usr/bin/env node
import { promises as fsp } from 'node:fs';
import path from 'node:path';
import { extractContactStyleDirectives, normalizeContactStyle } from '../lib/dm-contact-style.js';

interface StyleCase {
  id: string;
  text: string;
  expected_any: string[];
  expected_none?: boolean;
}

interface FixtureFile {
  cases: StyleCase[];
}

function hasExpectedMatch(actual: string[], expectedAny: string[]): boolean {
  const actualSet = new Set(actual.map((item) => item.trim().toLowerCase()).filter(Boolean));
  for (const candidate of expectedAny) {
    if (actualSet.has(candidate.trim().toLowerCase())) return true;
  }
  return false;
}

async function main(): Promise<void> {
  const filePath = path.resolve(process.cwd(), 'testcases/dm_contact_style_regression.json');
  const raw = await fsp.readFile(filePath, 'utf8');
  const parsed = JSON.parse(raw) as FixtureFile;
  const cases = Array.isArray(parsed.cases) ? parsed.cases : [];

  let passed = 0;
  let failed = 0;

  for (const c of cases) {
    const text = String(c.text || '');
    const directives = extractContactStyleDirectives(text);
    const expectNone = Boolean((c as any).expected_none);
    if (expectNone) {
      const ok = directives.length === 0;
      if (ok) {
        passed += 1;
        console.log(`PASS ${c.id} -> directives=[]`);
      } else {
        failed += 1;
        console.error(
          `FAIL ${c.id}\n  text=${JSON.stringify(text)}\n  expected_none=true\n  directives=${JSON.stringify(directives)}`,
        );
      }
      continue;
    }

    const expected = Array.isArray(c.expected_any) ? c.expected_any : [];
    const fallback = normalizeContactStyle(text);
    const actual = directives.length > 0 ? directives : (fallback ? [fallback] : []);
    const ok = hasExpectedMatch(actual, expected);

    if (ok) {
      passed += 1;
      console.log(`PASS ${c.id} -> actual=${JSON.stringify(actual)}`);
    } else {
      failed += 1;
      console.error(
        `FAIL ${c.id}\n  text=${JSON.stringify(text)}\n  expected_any=${JSON.stringify(expected)}\n  actual=${JSON.stringify(actual)}`,
      );
    }
  }

  console.log(`\nStyle regression summary: passed=${passed} failed=${failed}`);
  if (failed > 0) process.exit(1);
}

main().catch((err) => {
  console.error('❌ dm-style-regress failed:', err);
  process.exit(1);
});
