#!/usr/bin/env node
/**
 * Lightweight regression harness for DM profile extraction heuristics.
 *
 * Usage:
 *   npm run build
 *   npm run dm-extract-regress
 */

import { promises as fsp } from 'node:fs';
import path from 'node:path';
import { extractProfileEventsFromTextHeuristic } from './ingest-dm-jsonl.js';

interface FactExpectation {
  field: string;
  contains?: string;
}

interface ExtractCase {
  id: string;
  text: string;
  expect_any_facts?: FactExpectation[];
  forbid_any_facts?: FactExpectation[];
}

interface FixtureFile {
  cases: ExtractCase[];
}

function matchesExpectation(actual: { field: string; new_value: string }, exp: FactExpectation): boolean {
  if (actual.field !== exp.field) return false;
  if (exp.contains) {
    return actual.new_value.toLowerCase().includes(exp.contains.toLowerCase());
  }
  return true;
}

async function main(): Promise<void> {
  const filePath = path.resolve(process.cwd(), 'testcases/dm_profile_extract_regression.json');
  const raw = await fsp.readFile(filePath, 'utf8');
  const parsed = JSON.parse(raw) as FixtureFile;
  const cases = Array.isArray(parsed.cases) ? parsed.cases : [];

  let passed = 0;
  let failed = 0;

  for (const c of cases) {
    const text = String(c.text || '');
    const events = extractProfileEventsFromTextHeuristic(text);
    const facts = events
      .flatMap((evt) => (Array.isArray(evt.extracted_facts) ? evt.extracted_facts : []))
      .map((f) => ({
        field: String((f as any).field || ''),
        new_value: String((f as any).new_value || ''),
      }))
      .filter((f) => f.field && f.new_value);

    const expect = Array.isArray(c.expect_any_facts) ? c.expect_any_facts : [];
    const forbid = Array.isArray(c.forbid_any_facts) ? c.forbid_any_facts : [];

    let ok = true;

    for (const exp of expect) {
      if (!facts.some((actual) => matchesExpectation(actual, exp))) {
        ok = false;
      }
    }

    for (const exp of forbid) {
      if (facts.some((actual) => matchesExpectation(actual, exp))) {
        ok = false;
      }
    }

    if (ok) {
      passed += 1;
      console.log(`PASS ${c.id} -> facts=${JSON.stringify(facts)}`);
    } else {
      failed += 1;
      console.error(
        `FAIL ${c.id}\n  text=${JSON.stringify(text)}\n  expect_any_facts=${JSON.stringify(expect)}\n  forbid_any_facts=${JSON.stringify(forbid)}\n  facts=${JSON.stringify(facts)}\n  events=${JSON.stringify(events)}`,
      );
    }
  }

  console.log(`\nDM extract regression summary: passed=${passed} failed=${failed}`);
  if (failed > 0) process.exit(1);
}

main().catch((err) => {
  console.error('❌ dm-extract-regress failed:', err);
  process.exit(1);
});

