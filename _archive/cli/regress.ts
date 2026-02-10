/**
 * Regression harness — unit-tests the inference engine against
 * known-good test cases WITHOUT touching the database.
 *
 * Usage:
 *   npm run regress
 *
 * Reads testcases/regression.json and runs each case through
 * scoreUser(), then checks expected/forbidden predicates.
 *
 * Exit code 0 = all pass, 1 = failures.
 */

import { readFileSync } from 'fs';
import { resolve } from 'path';
import { scoreUser, normalizeOrgName, type UserInferenceInput } from '../inference/engine.js';
import { loadInferenceConfig } from '../config/inference-config.js';
import type { Role, Intent } from '../config/taxonomies.js';

// ── Types ───────────────────────────────────────────────

interface PredicateCheck {
  predicate: 'has_role' | 'has_intent' | 'affiliated_with' | 'has_org_type';
  value?: string;
  status?: 'supported' | 'tentative';
}

interface TestCase {
  id: string;
  description: string;
  displayName: string | null;
  bio: string | null;
  messages: string[];
  memberGroupKinds: string[];
  totalMsgCount: number;
  totalReplyCount?: number;
  totalMentionCount?: number;
  expected: PredicateCheck[];
  forbidden: PredicateCheck[];
  expectedGated?: boolean;
}

interface TestSuite {
  cases: TestCase[];
}

// ── Status computation (mirrors infer-claims.ts Gate 3 logic) ──

function computeStatus(
  probability: number,
  totalMsgCount: number,
  evidence: { evidence_type: string }[],
): 'supported' | 'tentative' {
  const hasSubstantive = evidence.some(
    (e) => e.evidence_type === 'bio' || e.evidence_type === 'message' || e.evidence_type === 'display_name',
  );
  return (probability >= 0.55 && totalMsgCount >= 5 && hasSubstantive)
    ? 'supported'
    : 'tentative';
}

// ── Main ────────────────────────────────────────────────

function main(): void {
  const config = loadInferenceConfig();
  const suitePath = resolve('testcases/regression.json');
  const raw = readFileSync(suitePath, 'utf-8');
  const suite: TestSuite = JSON.parse(raw);

  console.log(`\n🧪 Regression harness — ${suite.cases.length} test cases (${config.version})\n`);

  let passed = 0;
  let failed = 0;
  const failures: string[] = [];

  for (const tc of suite.cases) {
    const input: UserInferenceInput = {
      userId: 0,
      displayName: tc.displayName,
      bio: tc.bio,
      memberGroupKinds: tc.memberGroupKinds as any,
      messageTexts: tc.messages,
      totalMsgCount: tc.totalMsgCount,
      totalReplyCount: tc.totalReplyCount ?? 0,
      totalMentionCount: tc.totalMentionCount ?? 0,
      avgMsgLen: tc.messages.length > 0 ? tc.messages.reduce((s, m) => s + m.length, 0) / tc.messages.length : 0,
      bdGroupMsgShare: 0,
      groupsActiveCount: tc.memberGroupKinds.length,
    };

    const result = scoreUser(input, config);
    const errors: string[] = [];

    // Build actual predicates list for matching
    interface ActualPredicate {
      predicate: string;
      value: string;
      status: 'supported' | 'tentative';
    }

    const actuals: ActualPredicate[] = [];

    for (const claim of result.roleClaims) {
      const status = computeStatus(claim.probability, input.totalMsgCount, claim.evidence);
      actuals.push({ predicate: 'has_role', value: claim.label, status });
    }

    if (result.intentClaim) {
      const status = computeStatus(result.intentClaim.probability, input.totalMsgCount, result.intentClaim.evidence);
      actuals.push({ predicate: 'has_intent', value: result.intentClaim.label, status });
    }

    for (const aff of result.affiliations) {
      actuals.push({ predicate: 'affiliated_with', value: aff.name, status: 'supported' });
    }

    for (const ot of result.orgTypes) {
      actuals.push({ predicate: 'has_org_type', value: ot.orgType, status: 'supported' });
    }

    // Check expected predicates
    for (const exp of tc.expected) {
      const found = actuals.find((a) => {
        if (a.predicate !== exp.predicate) return false;
        if (exp.value !== undefined) {
          // Fuzzy match on org names (normalised comparison)
          if (exp.predicate === 'affiliated_with') {
            return normalizeOrgName(a.value) === normalizeOrgName(exp.value);
          }
          if (a.value !== exp.value) return false;
        }
        if (exp.status !== undefined && a.status !== exp.status) return false;
        return true;
      });
      if (!found) {
        const detail = exp.value ? `${exp.predicate}=${exp.value}` : exp.predicate;
        const statusNote = exp.status ? ` (status=${exp.status})` : '';
        errors.push(`MISSING expected: ${detail}${statusNote}`);
      }
    }

    // Check forbidden predicates
    for (const fb of tc.forbidden) {
      const found = actuals.find((a) => {
        if (a.predicate !== fb.predicate) return false;
        if (fb.value !== undefined) {
          if (fb.predicate === 'affiliated_with') {
            return normalizeOrgName(a.value) === normalizeOrgName(fb.value);
          }
          if (a.value !== fb.value) return false;
        }
        if (fb.status !== undefined && a.status !== fb.status) return false;
        return true;
      });
      if (found) {
        const detail = fb.value ? `${fb.predicate}=${fb.value}` : fb.predicate;
        const statusNote = fb.status ? ` (status=${fb.status})` : '';
        errors.push(`FOUND forbidden: ${detail}${statusNote} → got ${found.value} (${found.status})`);
      }
    }

    // Check gating expectation
    if (tc.expectedGated && (result.roleClaims.length > 0 || result.intentClaim)) {
      errors.push(`Expected fully gated but got claims`);
    }

    if (errors.length === 0) {
      passed++;
      console.log(`  ✅ ${tc.id}`);
    } else {
      failed++;
      const summary = `  ❌ ${tc.id}\n${errors.map((e) => `     → ${e}`).join('\n')}`;
      console.log(summary);
      failures.push(summary);

      // Print actual predicates for debugging
      if (actuals.length > 0) {
        console.log(`     actuals: ${actuals.map((a) => `${a.predicate}=${a.value}(${a.status})`).join(', ')}`);
      } else {
        console.log(`     actuals: (none — fully gated)`);
        if (result.gatingNotes.length > 0) {
          console.log(`     gating: ${result.gatingNotes.join('; ')}`);
        }
      }
    }
  }

  // ── Summary ──────────────────────────────────────────
  console.log('\n' + '━'.repeat(50));
  console.log(`🧪 Results: ${passed} passed, ${failed} failed out of ${suite.cases.length}`);

  if (failures.length > 0) {
    console.log('\n📋 Failures:\n');
    for (const f of failures) {
      console.log(f);
    }
    process.exit(1);
  } else {
    console.log('🎉 All regression tests passed!\n');
    process.exit(0);
  }
}

main();
