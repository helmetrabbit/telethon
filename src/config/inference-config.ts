/**
 * Inference config loader.
 *
 * Reads a versioned JSON config file from config/inference.*.json.
 * The version string from the JSON is used as model_version on all
 * claims and abstention log entries.
 *
 * To create a new inference version:
 *   1. Copy config/inference.v0.2.0.json → config/inference.v0.3.0.json
 *   2. Edit weights/thresholds as needed
 *   3. Update the "version" field inside the JSON
 *   4. Run with: INFERENCE_CONFIG=config/inference.v0.3.0.json npm run infer-claims
 */

import { readFileSync } from 'fs';
import { resolve } from 'path';
import type { GroupKind, Role, Intent } from './taxonomies.js';

// ── Types ───────────────────────────────────────────────

export interface InferenceConfig {
  version: string;
  description: string;
  gating: {
    minNonMembershipEvidence: number;
    minClaimConfidence: number;
  };
  rolePriors: Record<GroupKind, Partial<Record<Role, number>>>;
  intentPriors: Record<GroupKind, Partial<Record<Intent, number>>>;
}

// ── Default config path ─────────────────────────────────

const DEFAULT_CONFIG_PATH = 'config/inference.v0.5.1.json';

// ── Loader ──────────────────────────────────────────────

let _cached: InferenceConfig | null = null;

export function loadInferenceConfig(configPath?: string): InferenceConfig {
  if (_cached && !configPath) return _cached;

  const path = configPath ?? process.env.INFERENCE_CONFIG ?? DEFAULT_CONFIG_PATH;
  const absPath = resolve(path);

  const raw = readFileSync(absPath, 'utf-8');
  const parsed = JSON.parse(raw) as InferenceConfig;

  // Basic validation
  if (!parsed.version || typeof parsed.version !== 'string') {
    throw new Error(`Inference config at ${absPath} is missing a "version" field`);
  }
  if (!parsed.gating?.minNonMembershipEvidence || !parsed.gating?.minClaimConfidence) {
    throw new Error(`Inference config at ${absPath} is missing gating thresholds`);
  }
  if (!parsed.rolePriors || !parsed.intentPriors) {
    throw new Error(`Inference config at ${absPath} is missing priors`);
  }

  console.log(`   📋 Loaded inference config: ${parsed.version} (${absPath})`);

  if (!configPath) _cached = parsed;
  return parsed;
}
