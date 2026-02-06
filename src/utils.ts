/**
 * Shared utility functions.
 */

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

/**
 * Compute SHA-256 hex digest of a file.
 * Used for raw_imports.sha256 to detect re-imports of the same file.
 */
export function fileSha256(filePath: string): string {
  const buf = readFileSync(filePath);
  return createHash('sha256').update(buf).digest('hex');
}

/**
 * Minimal CLI arg parser.
 * Parses --key value or --key=value pairs from process.argv.
 */
export function parseArgs(argv: string[] = process.argv.slice(2)): Record<string, string> {
  const args: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg.startsWith('--')) {
      const eqIdx = arg.indexOf('=');
      if (eqIdx !== -1) {
        // --key=value
        args[arg.slice(2, eqIdx)] = arg.slice(eqIdx + 1);
      } else {
        // --key value
        const next = argv[i + 1];
        if (next && !next.startsWith('--')) {
          args[arg.slice(2)] = next;
          i++;
        } else {
          args[arg.slice(2)] = 'true';
        }
      }
    }
  }
  return args;
}

/**
 * Sleep for ms milliseconds.
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Truncate a string to maxLen, appending '…' if truncated.
 */
export function truncate(s: string, maxLen: number): string {
  if (s.length <= maxLen) return s;
  return s.slice(0, maxLen - 1) + '…';
}
