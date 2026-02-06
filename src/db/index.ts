/**
 * Database connection helper.
 *
 * Thin wrapper around node-postgres ("pg"). All queries use parameterized
 * SQL — no string interpolation, no SQL injection.
 *
 * Usage:
 *   import { db } from '../db/index.js';
 *   const rows = await db.query<User>('SELECT * FROM users WHERE id = $1', [1]);
 *   await db.close();
 */

import dotenv from 'dotenv';
import pg from 'pg';

const { Pool } = pg;

// Load .env from project root (works whether we run from dist/ or root)
dotenv.config();

// ── Pool setup ──────────────────────────────────────────

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

pool.on('error', (err) => {
  console.error('[db] unexpected pool error:', err.message);
});

// ── Public API ──────────────────────────────────────────

export interface QueryResult<T = Record<string, unknown>> {
  rows: T[];
  rowCount: number | null;
}

/**
 * Run a parameterized query against the pool.
 * Returns typed rows.
 */
async function query<T = Record<string, unknown>>(
  text: string,
  params?: unknown[],
): Promise<QueryResult<T>> {
  const result = await pool.query(text, params);
  return { rows: result.rows as T[], rowCount: result.rowCount };
}

/**
 * Run a callback inside a transaction.
 * - BEGIN is called before your callback
 * - COMMIT is called if the callback completes
 * - ROLLBACK is called if the callback throws
 *
 * The client passed to the callback should be used for all queries
 * within the transaction so they share the same connection.
 */
async function transaction<T>(
  fn: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Get a raw client from the pool (for batch operations that
 * need manual transaction control). Caller must release.
 */
async function getClient(): Promise<pg.PoolClient> {
  return pool.connect();
}

/**
 * Gracefully close the pool. Call this before process exit.
 */
async function close(): Promise<void> {
  await pool.end();
}

export const db = {
  query,
  transaction,
  getClient,
  close,
  pool, // exposed for advanced use / testing
} as const;
