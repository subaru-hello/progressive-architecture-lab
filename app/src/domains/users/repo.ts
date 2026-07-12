import type { Pool } from 'pg';

export interface User {
  id: number;
  name: string;
}

export async function initSchema(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      token TEXT UNIQUE NOT NULL,
      name TEXT NOT NULL
    );
  `);
}

// デモ用シードデータ: 冪等（ON CONFLICT DO NOTHING）。
// Demo seed: idempotent via ON CONFLICT DO NOTHING.
export async function seed(pool: Pool): Promise<void> {
  await pool.query(`
    INSERT INTO users (token, name) VALUES
      ('demo-token-1', 'Alice'),
      ('demo-token-2', 'Bob')
    ON CONFLICT (token) DO NOTHING;
  `);
}

// token で認証。該当ユーザーなければ null を返す（例外なし）。
// Authenticate by token; returns null (no throw) when not found.
export async function authenticate(pool: Pool, token: string): Promise<User | null> {
  const { rows } = await pool.query(
    'SELECT id, name FROM users WHERE token = $1',
    [token],
  );
  return rows[0] ?? null;
}
