// PgUsersRepo: UsersRepoPort の Postgres 実装。SQL は repo.ts から移植。
// PgUsersRepo: Postgres implementation of UsersRepoPort. SQL moved verbatim from repo.ts.
import type { Pool } from 'pg';
import type { UsersRepoPort } from '../ports.js';
import type { User } from '../domain/user.js';

export class PgUsersRepo implements UsersRepoPort {
  constructor(private readonly pool: Pool) {}

  async initSchema(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        token TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL
      );
    `);
  }

  // デモ用シードデータ: 冪等（ON CONFLICT DO NOTHING）。
  // Demo seed: idempotent via ON CONFLICT DO NOTHING.
  async seed(): Promise<void> {
    await this.pool.query(`
      INSERT INTO users (token, name) VALUES
        ('demo-token-1', 'Alice'),
        ('demo-token-2', 'Bob')
      ON CONFLICT (token) DO NOTHING;
    `);
  }

  // token で認証。該当ユーザーなければ null を返す（例外なし）。
  // Authenticate by token; returns null (no throw) when not found.
  async authenticate(token: string): Promise<User | null> {
    const { rows } = await this.pool.query(
      'SELECT id, name FROM users WHERE token = $1',
      [token],
    );
    return rows[0] ?? null;
  }
}
