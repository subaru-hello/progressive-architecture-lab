// mud/schema.ts — ARCH=mud 専用のスキーマ初期化とシード。
// mud/schema.ts — Schema init + seed for ARCH=mud only.
//
// 意図的な結合: users・items・orders が FK で繋がった ONE SHARED SCHEMA。
// Intentional coupling: users, items, orders in ONE shared schema with cross-domain FKs.
// これはヘックス版とは切り分けられた別テーブル名は使わず、同名テーブルを共有 DB に作る。
// (Same table names as hex — mud is loaded exclusively when ARCH=mud, never simultaneously.)

import type { Pool } from 'pg';

export async function mudInitSchema(pool: Pool): Promise<void> {
  // 3 ドメインのテーブルを一括で CREATE — 意図的にひとつのクエリブロックに詰め込む。
  // All three domain tables in one block — intentionally monolithic DDL.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id            SERIAL PRIMARY KEY,
      token         TEXT UNIQUE NOT NULL,
      name          TEXT NOT NULL,
      last_order_at TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS items (
      id         SERIAL PRIMARY KEY,
      name       TEXT NOT NULL,
      stock      INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    -- orders は users・items を FK で参照 — ドメイン間結合が明示的に埋め込まれている。
    -- orders has cross-domain FKs to users and items — coupling is deliberate and visible.
    CREATE TABLE IF NOT EXISTS orders (
      id         SERIAL PRIMARY KEY,
      user_id    INT NOT NULL REFERENCES users(id),
      item_id    INT NOT NULL REFERENCES items(id),
      qty        INT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}

// デモシード: items の seed と同じ pg_advisory_xact_lock パターンで冪等化。
// Demo seed: idempotent using the same pg_advisory_xact_lock pattern as items seed.
export async function mudSeed(pool: Pool): Promise<void> {
  const conn = await pool.connect();
  try {
    await conn.query('BEGIN');
    // ロック ID 823042 — items seed の 823041 と衝突しない任意定数。
    // Lock id 823042 — distinct from items seed lock (823041).
    await conn.query('SELECT pg_advisory_xact_lock(823042)');

    const { rows: check } = await conn.query('SELECT 1 FROM items LIMIT 1');
    if (check.length > 0) {
      // 既にシード済み — 何もしない。
      // Already seeded — no-op.
      await conn.query('ROLLBACK');
      return;
    }

    // 50 デモアイテム (stock=1_000_000)。
    // 50 demo items (stock=1_000_000).
    const values: string[] = [];
    const params: (string | number)[] = [];
    for (let i = 1; i <= 50; i++) {
      const base = (i - 1) * 2;
      values.push(`($${base + 1}, $${base + 2})`);
      params.push(`demo-item-${i}`, 1_000_000);
    }
    await conn.query(`INSERT INTO items(name, stock) VALUES ${values.join(', ')}`, params);

    // 2 デモユーザー — items シードと同じトランザクションで挿入。
    // 2 demo users — inserted in the same transaction as items seed.
    await conn.query(`
      INSERT INTO users (token, name) VALUES
        ('demo-token-1', 'Alice'),
        ('demo-token-2', 'Bob')
      ON CONFLICT (token) DO NOTHING;
    `);

    await conn.query('COMMIT');
  } catch (err) {
    await conn.query('ROLLBACK');
    throw err;
  } finally {
    conn.release();
  }
}
