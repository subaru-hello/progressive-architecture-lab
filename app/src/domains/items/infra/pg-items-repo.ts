// PgItemsRepo: ItemsRepoPort の Postgres 実装。SQL は repo.ts から移植。
// PgItemsRepo: Postgres implementation of ItemsRepoPort. SQL moved verbatim from repo.ts.
import type { Pool } from 'pg';
import type { ItemsRepoPort } from '../ports.js';
import type { Item } from '../domain/item.js';

export class PgItemsRepo implements ItemsRepoPort {
  constructor(
    private readonly writePool: Pool,
    private readonly readPool: Pool,
  ) {}

  async initSchema(): Promise<void> {
    await this.writePool.query(`
      CREATE TABLE IF NOT EXISTS items (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        stock INT NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
  }

  async listLatest100(): Promise<Pick<Item, 'id' | 'name' | 'created_at'>[]> {
    const { rows } = await this.readPool.query(
      'SELECT id, name, created_at FROM items ORDER BY id DESC LIMIT 100',
    );
    return rows;
  }

  // Lv18 バックフィル: LIMIT なしで全件返す（ソース DB 読み取り用）。
  // Lv18 backfill: return all rows without LIMIT (for reading the source-of-truth DB).
  async listAll(): Promise<Item[]> {
    const { rows } = await this.readPool.query(
      'SELECT id, name, stock, created_at FROM items ORDER BY id',
    );
    return rows;
  }

  // Lv18 バックフィル: id 保持の冪等 upsert。INSERT と setval を同一トランザクションで実行。
  // Lv18 backfill: idempotent upsert preserving explicit ids. INSERT + setval in one transaction
  // so the sequence is always consistent with MAX(id) — mirrors the pattern used by seed().
  async bulkUpsert(rows: Pick<Item, 'id' | 'name' | 'stock' | 'created_at'>[]): Promise<number> {
    if (rows.length === 0) return 0;
    const values: string[] = [];
    const params: (number | string)[] = [];
    let p = 1;
    for (const row of rows) {
      values.push(`($${p++}, $${p++}, $${p++}, $${p++})`);
      params.push(row.id, row.name, row.stock, row.created_at);
    }
    const c = await this.writePool.connect();
    try {
      await c.query('BEGIN');
      await c.query(
        `INSERT INTO items (id, name, stock, created_at) VALUES ${values.join(', ')}
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, stock = EXCLUDED.stock`,
        params,
      );
      // 早期リターン済みなのでここに来た時点で MAX(id) は必ず non-null。
      // Early-return guarantees rows.length > 0, so MAX(id) is non-null here.
      await c.query(
        `SELECT setval(pg_get_serial_sequence('items','id'), (SELECT MAX(id) FROM items))`,
      );
      await c.query('COMMIT');
    } catch (err) {
      await c.query('ROLLBACK');
      throw err;
    } finally {
      c.release();
    }
    return rows.length;
  }

  async getById(id: number): Promise<Item | null> {
    const { rows } = await this.readPool.query(
      'SELECT id, name, stock, created_at FROM items WHERE id = $1',
      [id],
    );
    return rows[0] ?? null;
  }

  async create(name: string): Promise<Pick<Item, 'id' | 'name' | 'created_at'>> {
    const { rows } = await this.writePool.query(
      'INSERT INTO items(name) VALUES($1) RETURNING id, name, created_at',
      [name],
    );
    return rows[0];
  }

  // デモ用シードデータ: 複数レプリカが同時に起動しても重複しないよう
  // pg_advisory_xact_lock でシリアライズし、空チェックをロック内で再実施する。
  // Demo seed: race-safe under concurrent replicas — advisory lock serializes seeders;
  // emptiness re-checked inside the transaction so only one replica actually inserts.
  async seed(): Promise<void> {
    const conn = await this.writePool.connect();
    try {
      await conn.query('BEGIN');
      // 定数ロック ID (823041 = 任意定数 — 衝突回避のため固定)。
      // Constant lock id (823041 = arbitrary; fixed to avoid cross-feature collision).
      await conn.query('SELECT pg_advisory_xact_lock(823041)');

      const { rows: check } = await conn.query('SELECT 1 FROM items LIMIT 1');
      if (check.length > 0) {
        // 既にデータあり → 何もしない。
        // Already seeded by another replica — do nothing.
        await conn.query('ROLLBACK');
        return;
      }

      const values: string[] = [];
      const params: (string | number)[] = [];
      for (let i = 1; i <= 50; i++) {
        const base = (i - 1) * 2;
        values.push(`($${base + 1}, $${base + 2})`);
        params.push(`demo-item-${i}`, 1_000_000);
      }
      await conn.query(
        `INSERT INTO items(name, stock) VALUES ${values.join(', ')}`,
        params,
      );
      await conn.query('COMMIT');
    } catch (err) {
      await conn.query('ROLLBACK');
      throw err;
    } finally {
      conn.release();
    }
  }

  // Atomic: stock が足りる場合のみデクリメント。足りなければ ok:false を返す（例外なし）。
  // Atomic: decrements only when stock >= qty; returns ok:false (no throw) when insufficient.
  async decrementStock(id: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    const { rows } = await this.writePool.query(
      'UPDATE items SET stock = stock - $2 WHERE id = $1 AND stock >= $2 RETURNING stock',
      [id, qty],
    );
    if (rows.length === 0) return { ok: false, stock: 0 };
    return { ok: true, stock: rows[0].stock };
  }
}
