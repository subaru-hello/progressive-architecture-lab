// PgOrdersRepo: OrdersRepoPort の Postgres 実装。SQL は repo.ts から移植。
// PgOrdersRepo: Postgres implementation of OrdersRepoPort. SQL moved verbatim from repo.ts.
import type { Pool } from 'pg';
import type { OrdersRepoPort } from '../ports.js';
import type { Order } from '../domain/order.js';

export class PgOrdersRepo implements OrdersRepoPort {
  constructor(private readonly pool: Pool) {}

  async insert(args: { userId: number; itemId: number; qty: number }): Promise<Order> {
    const { rows } = await this.pool.query(
      'INSERT INTO orders(user_id, item_id, qty) VALUES($1, $2, $3) RETURNING id, user_id, item_id, qty, created_at',
      [args.userId, args.itemId, args.qty],
    );
    return rows[0];
  }

  async listRecent(): Promise<Order[]> {
    const { rows } = await this.pool.query(
      'SELECT id, user_id, item_id, qty, created_at FROM orders ORDER BY id DESC LIMIT 100',
    );
    return rows;
  }

  // join モード専用: items テーブルと同じ DB に存在する前提で stock チェック + デクリメント +
  // order 挿入をひとつのトランザクションで行う。
  // join-mode only: assumes items is in the same DB; stock check + decrement + insert in one tx.
  async createWithDirectStock(
    args: { userId: number; itemId: number; qty: number },
  ): Promise<{ ok: boolean; order?: Order; stock?: number }> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      const dec = await client.query(
        'UPDATE items SET stock = stock - $2 WHERE id = $1 AND stock >= $2 RETURNING stock',
        [args.itemId, args.qty],
      );
      if (dec.rows.length === 0) {
        await client.query('ROLLBACK');
        return { ok: false, stock: 0 };
      }

      const { rows } = await client.query(
        'INSERT INTO orders(user_id, item_id, qty) VALUES($1, $2, $3) RETURNING id, user_id, item_id, qty, created_at',
        [args.userId, args.itemId, args.qty],
      );
      await client.query('COMMIT');
      return { ok: true, order: rows[0] };
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  async initSchema(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        item_id INT NOT NULL,
        qty INT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
  }
}
