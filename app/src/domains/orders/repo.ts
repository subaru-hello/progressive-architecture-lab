import type { Pool } from 'pg';

export interface Order {
  id: number;
  user_id: number;
  item_id: number;
  qty: number;
  created_at: string;
}

export async function initSchema(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      id SERIAL PRIMARY KEY,
      user_id INT NOT NULL,
      item_id INT NOT NULL,
      qty INT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}

export async function insert(pool: Pool, args: { userId: number; itemId: number; qty: number }): Promise<Order> {
  const { rows } = await pool.query(
    'INSERT INTO orders(user_id, item_id, qty) VALUES($1, $2, $3) RETURNING id, user_id, item_id, qty, created_at',
    [args.userId, args.itemId, args.qty],
  );
  return rows[0];
}

export async function listRecent(pool: Pool): Promise<Order[]> {
  const { rows } = await pool.query(
    'SELECT id, user_id, item_id, qty, created_at FROM orders ORDER BY id DESC LIMIT 100',
  );
  return rows;
}

// join モード専用: items テーブルと同じ DB に存在する前提で stock チェック + デクリメント +
// order 挿入をひとつのトランザクションで行う。
// join-mode only: assumes items is in the same DB; stock check + decrement + insert in one tx.
export async function createWithDirectStock(
  pool: Pool,
  args: { userId: number; itemId: number; qty: number },
): Promise<{ ok: boolean; order?: Order; stock?: number }> {
  const client = await pool.connect();
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
