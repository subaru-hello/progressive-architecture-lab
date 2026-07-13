// PgOrdersRepo: OrdersRepoPort の Postgres 実装。SQL は repo.ts から移植。
// PgOrdersRepo: Postgres implementation of OrdersRepoPort. SQL moved verbatim from repo.ts.
import type { Pool } from 'pg';
import type { OrdersRepoPort, SagaState, SagaLogRow } from '../ports.js';
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

  // Lv19 saga スキーマ: saga_log テーブル (orders-db 側)。
  // Lv19 saga schema: saga_log table on orders-db side.
  async initSagaSchema(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS saga_log (
        gid TEXT PRIMARY KEY,
        user_id INT NOT NULL,
        item_id INT NOT NULL,
        qty INT NOT NULL,
        state TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
  }

  // Lv19 2PC: BEGIN → INSERT → PREPARE TRANSACTION '<gid>'。
  // クライアントを PREPARE 後に release — prepared tx は orders-db 内に保持される。
  // Lv19 2PC: BEGIN → INSERT → PREPARE TRANSACTION; release client after PREPARE.
  async prepareInsert(gid: string, args: { userId: number; itemId: number; qty: number }): Promise<Order> {
    const c = await this.pool.connect();
    try {
      await c.query('BEGIN');
      const { rows } = await c.query(
        'INSERT INTO orders(user_id, item_id, qty) VALUES($1, $2, $3) RETURNING id, user_id, item_id, qty, created_at',
        [args.userId, args.itemId, args.qty],
      );
      await c.query(`PREPARE TRANSACTION '${gid.replace(/'/g, "''")}'`);
      return rows[0];
    } catch (err) {
      try { await c.query('ROLLBACK'); } catch { /* ignore */ }
      throw err;
    } finally {
      // PREPARE 後は tx が切り離される — release してもデータは失われない。
      // After PREPARE the tx is dissociated — releasing the client does NOT lose the row.
      c.release();
    }
  }

  // Lv19 2PC: COMMIT PREPARED — 冪等 (prepared tx not found は無視)。
  // Lv19 2PC: COMMIT PREPARED — idempotent.
  async commitPrepared(gid: string): Promise<void> {
    try {
      await this.pool.query(`COMMIT PREPARED '${gid.replace(/'/g, "''")}'`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (/prepared transaction with identifier.*does not exist/i.test(msg)) return;
      throw err;
    }
  }

  // Lv19 2PC: ROLLBACK PREPARED — 冪等。
  // Lv19 2PC: ROLLBACK PREPARED — idempotent.
  async rollbackPrepared(gid: string): Promise<void> {
    try {
      await this.pool.query(`ROLLBACK PREPARED '${gid.replace(/'/g, "''")}'`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (/prepared transaction with identifier.*does not exist/i.test(msg)) return;
      throw err;
    }
  }

  // Lv19 saga: saga_log に初期行を挿入 (state='reserved')。
  // Lv19 saga: insert initial saga_log row with state='reserved'.
  async insertSagaLog(args: { gid: string; userId: number; itemId: number; qty: number }): Promise<void> {
    await this.pool.query(
      `INSERT INTO saga_log(gid, user_id, item_id, qty, state) VALUES($1, $2, $3, $4, 'reserved')`,
      [args.gid, args.userId, args.itemId, args.qty],
    );
  }

  // Lv19 saga: saga_log の state を更新し updated_at をリフレッシュ。
  // Lv19 saga: update saga_log state and refresh updated_at.
  async updateSagaLogState(gid: string, state: SagaState): Promise<void> {
    await this.pool.query(
      `UPDATE saga_log SET state = $2, updated_at = now() WHERE gid = $1`,
      [gid, state],
    );
  }

  // Lv19 saga 回復ポーラー: state='reserved' かつ threshold 秒以上前の行を
  // アトミックに 'compensating' に遷移させて返す (re-entrant safe)。
  // Lv19 saga recovery: atomically move stale 'reserved' rows to 'compensating' and return them.
  async claimStuckSagaLogs(thresholdSeconds: number): Promise<SagaLogRow[]> {
    const { rows } = await this.pool.query<SagaLogRow>(
      `UPDATE saga_log
       SET state = 'compensating', updated_at = now()
       WHERE state = 'reserved'
         AND updated_at < now() - ($1 || ' seconds')::interval
       RETURNING gid, user_id, item_id, qty, state, created_at, updated_at`,
      [String(thresholdSeconds)],
    );
    return rows;
  }
}
