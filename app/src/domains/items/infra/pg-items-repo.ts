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

  // Lv19 2PC: BEGIN → UPDATE → PREPARE TRANSACTION '<gid>' の順で実行。
  // クライアントを PREPARE 後に release — prepared tx は pool 外で生き続ける。
  // Lv19 2PC: BEGIN → UPDATE → PREPARE TRANSACTION '<gid>'; release client after PREPARE.
  // The prepared tx is dissociated from the connection and persists in items-db.
  async prepareDecrement(gid: string, id: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    const c = await this.writePool.connect();
    try {
      await c.query('BEGIN');
      const { rows } = await c.query(
        'UPDATE items SET stock = stock - $2 WHERE id = $1 AND stock >= $2 RETURNING stock',
        [id, qty],
      );
      if (rows.length === 0) {
        await c.query('ROLLBACK');
        return { ok: false, stock: 0 };
      }
      // gid をシングルクォートエスケープして安全に埋め込む (gid は crypto.randomUUID() 由来)。
      // Escape gid single-quotes before embedding — gid comes from crypto.randomUUID() so
      // no injection risk, but defensive programming is warranted for prepared-tx names.
      await c.query(`PREPARE TRANSACTION '${gid.replace(/'/g, "''")}'`);
      return { ok: true, stock: rows[0].stock };
    } catch (err) {
      try { await c.query('ROLLBACK'); } catch { /* ignore cleanup error */ }
      throw err;
    } finally {
      // PREPARE 後は tx が切り離されるのでクライアントを解放しても tx は保持される。
      // After PREPARE the tx is dissociated — releasing the client does NOT lose the tx.
      c.release();
    }
  }

  // Lv19 2PC: COMMIT PREPARED — 任意のプール接続で実行できる。
  // "prepared transaction not found" は既に解決済みとして扱い、静かに無視する (冪等)。
  // Lv19 2PC: COMMIT PREPARED on any pooled connection. Idempotent — ignore "not found".
  async commitPrepared(gid: string): Promise<void> {
    try {
      await this.writePool.query(`COMMIT PREPARED '${gid.replace(/'/g, "''")}'`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (/prepared transaction with identifier.*does not exist/i.test(msg)) return; // already resolved
      throw err;
    }
  }

  // Lv19 2PC: ROLLBACK PREPARED — 冪等。
  // Lv19 2PC: ROLLBACK PREPARED — idempotent.
  async rollbackPrepared(gid: string): Promise<void> {
    try {
      await this.writePool.query(`ROLLBACK PREPARED '${gid.replace(/'/g, "''")}'`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (/prepared transaction with identifier.*does not exist/i.test(msg)) return; // already resolved
      throw err;
    }
  }

  // Lv21 2PC リゾルバ: items-db の prepared transaction 一覧 (gid LIKE 'ord-%')。
  // Lv21 2PC resolver: list prepared transactions on items-db (gid LIKE 'ord-%').
  async listPreparedGids(): Promise<string[]> {
    const { rows } = await this.writePool.query<{ gid: string }>(
      `SELECT gid FROM pg_prepared_xacts WHERE gid LIKE 'ord-%'`,
    );
    return rows.map((r) => r.gid);
  }

  // Lv19 saga: reservations テーブルを利用した冪等 reserve。
  // gid が既に reservations に存在 → 以前の結果を返す (idempotent replay)。
  // なければ stock -= qty を試み、成功なら reservations に INSERT。
  // Lv19 saga: idempotent reserve using reservations table.
  async reserve(gid: string, id: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    const c = await this.writePool.connect();
    try {
      await c.query('BEGIN');
      // 冪等チェック: gid が既に存在すれば以前の結果を返す。
      // Idempotency check: if gid already exists return the prior outcome.
      const existing = await c.query(
        'SELECT qty, released FROM reservations WHERE gid = $1',
        [gid],
      );
      if (existing.rows.length > 0) {
        await c.query('COMMIT');
        // released は補償済みを意味するが reserve として ok:true で返す (saga が判断)。
        // released means compensated but we return ok:true — the saga coordinator decides.
        const { rows: stockRows } = await c.query(
          'SELECT stock FROM items WHERE id = $1', [id],
        );
        return { ok: true, stock: stockRows[0]?.stock ?? 0 };
      }
      const { rows } = await c.query(
        'UPDATE items SET stock = stock - $2 WHERE id = $1 AND stock >= $2 RETURNING stock',
        [id, qty],
      );
      if (rows.length === 0) {
        await c.query('ROLLBACK');
        return { ok: false, stock: 0 };
      }
      await c.query(
        'INSERT INTO reservations(gid, item_id, qty) VALUES($1, $2, $3)',
        [gid, id, qty],
      );
      await c.query('COMMIT');
      return { ok: true, stock: rows[0].stock };
    } catch (err) {
      try { await c.query('ROLLBACK'); } catch { /* ignore */ }
      throw err;
    } finally {
      c.release();
    }
  }

  // Lv19 saga: stock を戻し released=true にマーク。冪等 (released 済みなら何もしない)。
  // Lv19 saga: return stock and mark released. Idempotent — no-op if already released.
  async release(gid: string): Promise<void> {
    const c = await this.writePool.connect();
    try {
      await c.query('BEGIN');
      const { rows } = await c.query(
        'SELECT item_id, qty, released FROM reservations WHERE gid = $1 FOR UPDATE',
        [gid],
      );
      if (rows.length === 0 || rows[0].released) {
        // 既に補償済み、または存在しない → 冪等として成功扱い。
        // Already released or no reservation found — idempotent no-op.
        await c.query('COMMIT');
        return;
      }
      await c.query(
        'UPDATE items SET stock = stock + $2 WHERE id = $1',
        [rows[0].item_id, rows[0].qty],
      );
      await c.query(
        'UPDATE reservations SET released = true WHERE gid = $1',
        [gid],
      );
      await c.query('COMMIT');
    } catch (err) {
      try { await c.query('ROLLBACK'); } catch { /* ignore */ }
      throw err;
    } finally {
      c.release();
    }
  }

  // Lv19 saga: reservations テーブルを作成 (saga モードがアクティブな場合のみ呼ばれる)。
  // Lv19 saga: create the reservations table (called only when saga mode is active).
  async initSagaSchema(): Promise<void> {
    await this.writePool.query(`
      CREATE TABLE IF NOT EXISTS reservations (
        gid TEXT PRIMARY KEY,
        item_id INT NOT NULL,
        qty INT NOT NULL,
        released BOOL NOT NULL DEFAULT false,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
  }

  // Lv22 outbox: processed_messages テーブルを作成 (冪等 receiver 用)。
  // Lv22 outbox: create processed_messages table (idempotent receiver inbox).
  async initInboxSchema(): Promise<void> {
    await this.writePool.query(`
      CREATE TABLE IF NOT EXISTS processed_messages (
        msg_id TEXT PRIMARY KEY,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
  }

  // Lv22 outbox: 冪等 decrement。ON CONFLICT DO NOTHING で重複 msg_id を検出。
  // 単一 tx: INSERT processed_messages → rowCount === 0 なら既処理 → 何もしない。
  // Lv22 outbox: idempotent decrement in a single tx.
  // INSERT processed_messages; rowCount=0 → duplicate → no-op; rowCount=1 → apply decrement.
  async applyDecrementIdempotent(
    msgId: string,
    itemId: number,
    qty: number,
  ): Promise<{ applied: boolean; duplicate: boolean }> {
    const c = await this.writePool.connect();
    try {
      await c.query('BEGIN');
      const ins = await c.query(
        `INSERT INTO processed_messages(msg_id) VALUES($1) ON CONFLICT DO NOTHING`,
        [msgId],
      );
      if (ins.rowCount === 0) {
        // 既処理 — 重複配送。何もしない。
        // Already processed — duplicate delivery; no-op.
        await c.query('COMMIT');
        return { applied: false, duplicate: true };
      }
      await c.query(
        'UPDATE items SET stock = stock - $2 WHERE id = $1',
        [itemId, qty],
      );
      await c.query('COMMIT');
      return { applied: true, duplicate: false };
    } catch (err) {
      try { await c.query('ROLLBACK'); } catch { /* ignore cleanup error */ }
      throw err;
    } finally {
      c.release();
    }
  }
}
