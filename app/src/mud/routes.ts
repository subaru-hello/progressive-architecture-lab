// mud/routes.ts — ARCH=mud 専用のルート定義。
// mud/routes.ts — Route handlers for ARCH=mud.
//
// ⚠️ 意図的なビッグボールオブマッド実装 ⚠️
// ⚠️ Intentional big-ball-of-mud implementation ⚠️
//
// ドメイン境界なし・ポートなし・リポジトリなし・ユースケース分離なし。
// No domain boundaries, no ports, no repos, no use-case separation.
// ハンドラの中に生 SQL を直書きし、3 ドメインを跨ぐトランザクションを 1 関数に押し込む。
// Raw SQL inline in handlers; cross-domain 3-table transaction collapsed into one handler.
// これはアンチパターンの見本 — ヘックス版との対比が目的。
// This is an anti-pattern showcase — exists to contrast against the hex version.

import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';

export interface MudPluginOptions {
  writePool: Pool;
  INSTANCE: string;
}

export async function mudRoutes(app: FastifyInstance, opts: MudPluginOptions): Promise<void> {
  const { writePool, INSTANCE } = opts;

  // GET /items — items 一覧 (stock は含めない、Lv0-12 後方互換)。
  // GET /items — list items; stock excluded for Lv0-12 backward-compat.
  app.get('/items', async () => {
    // 生 SQL 直打ち — repo も port も経由しない。
    // Raw SQL inline — no repo, no port.
    const { rows } = await writePool.query(
      'SELECT id, name, created_at FROM items ORDER BY id DESC LIMIT 100',
    );
    return { instance: INSTANCE, items: rows };
  });

  // GET /items/:id — 個別 item 取得 (stock 含む)。
  // GET /items/:id — single item fetch (stock included).
  app.get('/items/:id', async (req, reply) => {
    const { id } = req.params as { id: string };
    // 生 SQL 直打ち。
    // Raw SQL inline.
    const { rows } = await writePool.query(
      'SELECT id, name, stock, created_at FROM items WHERE id = $1',
      [Number(id)],
    );
    if (rows.length === 0) {
      reply.code(404);
      return { error: 'not found' };
    }
    return { instance: INSTANCE, item: rows[0] };
  });

  // POST /items — item 作成 (Redis/キャッシュなし — mud に cache seam はない)。
  // POST /items — create item (no Redis/cache — mud has no cache seam).
  app.post('/items', async (req, reply) => {
    const body = req.body as { name?: string };
    if (!body?.name) {
      reply.code(400);
      return { error: 'name is required' };
    }
    // 生 SQL INSERT — ドメイン関数を呼ばず直接 writePool を叩く。
    // Raw INSERT directly against writePool — no domain function call.
    const { rows } = await writePool.query(
      'INSERT INTO items(name) VALUES($1) RETURNING id, name, created_at',
      [body.name],
    );
    reply.code(201);
    return { instance: INSTANCE, item: rows[0] };
  });

  // GET /orders — クロスドメイン JOIN で orders を返す。
  // GET /orders — returns orders enriched via cross-domain JOIN.
  //
  // 意図的結合: orders・items・users を JOIN してリッチなレスポンスを一発で作る。
  // Intentional coupling: single JOIN spanning orders, items, users tables.
  app.get('/orders', async () => {
    const { rows } = await writePool.query(
      `SELECT o.id, o.qty, o.created_at,
              i.name AS item_name,
              u.name AS user_name
         FROM orders o
         JOIN items  i ON i.id = o.item_id
         JOIN users  u ON u.id = o.user_id
        ORDER BY o.id DESC LIMIT 100`,
    );
    return { instance: INSTANCE, orders: rows };
  });

  // POST /orders — 3 ドメインテーブルを跨ぐ 1 トランザクション。
  // POST /orders — one transaction spanning 3 domain tables.
  //
  // 意図的結合の核心: auth・在庫・注文・ユーザー更新が全部ここに詰まっている。
  // Core of the coupling: auth, stock, order insert, user update — all inline, one tx.
  app.post('/orders', async (req, reply) => {
    const token = req.headers['x-auth-token'] as string | undefined;
    const body = req.body as { itemId?: unknown; qty?: unknown };

    // 認証ヘッダを先にチェック（hex と同順: token→401 が body→400 より先）。
    // Check the auth header first (same order as hex: token→401 precedes body→400).
    if (!token) {
      reply.code(401);
      return { error: 'x-auth-token header required' };
    }

    // 入力バリデーション — 正の整数チェック。
    // Input validation — require positive integers.
    const itemId = Number(body?.itemId);
    const qty = Number(body?.qty);
    if (!Number.isInteger(itemId) || itemId <= 0 || !Number.isInteger(qty) || qty <= 0) {
      reply.code(400);
      return { error: 'itemId and qty must be positive integers' };
    }

    // クライアントをチェックアウトして BEGIN — 3 ドメインにまたがるトランザクション開始。
    // Checkout a client and BEGIN — start the cross-domain transaction.
    const client = await writePool.connect();
    try {
      await client.query('BEGIN');

      // Step 1: インライン認証 — users テーブルを直接参照 (port/adapter なし)。
      // Step 1: inline auth — raw query against users table (no port/adapter).
      const authRes = await client.query(
        'SELECT id FROM users WHERE token = $1',
        [token],
      );
      if (authRes.rows.length === 0) {
        await client.query('ROLLBACK');
        reply.code(401);
        return { error: 'invalid token' };
      }
      const userId: number = authRes.rows[0].id;

      // Step 2: 在庫デクリメント — items テーブルを直接 UPDATE (在庫不足で 409)。
      // Step 2: stock decrement — direct UPDATE on items table (409 on insufficient stock).
      const stockRes = await client.query(
        'UPDATE items SET stock = stock - $2 WHERE id = $1 AND stock >= $2 RETURNING stock',
        [itemId, qty],
      );
      if (stockRes.rows.length === 0) {
        await client.query('ROLLBACK');
        reply.code(409);
        return { error: 'insufficient stock' };
      }

      // Step 3: orders INSERT — 同一 TX 内。
      // Step 3: insert order — within the same tx.
      const orderRes = await client.query(
        'INSERT INTO orders(user_id, item_id, qty) VALUES($1, $2, $3) RETURNING id, user_id, item_id, qty, created_at',
        [userId, itemId, qty],
      );

      // Step 4: users.last_order_at 更新 — ドメイン横断書き込み、同一 TX 内。
      // Step 4: update users.last_order_at — cross-domain write, same tx.
      await client.query(
        'UPDATE users SET last_order_at = now() WHERE id = $1',
        [userId],
      );

      await client.query('COMMIT');
      reply.code(201);
      return { instance: INSTANCE, order: orderRes.rows[0] };
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  });
}
