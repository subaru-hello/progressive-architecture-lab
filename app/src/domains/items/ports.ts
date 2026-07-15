// items ドメインが所有する driven ポート定義。
// Driven ports owned by the items domain.
import type { Item } from './domain/item.js';

export interface ItemsRepoPort {
  getById(id: number): Promise<Item | null>;
  create(name: string): Promise<Pick<Item, 'id' | 'name' | 'created_at'>>;
  decrementStock(id: number, qty: number): Promise<{ ok: boolean; stock: number }>;
  listLatest100(): Promise<Pick<Item, 'id' | 'name' | 'created_at'>[]>;
  // Lv18: バックフィル用 — 全件取得 / 冪等 upsert。
  // Lv18: backfill helpers — fetch all rows / idempotent bulk upsert.
  listAll(): Promise<Item[]>;
  bulkUpsert(rows: Pick<Item, 'id' | 'name' | 'stock' | 'created_at'>[]): Promise<number>;
  initSchema(): Promise<void>;
  seed(): Promise<void>;

  // Lv19 2PC: items-db 側の prepare/commit/rollback。
  // Lv19 2PC: items-db side prepare/commit/rollback via PREPARE TRANSACTION.
  prepareDecrement(gid: string, id: number, qty: number): Promise<{ ok: boolean; stock: number }>;
  commitPrepared(gid: string): Promise<void>;
  rollbackPrepared(gid: string): Promise<void>;

  // Lv21 2PC リゾルバ: items-db の prepared transaction 一覧 (gid LIKE 'ord-%')。
  // Lv21 2PC resolver: list prepared transactions on items-db (gid LIKE 'ord-%').
  listPreparedGids(): Promise<string[]>;

  // Lv19 saga: 冪等 reserve / release。
  // Lv19 saga: idempotent reserve / release using reservations table.
  reserve(gid: string, id: number, qty: number): Promise<{ ok: boolean; stock: number }>;
  release(gid: string): Promise<void>;

  // Lv19 スキーマ拡張フラグ: 2pc/saga 用テーブルが存在する場合のみ true。
  // Lv19 schema extension flag: create reservations table when saga mode is active.
  initSagaSchema(): Promise<void>;

  // Lv22 outbox: processed_messages テーブルを作成 (冪等 receiver 用)。
  // Lv22 outbox: create processed_messages table (idempotent receiver inbox).
  initInboxSchema(): Promise<void>;

  // Lv22 outbox: 冪等 decrement。ON CONFLICT DO NOTHING で重複 msg_id を検出。
  // Lv22 outbox: idempotent decrement — duplicate msg_id detected via ON CONFLICT DO NOTHING.
  applyDecrementIdempotent(msgId: string, itemId: number, qty: number): Promise<{ applied: boolean; duplicate: boolean }>;

  // Lv23 choreography saga: choreo_outbox テーブルを items-db に作成。
  // Lv23 choreography: create choreo_outbox table on items-db.
  initChoreoSchema(): Promise<void>;

  // Lv23 choreography saga: OrderCreated イベントを冪等に処理。
  // 在庫予約成功なら StockReserved、失敗なら StockRejected を choreo_outbox に書く。
  // Lv23 choreography: idempotent OrderCreated handler.
  // On success: emit StockReserved; on failure: emit StockRejected — both in a single tx.
  handleOrderCreatedIdempotent(
    msgId: string,
    orderId: number,
    itemId: number,
    qty: number,
  ): Promise<{ result: 'reserved' | 'rejected' }>;

  // Lv23 choreography saga: 未配送の items choreo_outbox 行を取得。
  // Lv23 choreography: fetch undelivered items choreo_outbox rows.
  claimUndeliveredChoreoOutbox(limit: number): Promise<{ msg_id: string; event_type: string; payload: unknown }[]>;

  // Lv23 choreography saga: items choreo_outbox 行を配送済みにマーク。
  // Lv23 choreography: mark items choreo_outbox row as delivered.
  markChoreoOutboxDelivered(msgId: string): Promise<void>;
}
