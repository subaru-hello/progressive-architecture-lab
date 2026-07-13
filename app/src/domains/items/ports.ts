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

  // Lv19 saga: 冪等 reserve / release。
  // Lv19 saga: idempotent reserve / release using reservations table.
  reserve(gid: string, id: number, qty: number): Promise<{ ok: boolean; stock: number }>;
  release(gid: string): Promise<void>;

  // Lv19 スキーマ拡張フラグ: 2pc/saga 用テーブルが存在する場合のみ true。
  // Lv19 schema extension flag: create reservations table when saga mode is active.
  initSagaSchema(): Promise<void>;
}
