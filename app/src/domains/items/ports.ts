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
}
