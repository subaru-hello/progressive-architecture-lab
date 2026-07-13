// items ドメインが所有する driven ポート定義。
// Driven ports owned by the items domain.
import type { Item } from './domain/item.js';

export interface ItemsRepoPort {
  getById(id: number): Promise<Item | null>;
  create(name: string): Promise<Pick<Item, 'id' | 'name' | 'created_at'>>;
  decrementStock(id: number, qty: number): Promise<{ ok: boolean; stock: number }>;
  listLatest100(): Promise<Pick<Item, 'id' | 'name' | 'created_at'>[]>;
  initSchema(): Promise<void>;
  seed(): Promise<void>;
}
