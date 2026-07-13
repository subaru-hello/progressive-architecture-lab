// ItemsPort: orders が items ドメインにアクセスするための driven ポート。
// ItemsPort: driven port through which orders accesses the items domain.
import type { ItemsRepoPort } from '../domains/items/ports.js';
import type { Item } from '../domains/items/domain/item.js';

export type { Item };

export interface ItemsPort {
  getItem(id: number): Promise<Item | null>;
  decrementStock(id: number, qty: number): Promise<{ ok: boolean; stock: number }>;
}

// インプロセスアダプタ: 同一プロセス内の items リポジトリを直接呼ぶ。
// In-process adapter: delegates to the items domain repo (monolith/modular-monolith).
export class InProcessItemsAdapter implements ItemsPort {
  constructor(private readonly itemsRepo: ItemsRepoPort) {}

  getItem(id: number): Promise<Item | null> {
    return this.itemsRepo.getById(id);
  }

  decrementStock(id: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    return this.itemsRepo.decrementStock(id, qty);
  }
}

// HTTP アダプタ: 別プロセスの items サービスに HTTP 経由でアクセス（マイクロサービス）。
// HTTP adapter: reaches items service over HTTP (microservice deployment).
export class HttpItemsAdapter implements ItemsPort {
  constructor(private readonly baseUrl: string) {}

  async getItem(id: number): Promise<Item | null> {
    const res = await fetch(`${this.baseUrl}/internal/items/${id}`, {
      signal: AbortSignal.timeout(2000),
    });
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`items service getItem failed: ${res.status}`);
    const data = await res.json() as { item: Item };
    return data.item;
  }

  async decrementStock(id: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    const res = await fetch(`${this.baseUrl}/internal/items/${id}/decrement`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ qty }),
      signal: AbortSignal.timeout(2000),
    });
    // 409 は在庫不足（ok:false）— 例外ではなく値として返す。
    // 409 means insufficient stock (ok:false) — return as value, not throw.
    if (res.status === 409) {
      const data = await res.json() as { stock: number };
      return { ok: false, stock: data.stock ?? 0 };
    }
    if (!res.ok) throw new Error(`items service decrementStock failed: ${res.status}`);
    const data = await res.json() as { ok: boolean; stock: number };
    return { ok: data.ok, stock: data.stock };
  }
}
