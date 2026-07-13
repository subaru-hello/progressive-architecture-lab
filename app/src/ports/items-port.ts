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

// Lv18 移行モード型: ランタイムに切り替え可能 — コンテナ再起動不要。
// Lv18 migration mode: switchable at runtime without restarting the container.
//   primary_only  — read+write primary only (baseline; default).
//   dual_write    — write both (primary authoritative); read primary.
//   dual_shadow   — dual_write + shadow-read secondary on getItem → compare stock/null.
//   secondary_only— read+write go to secondary only (post-cutover: items-service is source of truth).
export type MigrationMode = 'primary_only' | 'dual_write' | 'dual_shadow' | 'secondary_only';
export const MIGRATION_MODES: readonly MigrationMode[] = ['primary_only', 'dual_write', 'dual_shadow', 'secondary_only'];

// デュアルライトアダプタ: ランタイムで切り替えられる移行モードを持つ。
// Dual-write adapter: holds a mutable migration mode switchable at runtime (zero restart).
// Lv18 ストラングラーフィグ移行フェーズで使用 — ITEMS_SERVICE_URL が設定されたときのみ有効化。
// Used during Lv18 strangler-fig extraction — only active when ITEMS_SERVICE_URL is set.
export class DualWriteItemsAdapter implements ItemsPort {
  private mode: MigrationMode;

  constructor(
    private readonly primary: ItemsPort,
    private readonly secondary: ItemsPort,
    private readonly opts: {
      initialMode: MigrationMode;
      onMismatch: () => void;
      onSecondaryError: () => void;
    },
  ) {
    this.mode = opts.initialMode;
  }

  /** ランタイムにモードを切り替える（コンテナ再起動不要）。 */
  /** Switch migration mode at runtime — no container restart needed. */
  setMode(mode: MigrationMode): void {
    this.mode = mode;
  }

  getMode(): MigrationMode {
    return this.mode;
  }

  async getItem(id: number): Promise<Item | null> {
    // モードを呼び出し開始時に一度だけ読む — await 後の setMode 呼び出しで分岐が変わらないよう。
    // Snapshot mode once at call entry so a concurrent setMode cannot tear this call's behavior.
    const mode = this.mode;

    if (mode === 'secondary_only') {
      // 切り替え完了: セカンダリが権威。エラーは呼び出し元に伝播。
      // Post-cutover: secondary is authoritative; propagate errors.
      return this.secondary.getItem(id);
    }

    if (mode === 'primary_only') {
      return this.primary.getItem(id);
    }

    // dual_write / dual_shadow: プライマリが権威。
    // dual_write / dual_shadow: primary is authoritative.
    const primaryResult = await this.primary.getItem(id);

    if (mode === 'dual_shadow') {
      // セカンダリのシャドウ読み取り — 失敗は握る。
      // Shadow-read secondary; swallow any error (non-authoritative).
      try {
        const secondaryResult = await this.secondary.getItem(id);
        const mismatch =
          (primaryResult === null) !== (secondaryResult === null) ||
          (primaryResult !== null && secondaryResult !== null && primaryResult.stock !== secondaryResult.stock);
        if (mismatch) this.opts.onMismatch();
      } catch {
        // shadow read failure is non-fatal
      }
    }

    return primaryResult;
  }

  async decrementStock(id: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    // モードを呼び出し開始時に一度だけ読む — await 後の setMode 呼び出しで分岐が変わらないよう。
    // Snapshot mode once at call entry so a concurrent setMode cannot tear this call's behavior.
    const mode = this.mode;

    if (mode === 'secondary_only') {
      // 切り替え完了: セカンダリが権威。エラーは呼び出し元に伝播。
      // Post-cutover: secondary is authoritative; propagate errors.
      return this.secondary.decrementStock(id, qty);
    }

    if (mode === 'primary_only') {
      return this.primary.decrementStock(id, qty);
    }

    // dual_write / dual_shadow: プライマリへ先に書き込む — その結果を返す。
    // dual_write / dual_shadow: write primary first — its result is what caller receives.
    const result = await this.primary.decrementStock(id, qty);

    // セカンダリへベストエフォートでミラー — 失敗してもリクエストは失敗させない。
    // Mirror to secondary best-effort; never fail the caller on secondary error.
    try {
      const sec = await this.secondary.decrementStock(id, qty);
      if (!sec.ok) this.opts.onSecondaryError();
    } catch {
      this.opts.onSecondaryError();
    }

    return result;
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
