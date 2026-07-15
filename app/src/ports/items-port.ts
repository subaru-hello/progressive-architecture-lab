// ItemsPort: orders が items ドメインにアクセスするための driven ポート。
// ItemsPort: driven port through which orders accesses the items domain.
import type { ItemsRepoPort } from '../domains/items/ports.js';
import type { Item } from '../domains/items/domain/item.js';

export type { Item };

export interface ItemsPort {
  getItem(id: number): Promise<Item | null>;
  decrementStock(id: number, qty: number): Promise<{ ok: boolean; stock: number }>;

  // Lv19 2PC: items-service の prepare/commit/rollback。
  // Lv19 2PC: items-service side prepare/commit/rollback (HTTP calls to /internal/tx/*).
  prepareTxDecrement(gid: string, itemId: number, qty: number): Promise<{ ok: boolean; stock: number }>;
  commitTx(gid: string): Promise<void>;
  rollbackTx(gid: string): Promise<void>;

  // Lv21 2PC リゾルバ: items-db の prepared gid 一覧。
  // Lv21 2PC resolver: list prepared gids on items-db (for coordinator crash recovery).
  listPreparedTx(): Promise<string[]>;

  // Lv19 saga: 冪等 reserve / release。
  // Lv19 saga: idempotent reserve / release (HTTP calls to /internal/reserve|release).
  reserveStock(gid: string, itemId: number, qty: number): Promise<{ ok: boolean; stock: number }>;
  releaseStock(gid: string): Promise<void>;

  // Lv22 outbox: 冪等 decrement 配送 (outbox poller から呼ばれる)。
  // Lv22 outbox: idempotent decrement delivery (called by the outbox poller).
  deliverDecrement(msgId: string, itemId: number, qty: number): Promise<{ applied: boolean; duplicate: boolean }>;
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

  // Lv19 2PC: インプロセスはリポジトリに直接委譲。
  // Lv19 2PC: in-process delegates directly to the repo.
  prepareTxDecrement(gid: string, itemId: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    return this.itemsRepo.prepareDecrement(gid, itemId, qty);
  }

  commitTx(gid: string): Promise<void> {
    return this.itemsRepo.commitPrepared(gid);
  }

  rollbackTx(gid: string): Promise<void> {
    return this.itemsRepo.rollbackPrepared(gid);
  }

  // Lv21 2PC リゾルバ: インプロセスはリポジトリに直接委譲。
  // Lv21 2PC resolver: in-process delegates directly to the repo.
  listPreparedTx(): Promise<string[]> {
    return this.itemsRepo.listPreparedGids();
  }

  // Lv19 saga: インプロセスはリポジトリに直接委譲。
  // Lv19 saga: in-process delegates directly to the repo.
  reserveStock(gid: string, itemId: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    return this.itemsRepo.reserve(gid, itemId, qty);
  }

  releaseStock(gid: string): Promise<void> {
    return this.itemsRepo.release(gid);
  }

  // Lv22 outbox: インプロセスはリポジトリに直接委譲。
  // Lv22 outbox: in-process delegates directly to the repo.
  deliverDecrement(msgId: string, itemId: number, qty: number): Promise<{ applied: boolean; duplicate: boolean }> {
    return this.itemsRepo.applyDecrementIdempotent(msgId, itemId, qty);
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

  // Lv19 2PC/saga: DualWriteAdapter はモードに応じてプライマリかセカンダリに委譲。
  // Lv19 2PC/saga: DualWriteAdapter delegates to primary or secondary based on mode.
  // secondary_only → secondary; others → primary (2pc/saga は移行後の secondary_only 想定)。
  // In practice these are called only after migration is complete (secondary_only mode).
  prepareTxDecrement(gid: string, itemId: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    return this.mode === 'secondary_only'
      ? this.secondary.prepareTxDecrement(gid, itemId, qty)
      : this.primary.prepareTxDecrement(gid, itemId, qty);
  }

  commitTx(gid: string): Promise<void> {
    return this.mode === 'secondary_only'
      ? this.secondary.commitTx(gid)
      : this.primary.commitTx(gid);
  }

  rollbackTx(gid: string): Promise<void> {
    return this.mode === 'secondary_only'
      ? this.secondary.rollbackTx(gid)
      : this.primary.rollbackTx(gid);
  }

  // Lv21 2PC リゾルバ: モードに応じてプライマリかセカンダリに委譲。
  // Lv21 2PC resolver: delegate to primary or secondary based on mode.
  listPreparedTx(): Promise<string[]> {
    return this.mode === 'secondary_only'
      ? this.secondary.listPreparedTx()
      : this.primary.listPreparedTx();
  }

  reserveStock(gid: string, itemId: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    return this.mode === 'secondary_only'
      ? this.secondary.reserveStock(gid, itemId, qty)
      : this.primary.reserveStock(gid, itemId, qty);
  }

  releaseStock(gid: string): Promise<void> {
    return this.mode === 'secondary_only'
      ? this.secondary.releaseStock(gid)
      : this.primary.releaseStock(gid);
  }

  // Lv22 outbox: モードに応じてプライマリかセカンダリに委譲。
  // Lv22 outbox: delegate to primary or secondary based on mode.
  deliverDecrement(msgId: string, itemId: number, qty: number): Promise<{ applied: boolean; duplicate: boolean }> {
    return this.mode === 'secondary_only'
      ? this.secondary.deliverDecrement(msgId, itemId, qty)
      : this.primary.deliverDecrement(msgId, itemId, qty);
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

  // Lv19 2PC: items-service の /internal/tx/prepare-decrement を呼ぶ。
  // Lv19 2PC: call items-service /internal/tx/prepare-decrement.
  async prepareTxDecrement(gid: string, itemId: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    const res = await fetch(`${this.baseUrl}/internal/tx/prepare-decrement`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ gid, itemId, qty }),
      signal: AbortSignal.timeout(5000),
    });
    if (res.status === 409) {
      const data = await res.json() as { stock: number };
      return { ok: false, stock: data.stock ?? 0 };
    }
    if (!res.ok) throw new Error(`items service prepareTxDecrement failed: ${res.status}`);
    const data = await res.json() as { ok: boolean; stock: number };
    return { ok: data.ok, stock: data.stock };
  }

  // Lv19 2PC: items-service の /internal/tx/:gid/commit を呼ぶ。
  // Lv19 2PC: call items-service /internal/tx/:gid/commit.
  async commitTx(gid: string): Promise<void> {
    // gid は path に載る。body は無いので Content-Type は付けない
    // (application/json + 空 body は Fastify が 400 で弾く)。
    // gid travels in the path; send NO body and NO content-type
    // (empty body + application/json makes Fastify reject with 400).
    const res = await fetch(`${this.baseUrl}/internal/tx/${encodeURIComponent(gid)}/commit`, {
      method: 'POST',
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) throw new Error(`items service commitTx failed: ${res.status}`);
  }

  // Lv19 2PC: items-service の /internal/tx/:gid/rollback を呼ぶ。
  // Lv19 2PC: call items-service /internal/tx/:gid/rollback.
  async rollbackTx(gid: string): Promise<void> {
    // body 無し → Content-Type を付けない (Fastify の空 body 400 回避)。
    // No body → omit Content-Type (avoids Fastify's empty-body 400).
    const res = await fetch(`${this.baseUrl}/internal/tx/${encodeURIComponent(gid)}/rollback`, {
      method: 'POST',
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) throw new Error(`items service rollbackTx failed: ${res.status}`);
  }

  // Lv21 2PC リゾルバ: items-service の /internal/tx/prepared を呼ぶ。
  // Lv21 2PC resolver: call items-service GET /internal/tx/prepared.
  async listPreparedTx(): Promise<string[]> {
    const res = await fetch(`${this.baseUrl}/internal/tx/prepared`, {
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) throw new Error(`items service listPreparedTx failed: ${res.status}`);
    const data = await res.json() as { gids: string[] };
    return data.gids;
  }

  // Lv19 saga: items-service の /internal/reserve を呼ぶ。
  // Lv19 saga: call items-service /internal/reserve.
  async reserveStock(gid: string, itemId: number, qty: number): Promise<{ ok: boolean; stock: number }> {
    const res = await fetch(`${this.baseUrl}/internal/reserve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ gid, itemId, qty }),
      signal: AbortSignal.timeout(5000),
    });
    if (res.status === 409) {
      const data = await res.json() as { stock: number };
      return { ok: false, stock: data.stock ?? 0 };
    }
    if (!res.ok) throw new Error(`items service reserveStock failed: ${res.status}`);
    const data = await res.json() as { ok: boolean; stock: number };
    return { ok: data.ok, stock: data.stock };
  }

  // Lv19 saga: items-service の /internal/release を呼ぶ。
  // Lv19 saga: call items-service /internal/release.
  async releaseStock(gid: string): Promise<void> {
    const res = await fetch(`${this.baseUrl}/internal/release`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ gid }),
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) throw new Error(`items service releaseStock failed: ${res.status}`);
  }

  // Lv22 outbox: items-service の /internal/outbox/apply を呼ぶ (冪等 decrement 配送)。
  // Lv22 outbox: call items-service /internal/outbox/apply (idempotent decrement delivery).
  async deliverDecrement(msgId: string, itemId: number, qty: number): Promise<{ applied: boolean; duplicate: boolean }> {
    const res = await fetch(`${this.baseUrl}/internal/outbox/apply`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ msgId, itemId, qty }),
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) throw new Error(`items service deliverDecrement failed: ${res.status}`);
    const data = await res.json() as { applied: boolean; duplicate: boolean };
    return { applied: data.applied, duplicate: data.duplicate };
  }
}
