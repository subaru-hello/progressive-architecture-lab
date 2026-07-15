// orders ドメインの純粋型と純粋ルール。I/O 依存なし。
// Pure Order type + domain rules. Zero I/O imports.

export interface Order {
  id: number;
  user_id: number;
  item_id: number;
  qty: number;
  created_at: string;
  // Lv23 choreography saga: 注文ステータス。既存 SQL は DEFAULT 'confirmed' に落ちるため後方互換。
  // Lv23 choreography saga: order status. Existing SQL omits it and gets DEFAULT 'confirmed'.
  status?: string;
}

// qty が正の整数であることを保証する。ドメイン不変条件（純関数）。
// Assert qty is a positive integer. A pure domain invariant.
export function assertPositiveQuantity(qty: number): void {
  if (!Number.isInteger(qty) || qty <= 0) {
    throw new RangeError(`qty must be a positive integer, got ${qty}`);
  }
}

// 注記: 「在庫充足」は純ドメイン述語(canFulfil)にしない。check-then-act の race になるため。
// 在庫の不変条件は infra 側の atomic guard(`UPDATE ... WHERE stock >= qty`)が担う。
// = 並行性を持つ不変条件は純ドメインでなく DB に置くのが正しい（hexagonal の勘所）。
// NOTE: stock-sufficiency is deliberately NOT a pure predicate here — a pure canFulfil()
// would be a check-then-act race. That invariant lives in the infra atomic guard
// (`UPDATE ... WHERE stock >= qty`). Concurrency invariants belong at the DB, not the pure domain.
