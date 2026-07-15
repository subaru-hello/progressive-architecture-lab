// createOrder ユースケース: auth → stock check → insert のオーケストレーション。
// CreateOrder usecase: auth → stock check → insert orchestration.
import { randomUUID } from 'node:crypto';
import type { UsersPort } from '../../../ports/users-port.js';
import type { ItemsPort } from '../../../ports/items-port.js';
import type { OrdersRepoPort } from '../ports.js';
import { assertPositiveQuantity, type Order } from '../domain/order.js';

export class AuthError extends Error {
  readonly code = 'AUTH_ERROR';
  constructor() { super('unauthorized'); }
}

export class StockError extends Error {
  readonly code = 'STOCK_ERROR';
  readonly stock: number;
  constructor(stock: number) {
    super('insufficient stock');
    this.stock = stock;
  }
}

export interface CreateOrderCmd {
  token: string;
  itemId: number;
  qty: number;
}

// Lv22: ORDER_TX_MODE の 4 値。
// Lv22: four ORDER_TX_MODE values for cross-DB coordination strategy.
export type TxMode = 'none' | '2pc' | 'saga' | 'outbox';
export const TX_MODES: readonly TxMode[] = ['none', '2pc', 'saga', 'outbox'];

// フォルトインジェクション設定。
// Fault injection configuration.
export interface FaultOpts {
  faultPoint: string; // '' = disabled
}

export interface CreateOrderDeps {
  usersPort: UsersPort;
  itemsPort: ItemsPort;
  ordersRepo: OrdersRepoPort;
  // 'join': orders と items が同一 DB → 直接 SQL でトランザクション。
  // 'port': 別 DB の可能性あり → ItemsPort 経由。
  // 'join': orders and items share a DB — use direct SQL transaction.
  // 'port': possibly separate DBs — go through ItemsPort.
  mode: 'join' | 'port';
  // Lv19: port モードのみ有効。join モードでは無視。
  // Lv19: only active when mode='port'; ignored for mode='join'.
  txMode?: TxMode;
  fault?: FaultOpts;
}

export async function createOrder(cmd: CreateOrderCmd, deps: CreateOrderDeps): Promise<Order> {
  const { token, itemId, qty } = cmd;
  const { usersPort, itemsPort, ordersRepo, mode, txMode = 'none', fault } = deps;

  // (0) ドメイン不変条件: qty は正の整数（routes でも検証済だが usecase 単体でも守る=defense in depth）。
  // (0) Domain invariant: qty must be a positive integer (routes validate too; usecase self-guards).
  assertPositiveQuantity(qty);

  // (1) 認証: token が無効なら AuthError を throw → routes で 401。
  // (1) Auth: throw AuthError on invalid token → routes catches → 401.
  const user = await usersPort.authenticate(token);
  if (!user) throw new AuthError();

  if (mode === 'join') {
    // (2a) 同一 DB トランザクション: items 直接 UPDATE + orders INSERT。
    // (2a) Same-DB transaction: UPDATE items + INSERT orders atomically.
    const result = await ordersRepo.createWithDirectStock({ userId: user.id, itemId, qty });
    if (!result.ok || !result.order) throw new StockError(result.stock ?? 0);
    return result.order;
  }

  // (2b) port モード: txMode に応じて 3 種の調整戦略を使い分ける。
  // (2b) port mode: dispatch to one of three cross-DB coordination strategies.
  switch (txMode) {
    case 'none':
      return createOrderNone(cmd, { itemsPort, ordersRepo, userId: user.id, fault });

    case '2pc':
      return createOrder2pc(cmd, { itemsPort, ordersRepo, userId: user.id, fault });

    case 'saga':
      return createOrderSaga(cmd, { itemsPort, ordersRepo, userId: user.id, fault });

    case 'outbox':
      return createOrderOutbox(cmd, { ordersRepo, userId: user.id });
  }
}

// ── none ──────────────────────────────────────────────────────────────────────
// ベースライン: decrement → insert の逐次呼び出し。フォルト時に stock が漏れる (demo用)。
// Baseline: sequential decrement → insert. Fault leaves stock decremented with no order (leak demo).
async function createOrderNone(
  cmd: CreateOrderCmd,
  deps: { itemsPort: ItemsPort; ordersRepo: OrdersRepoPort; userId: number; fault?: FaultOpts },
): Promise<Order> {
  const { itemId, qty } = cmd;
  const { itemsPort, ordersRepo, userId, fault } = deps;

  const dec = await itemsPort.decrementStock(itemId, qty);
  if (!dec.ok) throw new StockError(dec.stock);

  // FAULT POINT: after-first-write — stock decremented, no order inserted → leak.
  if (fault?.faultPoint === 'after-first-write') {
    throw new Error('[fault] after-first-write: stock decremented but order not inserted');
  }

  return ordersRepo.insert({ userId, itemId, qty });
}

// ── 2pc ───────────────────────────────────────────────────────────────────────
// アプリ主導の 2 フェーズコミット。items-db と orders-db の両方に PREPARE TRANSACTION を発行。
// App-orchestrated 2-phase commit: PREPARE TRANSACTION on both items-db and orders-db.
async function createOrder2pc(
  cmd: CreateOrderCmd,
  deps: { itemsPort: ItemsPort; ordersRepo: OrdersRepoPort; userId: number; fault?: FaultOpts },
): Promise<Order> {
  const { itemId, qty } = cmd;
  const { itemsPort, ordersRepo, userId, fault } = deps;

  // gid: 両 DB の PREPARE TRANSACTION に使う一意識別子 (最大 200 文字)。
  // gid: unique identifier for PREPARE TRANSACTION on both DBs (max 200 chars).
  const gid = `ord-${randomUUID()}`;

  // Phase 1a: items-db に prepare を発行。
  // Phase 1a: send prepare to items-db via items-service.
  const itemsPrepare = await itemsPort.prepareTxDecrement(gid, itemId, qty);
  if (!itemsPrepare.ok) {
    // 在庫不足 → items-db は何も prepare していない → orders-db には触らない。
    // Insufficient stock — items-db prepared nothing; do not touch orders-db.
    throw new StockError(itemsPrepare.stock);
  }

  // Phase 1b: orders-db にローカルで prepare。
  // Phase 1b: prepare locally on orders-db.
  let order: Order;
  try {
    order = await ordersRepo.prepareInsert(gid, { userId, itemId, qty });
  } catch (err) {
    // orders 側の prepare 失敗 → items の prepared tx を rollback して 500。
    // orders prepare failed → rollback items prepared tx, then rethrow.
    try { await itemsPort.rollbackTx(gid); } catch { /* best effort */ }
    throw err;
  }

  // FAULT POINT: after-prepare-all — 両 DB に prepared tx が残った状態で意図的に throw。
  // ジャーナル書込の前 → recovery は ABORT する系列 (Lv19 互換)。
  // FAULT POINT: after-prepare-all — deliberately throw with both DBs holding prepared txns.
  // Placed BEFORE journal write → recovery will ABORT (maintains Lv19 semantics).
  if (fault?.faultPoint === 'after-prepare-all') {
    throw new Error('[fault] after-prepare-all: both prepared txns orphaned (in-doubt demo)');
  }

  // COMMIT POINT: 決定を永続化してから Phase 2 に進む。
  // COMMIT POINT: persist the commit decision before entering Phase 2.
  await ordersRepo.writeJournalCommit(gid);

  // FAULT POINT: after-journal — ジャーナル書込後・commit 前に throw。
  // recovery は COMMIT する系列 (Lv21 自動回復デモ)。
  // FAULT POINT: after-journal — throw after journal write, before commit.
  // recovery will COMMIT (Lv21 auto-recovery demo).
  if (fault?.faultPoint === 'after-journal') {
    throw new Error('[fault] after-journal: journaled commit decision but crash before committing');
  }

  // Phase 2: 両方をコミット。
  // Phase 2: commit both prepared transactions.
  await itemsPort.commitTx(gid);
  await ordersRepo.commitPrepared(gid);

  // ジャーナル行を掃除 (best-effort)。recovery は冪等なので消し損ねても安全。
  // Clean up journal row (best-effort). Recovery is idempotent so a missed delete is safe.
  await ordersRepo.deleteJournal(gid);

  return order;
}

// ── saga ──────────────────────────────────────────────────────────────────────
// オーケストレーション saga: saga_log + reservations テーブルで冪等性と補償を実現。
// Orchestration saga: saga_log + reservations tables for idempotency and compensation.
async function createOrderSaga(
  cmd: CreateOrderCmd,
  deps: { itemsPort: ItemsPort; ordersRepo: OrdersRepoPort; userId: number; fault?: FaultOpts },
): Promise<Order> {
  const { itemId, qty } = cmd;
  const { itemsPort, ordersRepo, userId, fault } = deps;

  const gid = `ord-${randomUUID()}`;

  // Step 1: saga_log に state='reserved' で初期行を作成 (orders-db)。
  // Step 1: insert saga_log row with state='reserved' into orders-db.
  await ordersRepo.insertSagaLog({ gid, userId, itemId, qty });

  // Step 2: items-service に reserve を送る (冪等)。
  // Step 2: send reserve to items-service (idempotent via reservations table).
  const reserveResult = await itemsPort.reserveStock(gid, itemId, qty);
  if (!reserveResult.ok) {
    // 在庫不足 → saga_log を 'compensated' に更新して 409。
    // Insufficient stock — no reservation made; mark saga_log compensated and return 409.
    await ordersRepo.updateSagaLogState(gid, 'compensated');
    throw new StockError(reserveResult.stock);
  }

  // FAULT POINT: after-first-write — stock reserved, order not inserted → 回復ポーラーが補償。
  // FAULT POINT: after-first-write — stock reserved but order not inserted; poller compensates.
  if (fault?.faultPoint === 'after-first-write') {
    throw new Error('[fault] after-first-write: reserved but order not inserted (poller will compensate)');
  }

  // Step 3: order を orders-db に INSERT し、saga_log を 'completed' に更新。
  // Step 3: INSERT order into orders-db and update saga_log to 'completed'.
  let order: Order;
  try {
    order = await ordersRepo.insert({ userId, itemId, qty });
  } catch (err) {
    // order insert 失敗 → インライン補償: release → saga_log 'compensated' → 500。
    // order insert failed → inline compensation: release stock, mark saga compensated, rethrow.
    try { await itemsPort.releaseStock(gid); } catch { /* best effort */ }
    await ordersRepo.updateSagaLogState(gid, 'compensated');
    throw err;
  }

  await ordersRepo.updateSagaLogState(gid, 'completed');
  return order;
}

// ── outbox ────────────────────────────────────────────────────────────────────
// Transactional outbox: orders insert + outbox insert を同一 orders-db tx で書く。
// items への配送は非同期ポーラーが担当 — ここでは dual-write しない。
// Transactional outbox: orders insert + outbox insert in a single orders-db tx.
// Delivery to items is handled asynchronously by the outbox poller — no dual-write here.
async function createOrderOutbox(
  cmd: CreateOrderCmd,
  deps: { ordersRepo: OrdersRepoPort; userId: number },
): Promise<Order> {
  const { itemId, qty } = cmd;
  const { ordersRepo, userId } = deps;

  const msgId = `msg-${randomUUID()}`;
  return ordersRepo.insertOrderWithOutbox(msgId, { userId, itemId, qty });
}
