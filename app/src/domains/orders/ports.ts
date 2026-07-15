// orders ドメインが所有する driven ポート定義。
// Driven ports owned by the orders domain.
import type { Order } from './domain/order.js';

// Lv19 saga_log のステート型。
// Lv19 saga_log state machine states.
export type SagaState = 'reserved' | 'completed' | 'compensating' | 'compensated';

export interface SagaLogRow {
  gid: string;
  user_id: number;
  item_id: number;
  qty: number;
  state: SagaState;
  created_at: string;
  updated_at: string;
}

export interface OrdersRepoPort {
  insert(args: { userId: number; itemId: number; qty: number }): Promise<Order>;
  listRecent(): Promise<Order[]>;
  createWithDirectStock(args: { userId: number; itemId: number; qty: number }): Promise<{
    ok: boolean;
    order?: Order;
    stock?: number;
  }>;

  // Lv19 2PC: orders-db 側の prepare/commit/rollback。
  // Lv19 2PC: orders-db side prepare/commit/rollback via PREPARE TRANSACTION.
  prepareInsert(gid: string, args: { userId: number; itemId: number; qty: number }): Promise<Order>;
  commitPrepared(gid: string): Promise<void>;
  rollbackPrepared(gid: string): Promise<void>;

  // Lv21 2PC 決定ジャーナル: crash recovery 用のコミット決定永続化。
  // Lv21 2PC decision journal: persist commit decision for crash recovery.
  initTxJournalSchema(): Promise<void>;
  writeJournalCommit(gid: string): Promise<void>;
  deleteJournal(gid: string): Promise<void>;
  listJournalCommits(): Promise<string[]>;
  listPreparedGids(): Promise<string[]>;

  // Lv19 saga: saga_log 操作。
  // Lv19 saga: saga_log CRUD operations.
  insertSagaLog(args: { gid: string; userId: number; itemId: number; qty: number }): Promise<void>;
  updateSagaLogState(gid: string, state: SagaState): Promise<void>;
  // 回復ポーラー用: state='reserved' かつ threshold より古い行を 'compensating' に更新して返す。
  // For recovery poller: atomically move stale 'reserved' rows to 'compensating' and return them.
  claimStuckSagaLogs(thresholdSeconds: number): Promise<SagaLogRow[]>;

  // Lv22 outbox: orders insert + outbox insert を単一 tx で実行。
  // Lv22 outbox: insert order and outbox row in a single tx on orders-db.
  initOutboxSchema(): Promise<void>;
  insertOrderWithOutbox(msgId: string, args: { userId: number; itemId: number; qty: number }): Promise<Order>;
  claimUndeliveredOutbox(limit: number): Promise<{ msg_id: string; order_id: number; item_id: number; qty: number }[]>;
  markOutboxDelivered(msgId: string): Promise<void>;

  // Lv23 choreography saga: choreo_outbox スキーマ + 送受信メソッド。
  // Lv23 choreography saga: choreo_outbox schema + send/receive methods.
  initChoreoSchema(): Promise<void>;
  // orders insert(status='pending') + choreo_outbox insert(OrderCreated) を単一 tx で実行。
  // Insert pending order + OrderCreated event in a single orders-db tx.
  insertPendingOrderWithEvent(msgId: string, args: { userId: number; itemId: number; qty: number }): Promise<Order>;
  // 未配送の choreo_outbox 行を取得。
  // Fetch undelivered choreo_outbox rows.
  claimUndeliveredChoreoOutbox(limit: number): Promise<{ msg_id: string; event_type: string; payload: unknown }[]>;
  // choreo_outbox 行を配送済みにマーク。
  // Mark choreo_outbox row as delivered.
  markChoreoOutboxDelivered(msgId: string): Promise<void>;
  // processed_messages + orders status 更新を単一 orders-db tx で実行 (冪等)。
  // Apply StockReserved/StockRejected event: dedup + status update in a single orders-db tx.
  applyOrderEventIdempotent(msgId: string, eventType: string, payload: { orderId: number }): Promise<void>;
  // orders-db の processed_messages スキーマ (choreography 用)。
  // orders-db processed_messages schema (for choreography dedup).
  initOrdersInboxSchema(): Promise<void>;
}
