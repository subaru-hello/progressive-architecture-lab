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
}
