// createOrder ユースケース: auth → stock check → insert のオーケストレーション。
// CreateOrder usecase: auth → stock check → insert orchestration.
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

export interface CreateOrderDeps {
  usersPort: UsersPort;
  itemsPort: ItemsPort;
  ordersRepo: OrdersRepoPort;
  // 'join': orders と items が同一 DB → 直接 SQL でトランザクション。
  // 'port': 別 DB の可能性あり → ItemsPort 経由。
  // 'join': orders and items share a DB — use direct SQL transaction.
  // 'port': possibly separate DBs — go through ItemsPort.
  mode: 'join' | 'port';
}

export async function createOrder(cmd: CreateOrderCmd, deps: CreateOrderDeps): Promise<Order> {
  const { token, itemId, qty } = cmd;
  const { usersPort, itemsPort, ordersRepo, mode } = deps;

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
  } else {
    // (2b) port モード: ItemsPort.decrementStock → orders INSERT のみ。
    // (2b) port mode: deduct via ItemsPort, then insert order row only.
    const dec = await itemsPort.decrementStock(itemId, qty);
    if (!dec.ok) throw new StockError(dec.stock);
    return await ordersRepo.insert({ userId: user.id, itemId, qty });
  }
}
