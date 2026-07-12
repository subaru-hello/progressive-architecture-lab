import type { Pool } from 'pg';
import type { UsersPort } from '../../ports/users-port.js';
import type { ItemsPort } from '../../ports/items-port.js';
import { insert, createWithDirectStock } from './repo.js';
import type { Order } from './repo.js';

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

interface CreateOrderArgs {
  token: string;
  itemId: number;
  qty: number;
}

interface CreateOrderDeps {
  usersPort: UsersPort;
  itemsPort: ItemsPort;
  ordersPool: Pool;
  // 'join': orders と items が同一 DB → 直接 SQL でトランザクション。
  // 'port': 別 DB の可能性あり → ItemsPort 経由。
  // 'join': orders and items share a DB — use direct SQL transaction.
  // 'port': possibly separate DBs — go through ItemsPort.
  mode: 'join' | 'port';
}

export async function createOrder(args: CreateOrderArgs, deps: CreateOrderDeps): Promise<Order> {
  const { token, itemId, qty } = args;
  const { usersPort, itemsPort, ordersPool, mode } = deps;

  // (1) 認証: token が無効なら AuthError を throw → routes で 401。
  // (1) Auth: throw AuthError on invalid token → routes catches → 401.
  const user = await usersPort.authenticate(token);
  if (!user) throw new AuthError();

  if (mode === 'join') {
    // (2a) 同一 DB トランザクション: items 直接 UPDATE + orders INSERT。
    // (2a) Same-DB transaction: UPDATE items + INSERT orders atomically.
    const result = await createWithDirectStock(ordersPool, { userId: user.id, itemId, qty });
    if (!result.ok || !result.order) throw new StockError(result.stock ?? 0);
    return result.order;
  } else {
    // (2b) port モード: ItemsPort.decrementStock → orders INSERT のみ。
    // (2b) port mode: deduct via ItemsPort, then insert order row only.
    const dec = await itemsPort.decrementStock(itemId, qty);
    if (!dec.ok) throw new StockError(dec.stock);
    return await insert(ordersPool, { userId: user.id, itemId, qty });
  }
}
