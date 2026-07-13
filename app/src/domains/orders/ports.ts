// orders ドメインが所有する driven ポート定義。
// Driven ports owned by the orders domain.
import type { Order } from './domain/order.js';

export interface OrdersRepoPort {
  insert(args: { userId: number; itemId: number; qty: number }): Promise<Order>;
  listRecent(): Promise<Order[]>;
  createWithDirectStock(args: { userId: number; itemId: number; qty: number }): Promise<{
    ok: boolean;
    order?: Order;
    stock?: number;
  }>;
}
