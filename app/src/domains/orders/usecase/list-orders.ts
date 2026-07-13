// listOrders ユースケース: 最新 100 件の注文一覧を返す。
// ListOrders usecase: returns the most recent 100 orders.
import type { OrdersRepoPort } from '../ports.js';
import type { Order } from '../domain/order.js';

export interface ListOrdersDeps {
  ordersRepo: OrdersRepoPort;
}

export async function listOrders(deps: ListOrdersDeps): Promise<Order[]> {
  return deps.ordersRepo.listRecent();
}
