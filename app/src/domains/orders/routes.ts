// orders HTTP アダプタ: リクエスト解析 → ユースケース呼び出し → レスポンスマッピング。
// Orders HTTP adapter: parse request → call usecase → map response. No SQL, no business logic.
import type { FastifyInstance } from 'fastify';
import type { UsersPort } from '../../ports/users-port.js';
import type { ItemsPort } from '../../ports/items-port.js';
import type { OrdersRepoPort } from './ports.js';
import { createOrder, AuthError, StockError, type TxMode, type SagaStyle, type FaultOpts } from './usecase/create-order.js';
import { listOrders } from './usecase/list-orders.js';

export interface OrdersPluginOptions {
  ordersRepo: OrdersRepoPort;
  usersPort: UsersPort;
  itemsPort: ItemsPort;
  mode: 'join' | 'port';
  INSTANCE: string;
  // Lv19: port モードのみ有効。未設定時は 'none' = 既存動作。
  // Lv19: only active in port mode; defaults to 'none' = existing behavior.
  txMode?: TxMode;
  // Lv23: saga txMode 時のみ有効。未設定時は 'orchestration' = 既存動作。
  // Lv23: only meaningful when txMode='saga'; defaults to 'orchestration' = existing behavior.
  sagaStyle?: SagaStyle;
  fault?: FaultOpts;
}

export async function ordersRoutes(app: FastifyInstance, opts: OrdersPluginOptions): Promise<void> {
  const { ordersRepo, usersPort, itemsPort, mode, INSTANCE, txMode = 'none', sagaStyle = 'orchestration', fault } = opts;

  // Lv23 choreography saga: StockReserved/StockRejected イベント受信エンドポイント (冪等)。
  // Lv23 choreography: receive StockReserved/StockRejected; idempotent via processed_messages.
  // Lv23 choreography: orders が consume するのは StockReserved/StockRejected。items 側の受信 endpoint と
  // パスを分ける (SERVICE=all で同一インスタンスに両ドメインが載るため同名だと重複登録でクラッシュ)。
  app.post('/internal/choreo/stock-events', async (req, reply) => {
    const body = req.body as { msgId?: string; eventType?: string; payload?: { orderId?: number } };
    if (!body?.msgId || !body.eventType || body.payload?.orderId == null) {
      reply.code(400);
      return { error: 'msgId, eventType, and payload.orderId are required' };
    }
    if (body.eventType !== 'StockReserved' && body.eventType !== 'StockRejected') {
      reply.code(400);
      return { error: 'eventType must be StockReserved or StockRejected' };
    }
    try {
      await ordersRepo.applyOrderEventIdempotent(body.msgId, body.eventType, { orderId: Number(body.payload.orderId) });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      reply.code(500);
      return { error: `applyOrderEventIdempotent failed: ${msg}` };
    }
    return { ok: true };
  });

  app.post('/orders', async (req, reply) => {
    const token = req.headers['x-auth-token'] as string | undefined;
    if (!token) {
      reply.code(401);
      return { error: 'x-auth-token header required' };
    }
    const body = req.body as { itemId?: unknown; qty?: unknown };
    const itemId = Number(body?.itemId);
    const qty = Number(body?.qty);
    // 正の整数のみ受け付ける: 1.5 や 2.9 が SQL まで到達するのを防ぐ。
    // Require positive integers: block 1.5/2.9 from reaching SQL UPDATE/WHERE.
    if (!Number.isInteger(itemId) || itemId <= 0 || !Number.isInteger(qty) || qty <= 0) {
      reply.code(400);
      return { error: 'itemId and qty must be positive integers' };
    }

    try {
      const order = await createOrder(
        { token, itemId, qty },
        { usersPort, itemsPort, ordersRepo, mode, txMode, sagaStyle, fault },
      );
      reply.code(201);
      return { instance: INSTANCE, order };
    } catch (err) {
      if (err instanceof AuthError) {
        reply.code(401);
        return { error: err.message };
      }
      if (err instanceof StockError) {
        reply.code(409);
        return { error: err.message, stock: err.stock };
      }
      throw err;
    }
  });

  app.get('/orders', async () => {
    const orders = await listOrders({ ordersRepo });
    return { instance: INSTANCE, orders };
  });
}
