import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import type { UsersPort } from '../../ports/users-port.js';
import type { ItemsPort } from '../../ports/items-port.js';
import { listRecent } from './repo.js';
import { createOrder, AuthError, StockError } from './service.js';

export interface OrdersPluginOptions {
  ordersPool: Pool;
  usersPort: UsersPort;
  itemsPort: ItemsPort;
  mode: 'join' | 'port';
  INSTANCE: string;
}

export async function ordersRoutes(app: FastifyInstance, opts: OrdersPluginOptions): Promise<void> {
  const { ordersPool, usersPort, itemsPort, mode, INSTANCE } = opts;

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
      const order = await createOrder({ token, itemId, qty }, { usersPort, itemsPort, ordersPool, mode });
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
    const orders = await listRecent(ordersPool);
    return { instance: INSTANCE, orders };
  });
}
