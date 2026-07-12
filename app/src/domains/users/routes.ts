import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import { authenticate } from './repo.js';

export interface UsersPluginOptions {
  writePool: Pool;
  INSTANCE: string;
}

export async function usersRoutes(app: FastifyInstance, opts: UsersPluginOptions): Promise<void> {
  const { writePool, INSTANCE } = opts;

  // 内部エンドポイント: HTTP アダプタ用（orders → users）。
  // Internal endpoint: for HTTP adapter (orders → users).
  app.post('/internal/auth', async (req, reply) => {
    const body = req.body as { token?: string };
    if (!body?.token) {
      reply.code(400);
      return { error: 'token is required' };
    }
    const user = await authenticate(writePool, body.token);
    if (!user) {
      reply.code(401);
      return { error: 'unauthorized' };
    }
    return { user };
  });

  // オプション: x-auth-token ヘッダから現在ユーザーを返す。
  // Optional: return current user from x-auth-token header.
  app.get('/users/me', async (req, reply) => {
    const token = req.headers['x-auth-token'] as string | undefined;
    if (!token) {
      reply.code(401);
      return { error: 'x-auth-token header required' };
    }
    const user = await authenticate(writePool, token);
    if (!user) {
      reply.code(401);
      return { error: 'unauthorized' };
    }
    return { instance: INSTANCE, user };
  });
}
