// items HTTP アダプタ: リクエスト解析 → ユースケース呼び出し → レスポンスマッピング。
// Items HTTP adapter: parse request → call usecase → map response. No SQL here.
// Lv6-8 機構（cache-aside Redis + ASYNC_WRITE Redis Stream）はインフラ関心事としてここに保持。
// Lv6-8 machinery (cache-aside Redis + ASYNC_WRITE Redis Stream) kept here as infra concern.
import type { FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import type { Counter } from 'prom-client';
import type { ItemsRepoPort } from './ports.js';

// items ドメインが使う定数。server.ts から移植。
// Constants migrated from server.ts — single source of truth for items cache.
export const CACHE_KEY = 'items:latest100';
export const CACHE_TTL = 30; // seconds
export const STREAM_NAME = 'items:writes';

export interface ItemsPluginOptions {
  itemsRepo: ItemsRepoPort;
  redis: Redis | null;
  INSTANCE: string;
  ASYNC_WRITE: string | undefined;
  WRITE_QUEUE_MAX: number;
  cacheHits: Counter;
  cacheMisses: Counter;
  // In-process eyeball counters — mutated by reference via wrapper object.
  cacheCounters: { hits: number; misses: number };
}

export async function itemsRoutes(app: FastifyInstance, opts: ItemsPluginOptions): Promise<void> {
  const { itemsRepo, redis, INSTANCE, ASYNC_WRITE, WRITE_QUEUE_MAX, cacheHits, cacheMisses, cacheCounters } = opts;

  app.get('/items', async () => {
    // Cache-aside: Redis があれば先に試す。
    // Cache-aside: try Redis first when REDIS_URL is configured.
    if (redis) {
      try {
        const cached = await redis.get(CACHE_KEY);
        if (cached !== null) {
          cacheHits.inc();
          cacheCounters.hits++;
          return { instance: INSTANCE, items: JSON.parse(cached) };
        }
      } catch {
        // Redis 障害 → DB にフォールバック（miss 扱い）。
        // Redis unavailable — fall through to DB, count as miss.
      }
      cacheMisses.inc();
      cacheCounters.misses++;
    }

    const rows = await itemsRepo.listLatest100();

    if (redis) {
      try {
        await redis.set(CACHE_KEY, JSON.stringify(rows), 'EX', CACHE_TTL);
      } catch {
        // Best-effort write; ignore errors.
      }
    }

    return { instance: INSTANCE, items: rows };
  });

  app.post('/items', async (req, reply) => {
    const body = req.body as { name?: string };
    if (!body?.name) {
      reply.code(400);
      return { error: 'name is required' };
    }

    // Lv8: ASYNC_WRITE パス — Redis Stream に enqueue して 202 即返し。
    if (ASYNC_WRITE && redis) {
      let depth: number;
      try {
        depth = await redis.xlen(STREAM_NAME);
      } catch {
        reply.code(503);
        return { error: 'write queue unavailable', instance: INSTANCE };
      }
      if (depth >= WRITE_QUEUE_MAX) {
        reply.code(503);
        return { error: 'write queue saturated', depth, instance: INSTANCE };
      }
      let streamId: string;
      try {
        streamId = await redis.xadd(STREAM_NAME, '*', 'name', body.name, 'enqueued_at', String(Date.now())) as string;
      } catch {
        reply.code(503);
        return { error: 'write queue unavailable', instance: INSTANCE };
      }
      reply.code(202);
      return { status: 'queued', stream_id: streamId, instance: INSTANCE };
    }

    // 同期パス (Lv0-7 互換): INSERT + 201 + cache DEL。
    // Synchronous path (Lv0-7 compat): INSERT + 201 + cache DEL.
    const item = await itemsRepo.create(body.name);

    if (redis) {
      try {
        await redis.del(CACHE_KEY);
      } catch {
        // Best-effort invalidation; ignore errors.
      }
    }

    reply.code(201);
    return { instance: INSTANCE, item };
  });

  // Eyeball endpoint: quick check of cache efficiency for this process.
  app.get('/cache', async () => {
    const total = cacheCounters.hits + cacheCounters.misses;
    return {
      instance: INSTANCE,
      hits: cacheCounters.hits,
      misses: cacheCounters.misses,
      hit_ratio: total === 0 ? 0 : cacheCounters.hits / total,
    };
  });

  // 個別 item 取得。
  // Fetch single item by id.
  app.get('/items/:id', async (req, reply) => {
    const { id } = req.params as { id: string };
    const item = await itemsRepo.getById(Number(id));
    if (!item) {
      reply.code(404);
      return { error: 'not found' };
    }
    return { instance: INSTANCE, item };
  });

  // 内部エンドポイント: HTTP アダプタ用（orders → items）。
  // Internal endpoint: for HTTP adapter (orders → items).
  app.get('/internal/items/:id', async (req, reply) => {
    const { id } = req.params as { id: string };
    const item = await itemsRepo.getById(Number(id));
    if (!item) {
      reply.code(404);
      return { error: 'not found' };
    }
    return { item };
  });

  app.post('/internal/items/:id/decrement', async (req, reply) => {
    const { id } = req.params as { id: string };
    const body = req.body as { qty?: number };
    if (body?.qty == null || body.qty <= 0) {
      reply.code(400);
      return { error: 'qty must be a positive number' };
    }
    const result = await itemsRepo.decrementStock(Number(id), body.qty);
    if (!result.ok) {
      reply.code(409);
      return { error: 'insufficient stock', stock: result.stock };
    }
    return { ok: true, stock: result.stock };
  });

  // Lv19 2PC: items-db 側の prepare-decrement。
  // Lv19 2PC: prepare a decrement in items-db (phase-1 of 2PC).
  app.post('/internal/tx/prepare-decrement', async (req, reply) => {
    const body = req.body as { gid?: string; itemId?: number; qty?: number };
    if (!body?.gid || body.itemId == null || body.qty == null || body.qty <= 0) {
      reply.code(400);
      return { error: 'gid, itemId, and positive qty are required' };
    }
    const result = await itemsRepo.prepareDecrement(body.gid, Number(body.itemId), Number(body.qty));
    if (!result.ok) {
      reply.code(409);
      return { error: 'insufficient stock', stock: result.stock };
    }
    return { ok: true, stock: result.stock };
  });

  // Lv19 2PC: commit a prepared transaction in items-db.
  app.post('/internal/tx/:gid/commit', async (req, reply) => {
    const { gid } = req.params as { gid: string };
    try {
      await itemsRepo.commitPrepared(gid);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      reply.code(500);
      return { error: `commit prepared failed: ${msg}` };
    }
    return { ok: true };
  });

  // Lv19 2PC: rollback a prepared transaction in items-db.
  app.post('/internal/tx/:gid/rollback', async (req, reply) => {
    const { gid } = req.params as { gid: string };
    try {
      await itemsRepo.rollbackPrepared(gid);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      reply.code(500);
      return { error: `rollback prepared failed: ${msg}` };
    }
    return { ok: true };
  });

  // Lv19 saga: 冪等 reserve。
  // Lv19 saga: idempotent stock reservation.
  app.post('/internal/reserve', async (req, reply) => {
    const body = req.body as { gid?: string; itemId?: number; qty?: number };
    if (!body?.gid || body.itemId == null || body.qty == null || body.qty <= 0) {
      reply.code(400);
      return { error: 'gid, itemId, and positive qty are required' };
    }
    const result = await itemsRepo.reserve(body.gid, Number(body.itemId), Number(body.qty));
    if (!result.ok) {
      reply.code(409);
      return { error: 'insufficient stock', stock: result.stock };
    }
    return { ok: true, stock: result.stock };
  });

  // Lv19 saga: 冪等 release (補償)。
  // Lv19 saga: idempotent release (compensation).
  app.post('/internal/release', async (req, reply) => {
    const body = req.body as { gid?: string };
    if (!body?.gid) {
      reply.code(400);
      return { error: 'gid is required' };
    }
    try {
      await itemsRepo.release(body.gid);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      reply.code(500);
      return { error: `release failed: ${msg}` };
    }
    return { ok: true };
  });

  // Lv18 バックフィル用: 全 item 取得（LIMIT なし）。ストラングラーフィグ移行時のソース DB 読み取り。
  // Lv18 backfill: fetch ALL items (no LIMIT) — read source-of-truth during strangler-fig migration.
  app.get('/internal/items/all', async () => {
    const items = await itemsRepo.listAll();
    return { items };
  });

  // Lv18 バックフィル用: id 保持の冪等 bulk upsert。移行先 DB へのデータ投入に使う。
  // Lv18 backfill: idempotent bulk upsert preserving ids — used to populate the target DB.
  app.post('/internal/items/bulk-upsert', async (req, reply) => {
    const body = req.body as { items?: { id: number; name: string; stock: number; created_at: string }[] };
    if (!Array.isArray(body?.items)) {
      reply.code(400);
      return { error: 'items array is required' };
    }
    const upserted = await itemsRepo.bulkUpsert(body.items);
    return { upserted };
  });
}
