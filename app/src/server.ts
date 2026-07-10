import Fastify from 'fastify';
import os from 'node:os';
import pg from 'pg';
import client from 'prom-client';
import { Redis } from 'ioredis';

const { Pool } = pg;

const PORT = Number(process.env.PORT ?? 3000);
// INSTANCE_ID lets us SEE which replica/pod served a request (great for watching load balancing).
const INSTANCE = process.env.INSTANCE_ID ?? os.hostname();

const writePool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: Number(process.env.PG_POOL_MAX ?? 10),
});

// readPool falls back to DATABASE_URL when DATABASE_URL_RO is not set.
// This keeps Lv0–4 composes (which only set DATABASE_URL) working unchanged.
const readPool = new Pool({
  connectionString: process.env.DATABASE_URL_RO ?? process.env.DATABASE_URL,
  max: Number(process.env.PG_POOL_MAX_RO ?? process.env.PG_POOL_MAX ?? 10),
});

// --- Redis (optional cache layer) ---
// REDIS_URL を設定しない compose (Lv0-5) では redis 変数は null のまま。
// Redis が落ちても try/catch でフォールバックするので /ready には含めない。
const REDIS_URL = process.env.REDIS_URL;
let redis: Redis | null = null;
if (REDIS_URL) {
  redis = new Redis(REDIS_URL, {
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    connectTimeout: 2000,
    enableOfflineQueue: false,
  });
  redis.on('error', () => {
    // Suppress unhandled error events; individual call catch() handles fallback.
  });
}

// --- Prometheus metrics (observability from day 1) ---
const register = new client.Registry();
register.setDefaultLabels({ instance: INSTANCE });
client.collectDefaultMetrics({ register });
const httpHistogram = new client.Histogram({
  name: 'http_request_duration_seconds',
  help: 'HTTP request duration in seconds',
  labelNames: ['method', 'route', 'status'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
  registers: [register],
});
const cacheHits = new client.Counter({
  name: 'cache_hits_total',
  help: 'Number of Redis cache hits',
  registers: [register],
});
const cacheMisses = new client.Counter({
  name: 'cache_misses_total',
  help: 'Number of Redis cache misses (or Redis unavailable fallbacks)',
  registers: [register],
});

// In-process counters for /cache eyeball endpoint (mirrors Prometheus counters).
let hitCount = 0;
let missCount = 0;

const CACHE_KEY = 'items:latest100';
const CACHE_TTL = 30; // seconds

async function initDb(): Promise<void> {
  await writePool.query(`
    CREATE TABLE IF NOT EXISTS items (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}

const app = Fastify({ logger: true });

app.addHook('onResponse', async (req, reply) => {
  const route = req.routeOptions?.url ?? req.url;
  httpHistogram.labels(req.method, route, String(reply.statusCode)).observe(reply.elapsedTime / 1000);
});

// Liveness: cheap, no dependencies. k8s livenessProbe hits this.
app.get('/health', async () => ({ status: 'ok', instance: INSTANCE }));

// Readiness: verifies DB reachability. k8s readinessProbe hits this.
// Both writePool (primary) and readPool (replica or same as primary) must be reachable.
app.get('/ready', async (_req, reply) => {
  try {
    await Promise.all([writePool.query('SELECT 1'), readPool.query('SELECT 1')]);
    return { status: 'ready', instance: INSTANCE };
  } catch {
    reply.code(503);
    return { status: 'not-ready', instance: INSTANCE };
  }
});

app.get('/metrics', async (_req, reply) => {
  reply.header('Content-Type', register.contentType);
  return register.metrics();
});

// Work simulator: create latency (ms) and/or CPU load (cpu) to drive scaling experiments.
//   GET /work?ms=50        -> 50ms of I/O-ish wait
//   GET /work?cpu=20       -> ~20M sqrt iterations of CPU burn
app.get('/work', async (req) => {
  const q = req.query as { ms?: string; cpu?: string };
  const ms = Number(q.ms ?? 0);
  const cpu = Number(q.cpu ?? 0);
  if (ms > 0) await new Promise((r) => setTimeout(r, ms));
  let sum = 0;
  for (let i = 0; i < cpu * 1_000_000; i++) sum += Math.sqrt(i);
  return { instance: INSTANCE, ms, cpu, sum };
});

app.get('/items', async () => {
  // Cache-aside: try Redis first (only when REDIS_URL is configured).
  if (redis) {
    try {
      const cached = await redis.get(CACHE_KEY);
      if (cached !== null) {
        cacheHits.inc();
        hitCount++;
        // instance is always current (not cached) so LB visibility is preserved.
        return { instance: INSTANCE, items: JSON.parse(cached) };
      }
    } catch {
      // Redis unavailable — fall through to DB, count as miss.
    }
    cacheMisses.inc();
    missCount++;
  }

  const { rows } = await readPool.query(
    'SELECT id, name, created_at FROM items ORDER BY id DESC LIMIT 100',
  );

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
  const { rows } = await writePool.query(
    'INSERT INTO items(name) VALUES($1) RETURNING id, name, created_at',
    [body.name],
  );

  // Invalidate the cache key after writing. NOTE: this is best-effort, not a
  // correctness guarantee. Classic cache-aside race: a concurrent GET that read
  // the DB *before* this INSERT can still SET its stale snapshot *after* this DEL,
  // leaving stale data cached until the TTL expires. Under concurrent load this
  // window is real — it's the invalidation trap this stage exists to show.
  if (redis) {
    try {
      await redis.del(CACHE_KEY);
    } catch {
      // Best-effort invalidation; ignore errors.
    }
  }

  reply.code(201);
  return { instance: INSTANCE, item: rows[0] };
});

// Eyeball endpoint: quick check of cache efficiency for this process.
app.get('/cache', async () => {
  const total = hitCount + missCount;
  return {
    instance: INSTANCE,
    hits: hitCount,
    misses: missCount,
    hit_ratio: total === 0 ? 0 : hitCount / total,
  };
});

// Replication lag endpoint: only meaningful on a replica (primary returns null lag, which is normal).
app.get('/replication', async (_req, reply) => {
  try {
    const { rows } = await readPool.query(
      'SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::float AS lag_seconds',
    );
    return { instance: INSTANCE, lag_seconds: rows[0].lag_seconds };
  } catch {
    reply.code(503);
    return { instance: INSTANCE, error: 'replication query failed' };
  }
});

app.get('/', async () => ({
  service: 'progressive-architecture-lab api',
  instance: INSTANCE,
  endpoints: ['/health', '/ready', '/metrics', '/work?ms=&cpu=', 'GET/POST /items', '/replication', '/cache'],
}));

async function main(): Promise<void> {
  // Postgres may still be booting when the API starts; retry init before serving.
  for (let attempt = 1; ; attempt++) {
    try {
      await initDb();
      break;
    } catch (err) {
      if (attempt >= 30) throw err;
      app.log.warn(`DB not ready (attempt ${attempt}/30), retrying in 2s...`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  // The CREATE TABLE above runs on writePool (primary). When readPool is a replica,
  // that DDL reaches it only after it streams over — so an early GET /items could hit
  // the replica before `items` exists and 500 (cold-start read-after-write). Wait until
  // the table is visible on readPool. When readPool == writePool (Lv0–4) this passes on
  // the first try. to_regclass returns NULL (not an error) when the table is absent.
  for (let attempt = 1; ; attempt++) {
    try {
      const { rows } = await readPool.query(`SELECT to_regclass('public.items') AS t`);
      if (rows[0].t) break;
    } catch {
      // fall through to retry (readPool/replica may not be reachable yet)
    }
    if (attempt >= 30) throw new Error('items table not visible on readPool after 30 attempts');
    app.log.warn(`readPool: waiting for items to replicate (attempt ${attempt}/30)...`);
    await new Promise((r) => setTimeout(r, 1000));
  }
  await app.listen({ port: PORT, host: '0.0.0.0' });
}

// Graceful shutdown so k8s rolling updates / SIGTERM drain cleanly.
for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, async () => {
    app.log.info(`${sig} received, shutting down...`);
    try {
      await app.close();
      await Promise.all([writePool.end(), readPool.end(), ...(redis ? [redis.quit()] : [])]);
    } finally {
      process.exit(0);
    }
  });
}

main().catch((err) => {
  app.log.error(err);
  process.exit(1);
});
