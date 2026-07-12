import Fastify from 'fastify';
import os from 'node:os';
import pg from 'pg';
import client from 'prom-client';
import { Redis } from 'ioredis';
import { WorkerPool } from './pool.js';
import { itemsRoutes, STREAM_NAME } from './domains/items/routes.js';
import { usersRoutes } from './domains/users/routes.js';
import { ordersRoutes } from './domains/orders/routes.js';
import { initSchema as itemsInitSchema, seed as itemsSeed } from './domains/items/repo.js';
import { initSchema as usersInitSchema, seed as usersSeed } from './domains/users/repo.js';
import { initSchema as ordersInitSchema } from './domains/orders/repo.js';
import { InProcessItemsAdapter, HttpItemsAdapter } from './ports/items-port.js';
import { InProcessUsersAdapter, HttpUsersAdapter } from './ports/users-port.js';

const { Pool } = pg;

const PORT = Number(process.env.PORT ?? 3000);
// INSTANCE_ID lets us SEE which replica/pod served a request (great for watching load balancing).
const INSTANCE = process.env.INSTANCE_ID ?? os.hostname();

// SERVICE: どのドメインをこのプロセスがホストするか。
// SERVICE: which domain(s) this process hosts (default 'all' = monolith).
const SERVICE = process.env.SERVICE ?? 'all';

// ORDERS_CROSS_CONTEXT: orders が items テーブルに直接 JOIN するか Port 経由か。
// ORDERS_CROSS_CONTEXT: whether orders accesses items via direct SQL ('join') or port ('port').
const ORDERS_CROSS_CONTEXT = (process.env.ORDERS_CROSS_CONTEXT ?? 'join') as 'join' | 'port';

const ITEMS_SERVICE_URL = process.env.ITEMS_SERVICE_URL;
const USERS_SERVICE_URL = process.env.USERS_SERVICE_URL;

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

// --- Worker thread pool (optional CPU offload layer) ---
// WORKER_POOL_SIZE を設定しない compose (Lv0-6) では pool は null のまま → 同期パス継続。
// WORKER_POOL_SIZE unset → pool stays null → /work runs the synchronous path
// (Lv0–6 behavior, unchanged). A non-positive/garbage value would build an empty
// pool that queues forever and 503s everything, so validate it up front.
const poolSize = Math.trunc(Number(process.env.WORKER_POOL_SIZE));
let pool: WorkerPool | null = null;
if (poolSize > 0) {
  pool = new WorkerPool(poolSize, new URL('./cpu-worker.js', import.meta.url));
}
// Backpressure bound. Default: 100 jobs per worker. Floor at 1 so a stray
// WORKER_QUEUE_MAX="0" can't turn every /work into an instant 503.
const WORKER_QUEUE_MAX = Math.max(
  1,
  Math.trunc(Number(process.env.WORKER_QUEUE_MAX ?? poolSize * 100)),
);

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

// --- Async write queue (optional write-behind layer, Lv8) ---
// ASYNC_WRITE を設定しない compose (Lv0-7) では同期 INSERT+201 のまま（後方互換）。
// ASYNC_WRITE が truthy かつ REDIS_URL 未設定は設定ミスなので即死させる。
const ASYNC_WRITE = process.env.ASYNC_WRITE;
if (ASYNC_WRITE && !REDIS_URL) {
  throw new Error('ASYNC_WRITE requires REDIS_URL to be set');
}

// SEED_DEMO: 未設定（デフォルト）はシードなし → 旧ステージ（Lv0-12）の空リスト基準を維持。
// SEED_DEMO unset (default) → no seeding → old-stage empty-list baseline is preserved.
// New Lv13+ composes set SEED_DEMO=1 to populate demo data.
const SEED_DEMO = process.env.SEED_DEMO;

// join モード + HTTP サービス URL は設定ミス: items テーブルが同一 DB に無い前提で
// 直接 SQL を打つと全 order が 500 になる → 起動時に即死させる。
// join mode + HTTP service URL is a misconfiguration: direct SQL against items table
// will always fail when items runs as a separate service → fail fast at boot.
if (ORDERS_CROSS_CONTEXT === 'join' && (ITEMS_SERVICE_URL || USERS_SERVICE_URL)) {
  throw new Error(
    'ORDERS_CROSS_CONTEXT=join is incompatible with ITEMS_SERVICE_URL / USERS_SERVICE_URL. ' +
    'Use ORDERS_CROSS_CONTEXT=port for microservice deployments.',
  );
}
// WRITE_QUEUE_MAX: 背圧しきい値。Stream depth がこれを超えると 503 を返す。
// 未設定なら大きめの既定値 100000。floor at 1 でゼロ設定ガード。
const WRITE_QUEUE_MAX = Math.max(
  1,
  Math.trunc(Number(process.env.WRITE_QUEUE_MAX ?? 100000)),
);

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
const workerQueueDepth = new client.Gauge({
  name: 'worker_pool_queue_depth',
  help: 'Number of jobs waiting in the worker pool queue',
  registers: [register],
});
const workerBusy = new client.Gauge({
  name: 'worker_pool_busy',
  help: 'Number of worker threads currently processing a job',
  registers: [register],
});
const writeQueueDepth = new client.Gauge({
  name: 'write_queue_depth',
  help: 'Number of pending entries in the write-behind Redis Stream (items:writes)',
  registers: [register],
});

// In-process counters for /cache eyeball endpoint (mirrors Prometheus counters).
// オブジェクト経由で渡すことで itemsRoutes プラグインが参照を共有できる。
// Passed as object so itemsRoutes plugin shares the same reference.
const cacheCounters = { hits: 0, misses: 0 };

const app = Fastify({ logger: true });

// onResponse フックは全ルートに適用されるよう、プラグイン登録より前に addHook する。
// Register onResponse before plugins so it covers all domain routes too.
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
  if (pool) {
    workerQueueDepth.set(pool.queueDepth);
    workerBusy.set(pool.busy);
  }
  if (ASYNC_WRITE && redis) {
    try {
      const depth = await redis.xlen(STREAM_NAME);
      writeQueueDepth.set(depth);
    } catch {
      // Keep previous value; don't block metrics on Redis unavailability.
    }
  }
  reply.header('Content-Type', register.contentType);
  return register.metrics();
});

// Work simulator: create latency (ms) and/or CPU load (cpu) to drive scaling experiments.
//   GET /work?ms=50        -> 50ms of I/O-ish wait
//   GET /work?cpu=20       -> ~20M sqrt iterations of CPU burn
//   WORKER_POOL_SIZE が設定されている場合は cpu > 0 のジョブを worker thread に offload。
app.get('/work', async (req, reply) => {
  const q = req.query as { ms?: string; cpu?: string };
  const ms = Number(q.ms ?? 0);
  const cpu = Number(q.cpu ?? 0);
  if (ms > 0) await new Promise((r) => setTimeout(r, ms));
  if (pool && cpu > 0) {
    if (pool.queueDepth >= WORKER_QUEUE_MAX) {
      reply.code(503);
      return { error: 'worker pool saturated', queueDepth: pool.queueDepth, instance: INSTANCE };
    }
    const { sum } = await pool.run({ cpu });
    return { instance: INSTANCE, ms, cpu, sum, offloaded: true };
  }
  let sum = 0;
  for (let i = 0; i < cpu * 1_000_000; i++) sum += Math.sqrt(i);
  return { instance: INSTANCE, ms, cpu, sum };
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

// --- ドメインルートの登録（SERVICE env で制御）---
// --- Domain route registration (controlled by SERVICE env) ---
const loadItems = SERVICE === 'all' || SERVICE === 'items';
const loadUsers = SERVICE === 'all' || SERVICE === 'users';
const loadOrders = SERVICE === 'all' || SERVICE === 'orders';

if (loadItems) {
  await app.register(itemsRoutes, {
    writePool,
    readPool,
    redis,
    INSTANCE,
    ASYNC_WRITE,
    WRITE_QUEUE_MAX,
    cacheHits,
    cacheMisses,
    cacheCounters,
  });
}

if (loadUsers) {
  await app.register(usersRoutes, { writePool, INSTANCE });
}

if (loadOrders) {
  // items ポートの選択: ITEMS_SERVICE_URL が設定されていれば HTTP アダプタ、なければインプロセス。
  // Items port selection: HTTP adapter when ITEMS_SERVICE_URL is set, in-process otherwise.
  const itemsPort = ITEMS_SERVICE_URL
    ? new HttpItemsAdapter(ITEMS_SERVICE_URL)
    : new InProcessItemsAdapter(writePool);

  // users ポートの選択: USERS_SERVICE_URL が設定されていれば HTTP アダプタ、なければインプロセス。
  // Users port selection: HTTP adapter when USERS_SERVICE_URL is set, in-process otherwise.
  const usersPort = USERS_SERVICE_URL
    ? new HttpUsersAdapter(USERS_SERVICE_URL)
    : new InProcessUsersAdapter(writePool);

  await app.register(ordersRoutes, {
    ordersPool: writePool,
    usersPort,
    itemsPort,
    mode: ORDERS_CROSS_CONTEXT,
    INSTANCE,
  });
}

async function initDb(): Promise<void> {
  if (loadItems) {
    await itemsInitSchema(writePool);
    // SEED_DEMO が設定されている場合のみシード実行（旧ステージとの後方互換を守る）。
    // Seed only when SEED_DEMO is set — preserves empty-list baseline for old stages.
    if (SEED_DEMO) await itemsSeed(writePool);
  }
  if (loadUsers) {
    await usersInitSchema(writePool);
    if (SEED_DEMO) await usersSeed(writePool);
  }
  if (loadOrders) await ordersInitSchema(writePool);
}

async function main(): Promise<void> {
  // ASYNC_WRITE パスでは Redis が write の必須依存になる。
  // lazyConnect のまま最初のリクエストで接続しようとすると enableOfflineQueue:false が
  // 邪魔して XLEN/XADD が 500 を返す（接続中にキューされないため）。
  // 起動時に明示的に connect() してサーブ前に Redis 接続を確立する。
  if (ASYNC_WRITE && redis) {
    try {
      await redis.connect();
    } catch (err) {
      // 「既に connecting/connected」の良性 reject だけ握る。Redis が本当に未達なら
      // fail-fast する。ASYNC_WRITE では Redis が write の本線なので、繋がらないまま
      // listen すると /ready は green のまま全 POST が 503 になる（write-dead な緑）。
      const msg = err instanceof Error ? err.message : String(err);
      if (!/already (connect|connecting|connected)/i.test(msg)) {
        throw new Error(`ASYNC_WRITE: Redis connect failed: ${msg}`);
      }
    }
  }

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
  // items ドメインをロードしているときだけレプリカ可視性チェックを行う。
  // Replica visibility check only when items domain is loaded.
  if (loadItems) {
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
  }

  await app.listen({ port: PORT, host: '0.0.0.0' });
}

// Graceful shutdown so k8s rolling updates / SIGTERM drain cleanly.
for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, async () => {
    app.log.info(`${sig} received, shutting down...`);
    try {
      await app.close();
      await Promise.all([writePool.end(), readPool.end(), ...(redis ? [redis.quit()] : []), ...(pool ? [pool.destroy()] : [])]);
    } finally {
      process.exit(0);
    }
  });
}

main().catch((err) => {
  app.log.error(err);
  process.exit(1);
});
