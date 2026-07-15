import Fastify from 'fastify';
import os from 'node:os';
import pg from 'pg';
import client from 'prom-client';
import { Redis } from 'ioredis';
import { WorkerPool } from './pool.js';
import { itemsRoutes, STREAM_NAME } from './domains/items/routes.js';
import { usersRoutes } from './domains/users/routes.js';
import { ordersRoutes } from './domains/orders/routes.js';
import { PgItemsRepo } from './domains/items/infra/pg-items-repo.js';
import { PgUsersRepo } from './domains/users/infra/pg-users-repo.js';
import { PgOrdersRepo } from './domains/orders/infra/pg-orders-repo.js';
import { InProcessItemsAdapter, HttpItemsAdapter, DualWriteItemsAdapter, MIGRATION_MODES } from './ports/items-port.js';
import type { MigrationMode } from './ports/items-port.js';
import { TX_MODES, SAGA_STYLES } from './domains/orders/usecase/create-order.js';
import type { TxMode, SagaStyle } from './domains/orders/usecase/create-order.js';
import { InProcessUsersAdapter, HttpUsersAdapter } from './ports/users-port.js';
import { mudRoutes } from './mud/routes.js';
import { mudInitSchema, mudSeed } from './mud/schema.js';

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

// ARCH: アーキテクチャ選択。'mud' = ビッグボールオブマッド、'hex' = ヘキサゴナル（デフォルト）。
// ARCH: architecture selector. 'mud' = big-ball-of-mud, 'hex' = hexagonal (default).
const ARCH = process.env.ARCH ?? 'hex';
if (ARCH !== 'mud' && ARCH !== 'hex') {
  throw new Error(`ARCH must be 'mud' or 'hex', got: '${ARCH}'`);
}

const ITEMS_SERVICE_URL = process.env.ITEMS_SERVICE_URL;
const USERS_SERVICE_URL = process.env.USERS_SERVICE_URL;

// Lv18: 移行モード。ITEMS_SERVICE_URL と組み合わせてのみ有効。未設定 → デフォルト動作を保持。
// Lv18: migration mode — only effective when ITEMS_SERVICE_URL is also set. Unset → default unchanged.
const ITEMS_MIGRATION_MODE = (process.env.ITEMS_MIGRATION_MODE ?? 'primary_only') as MigrationMode;
if (!MIGRATION_MODES.includes(ITEMS_MIGRATION_MODE)) {
  throw new Error(
    `ITEMS_MIGRATION_MODE must be one of ${MIGRATION_MODES.join(', ')}, got: '${ITEMS_MIGRATION_MODE}'`,
  );
}

// Lv19: 分散トランザクション戦略。ITEMS_SERVICE_URL がないと 2pc/saga は設定ミス。
// Lv19: distributed-tx strategy. 2pc/saga require ITEMS_SERVICE_URL — fail fast if missing.
const ORDER_TX_MODE = (process.env.ORDER_TX_MODE ?? 'none') as TxMode;
if (!TX_MODES.includes(ORDER_TX_MODE)) {
  throw new Error(`ORDER_TX_MODE must be one of ${TX_MODES.join(', ')}, got: '${ORDER_TX_MODE}'`);
}
if ((ORDER_TX_MODE === '2pc' || ORDER_TX_MODE === 'saga') && !ITEMS_SERVICE_URL) {
  throw new Error(`ORDER_TX_MODE=${ORDER_TX_MODE} requires ITEMS_SERVICE_URL to be set`);
}

// Lv23: saga スタイル。ORDER_TX_MODE=saga の時のみ意味を持つ。
// Lv23: saga style — only meaningful when ORDER_TX_MODE=saga. Default: orchestration.
const SAGA_STYLE = (process.env.SAGA_STYLE ?? 'orchestration') as SagaStyle;
if (!SAGA_STYLES.includes(SAGA_STYLE)) {
  throw new Error(`SAGA_STYLE must be one of ${SAGA_STYLES.join(', ')}, got: '${SAGA_STYLE}'`);
}

// Lv23: items → orders コールバック URL (choreography の双方向結合)。
// Lv23: items-to-orders callback URL (bidirectional coupling unique to choreography).
const ORDERS_EVENTS_URL = process.env.ORDERS_EVENTS_URL;

// Lv19 フォルトインジェクション。
// Lv19 fault injection: '' = off; 'after-first-write' | 'after-prepare-all' = inject fault.
const FAULT_POINT = process.env.FAULT_POINT ?? '';

// mud モードに外部サービス URL を組み合わせると「seam がない」という教訓が崩れる。
// Combining mud with external service URLs defeats the "no seam" lesson — fail fast.
if (ARCH === 'mud' && (ITEMS_SERVICE_URL || USERS_SERVICE_URL)) {
  throw new Error(
    'mud has no seam; it cannot be split by config. ' +
    'ITEMS_SERVICE_URL / USERS_SERVICE_URL are incompatible with ARCH=mud.',
  );
}

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
if (ARCH === 'hex' && ORDERS_CROSS_CONTEXT === 'join' && (ITEMS_SERVICE_URL || USERS_SERVICE_URL)) {
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
// Lv18 移行メトリクス: シャドウ読み取り不一致 / セカンダリ書き込みエラー。
// Lv18 migration metrics: shadow-read divergences / secondary write errors.
const shadowMismatch = new client.Counter({
  name: 'shadow_mismatch_total',
  help: 'Number of shadow-read divergences between primary and secondary items data (Lv18)',
  registers: [register],
});
const dualWriteSecondaryError = new client.Counter({
  name: 'dual_write_secondary_error_total',
  help: 'Number of best-effort secondary write errors during dual-write phase (Lv18)',
  registers: [register],
});

// In-process counters for /cache eyeball endpoint (mirrors Prometheus counters).
// オブジェクト経由で渡すことで itemsRoutes プラグインが参照を共有できる。
// Passed as object so itemsRoutes plugin shares the same reference.
const cacheCounters = { hits: 0, misses: 0 };

// Lv19 saga 回復ポーラー: saga モードかつ orders ロード時のみ起動。
// Lv19 saga recovery poller: started only in saga mode when orders are loaded.
let sagaPoller: ReturnType<typeof setInterval> | null = null;

// Lv22 outbox ポーラー: outbox モードかつ orders ロード時のみ起動。
// Lv22 outbox poller: started only in outbox mode when orders are loaded.
let outboxPoller: ReturnType<typeof setInterval> | null = null;

// Lv23 choreography ポーラー: saga+choreography 時に orders/items 双方で起動。
// Lv23 choreography pollers: started in saga+choreography mode for both orders and items sides.
let choreoOrdersPoller: ReturnType<typeof setInterval> | null = null;
let choreoItemsPoller: ReturnType<typeof setInterval> | null = null;

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

// --- ドメインルートの登録 ---
// --- Domain route registration ---

// Lv21 2PC 起動時リゾルバの thunk。composition root(loadOrders ブロック)で ordersRepo/itemsPort を
// クローズオーバーして格納し、main() が initDb() 完了後・listen 前に await 実行する。
// fire-and-forget にすると tx_journal 作成(initDb)とレースし初回起動でサイレント no-op になるため。
// Lv21 2PC startup-resolver thunk: captured in the composition root (loadOrders block) closing over
// ordersRepo/itemsPort; main() awaits it AFTER initDb() and BEFORE listen. Launching fire-and-forget
// would race tx_journal creation (initDb) and silently no-op recovery on first boot.
let run2pcResolver: (() => Promise<void>) | null = null;

if (ARCH === 'mud') {
  // mud モード: mud ルートだけを登録。ヘックス版ドメインは一切ロードしない。
  // mud mode: register only mud routes; hex domain code is never loaded.
  await app.register(mudRoutes, { writePool, INSTANCE });
} else {
  // hex モード（デフォルト）: SERVICE env で制御。既存動作を完全に保持。
  // hex mode (default): controlled by SERVICE env; existing behavior fully preserved.
  const loadItems = SERVICE === 'all' || SERVICE === 'items';
  const loadUsers = SERVICE === 'all' || SERVICE === 'users';
  const loadOrders = SERVICE === 'all' || SERVICE === 'orders';

  // コンポジションルート: pg リポジトリをインスタンス化し、ルートに注入する。
  // Composition root: instantiate pg repos and inject into routes.
  const itemsRepo = new PgItemsRepo(writePool, readPool);
  const usersRepo = new PgUsersRepo(writePool);

  if (loadItems) {
    await app.register(itemsRoutes, {
      itemsRepo,
      redis,
      INSTANCE,
      ASYNC_WRITE,
      WRITE_QUEUE_MAX,
      cacheHits,
      cacheMisses,
      cacheCounters,
    });

    // Lv23 choreography items ポーラー: choreo_outbox(items-db) の未配送を orders へ配送。
    // 起動条件: ORDERS_EVENTS_URL が設定されている場合のみ (items 側は ORDERS_EVENTS_URL の有無で判定)。
    // Lv23 choreography items poller: deliver StockReserved/StockRejected events to orders-service.
    // Start condition: ORDERS_EVENTS_URL is set (items side uses URL presence, not SAGA_STYLE).
    // SERVICE==='items' でのみ起動 (SERVICE=all で items も載る monolith に ORDERS_EVENTS_URL を渡しても
    // 自己ループ配送しないよう構造的にガード。env 衛生だけに頼らない)。
    if (ORDERS_EVENTS_URL && SERVICE === 'items') {
      const pollerItemsRepo = itemsRepo;
      const pollerOrdersEventsUrl = ORDERS_EVENTS_URL;

      choreoItemsPoller = setInterval(async () => {
        try {
          const rows = await pollerItemsRepo.claimUndeliveredChoreoOutbox(50);
          for (const r of rows) {
            try {
              // orders が consume する StockReserved/StockRejected を orders の受信 endpoint へ。
              const res = await fetch(`${pollerOrdersEventsUrl}/internal/choreo/stock-events`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ msgId: r.msg_id, eventType: r.event_type, payload: r.payload }),
                signal: AbortSignal.timeout(5000),
              });
              if (!res.ok) throw new Error(`orders choreo endpoint returned ${res.status}`);
              await pollerItemsRepo.markChoreoOutboxDelivered(r.msg_id);
            } catch (e) {
              app.log.warn({ msg_id: r.msg_id, err: e }, 'choreo items poller: delivery failed, will retry');
            }
          }
        } catch (e) {
          app.log.warn({ err: e }, 'choreo items poller: claim failed');
        }
      }, 2000);
    }
    // ORDERS_EVENTS_URL 未設定: items choreo poller は起動しない (警告不要 — 非 choreography 配備では正常)。
    // ORDERS_EVENTS_URL not set: items choreo poller stays off (no warning — normal for non-choreo deployments).
  }

  if (loadUsers) {
    await app.register(usersRoutes, { usersRepo, INSTANCE });
  }

  if (loadOrders) {
    const ordersRepo = new PgOrdersRepo(writePool);

    // Lv18 guard: join モードは itemsPort をバイパスするためデュアルライトと共存不可。
    // Lv18 guard: join mode bypasses itemsPort so it cannot mirror writes — fail fast.
    if (ORDERS_CROSS_CONTEXT === 'join' && ITEMS_SERVICE_URL) {
      throw new Error(
        'ORDERS_CROSS_CONTEXT=join bypasses itemsPort and cannot mirror writes to ITEMS_SERVICE_URL. ' +
        'Use ORDERS_CROSS_CONTEXT=port for Lv18 migration.',
      );
    }

    // items ポートの選択（Lv18 拡張）:
    // Items port selection (Lv18 extended):
    //   ITEMS_SERVICE_URL あり → DualWriteItemsAdapter (モードは ITEMS_MIGRATION_MODE で制御)
    //   なし → InProcess (デフォルト; Lv0-17 との後方互換を保持)
    let itemsPort: InProcessItemsAdapter | DualWriteItemsAdapter;
    let dualAdapter: DualWriteItemsAdapter | null = null;
    if (ITEMS_SERVICE_URL) {
      dualAdapter = new DualWriteItemsAdapter(
        new InProcessItemsAdapter(itemsRepo),
        new HttpItemsAdapter(ITEMS_SERVICE_URL),
        {
          initialMode: ITEMS_MIGRATION_MODE,
          onMismatch: () => { shadowMismatch.inc(); },
          onSecondaryError: () => { dualWriteSecondaryError.inc(); },
        },
      );
      itemsPort = dualAdapter;
    } else {
      itemsPort = new InProcessItemsAdapter(itemsRepo);
    }

    // users ポートの選択: USERS_SERVICE_URL が設定されていれば HTTP アダプタ、なければインプロセス。
    // Users port selection: HTTP adapter when USERS_SERVICE_URL is set, in-process otherwise.
    const usersPort = USERS_SERVICE_URL
      ? new HttpUsersAdapter(USERS_SERVICE_URL)
      : new InProcessUsersAdapter(usersRepo);

    await app.register(ordersRoutes, {
      ordersRepo,
      usersPort,
      itemsPort,
      mode: ORDERS_CROSS_CONTEXT,
      INSTANCE,
      // Lv19: port モードのみ有効。join モードでは txMode は無視。
      // Lv19: txMode only meaningful in port mode; ignored for join mode.
      txMode: ORDERS_CROSS_CONTEXT === 'port' ? ORDER_TX_MODE : 'none',
      // Lv23: saga + choreography 時のみ有効。
      // Lv23: only meaningful when txMode=saga.
      sagaStyle: SAGA_STYLE,
      fault: { faultPoint: FAULT_POINT },
    });

    // Lv18 管理エンドポイント: ランタイムで移行モードを切り替える（コンテナ再起動不要）。
    // Lv18 admin endpoints: flip migration mode at runtime — zero restart needed.
    // モノリス (SERVICE=all) かつ DualWriteAdapter が有効なときのみ登録。
    // Only registered when the dual-write adapter is active on the monolith (SERVICE=all).
    if (dualAdapter && loadItems) {
      const adapter = dualAdapter; // narrow for closure
      app.get('/admin/items-migration/mode', async () => ({ mode: adapter.getMode() }));
      app.post('/admin/items-migration/mode', async (req, reply) => {
        const body = req.body as { mode?: string };
        if (!body?.mode || !MIGRATION_MODES.includes(body.mode as MigrationMode)) {
          reply.code(400);
          return {
            error: `mode must be one of: ${MIGRATION_MODES.join(', ')}`,
            current: adapter.getMode(),
          };
        }
        adapter.setMode(body.mode as MigrationMode);
        return { mode: adapter.getMode() };
      });
    }

    // Lv19 saga 回復ポーラー: saga モード + orders ロード + ITEMS_SERVICE_URL がある場合に起動。
    // Lv19 saga recovery poller: start only when saga mode is active, orders are loaded, and
    // ITEMS_SERVICE_URL is set (so items-service release can be called).
    if (ORDER_TX_MODE === 'saga' && ITEMS_SERVICE_URL) {
      // itemsPort の型を narrowing して releaseStock を呼べるようにする。
      // Capture itemsPort in closure for poller; it satisfies ItemsPort (has releaseStock).
      const pollerItemsPort = itemsPort;
      const pollerOrdersRepo = ordersRepo;
      // スタック閾値: 3 秒以上 'reserved' のままの saga_log を「スタック」とみなす。
      // Threshold: saga_log rows stuck in 'reserved' for > 3s are considered orphaned.
      const STUCK_THRESHOLD_SECONDS = 3;

      sagaPoller = setInterval(async () => {
        try {
          // アトミックに 'compensating' に遷移させてから補償 — re-entrant safe。
          // Atomically transition to 'compensating' before compensating — re-entrant safe.
          const stuck = await pollerOrdersRepo.claimStuckSagaLogs(STUCK_THRESHOLD_SECONDS);
          for (const row of stuck) {
            try {
              // release は冪等 — 既に release 済みでも安全。
              // release is idempotent — safe to call even if already released.
              await pollerItemsPort.releaseStock(row.gid);
              await pollerOrdersRepo.updateSagaLogState(row.gid, 'compensated');
            } catch (err) {
              // 補償失敗は次回ポーリングで再試行 — state='compensating' のままで再 claim されない。
              // Compensation failure will be retried next interval — state stays 'compensating'
              // so claimStuckSagaLogs won't pick it up again (it only claims 'reserved' rows).
              app.log.warn({ gid: row.gid, err }, 'saga poller: compensation failed, will not retry (state=compensating)');
            }
          }
        } catch (err) {
          // DB 障害など — 次回ポーリングで再試行。
          // DB error — retry next interval.
          app.log.warn({ err }, 'saga poller: error querying stuck sagas');
        }
      }, 2000);
    }

    // Lv22 outbox ポーラー: outbox モード + orders ロード時に起動。
    // Lv22 outbox poller: start when outbox mode is active and orders are loaded.
    if (ORDER_TX_MODE === 'outbox') {
      const pollerOrdersRepo = ordersRepo;
      const pollerItemsPort = itemsPort;

      outboxPoller = setInterval(async () => {
        try {
          const rows = await pollerOrdersRepo.claimUndeliveredOutbox(50);
          for (const r of rows) {
            try {
              await pollerItemsPort.deliverDecrement(r.msg_id, r.item_id, r.qty);
              // FAULT POINT: after-deliver-before-mark — 配送後・mark前に throw (クラッシュ相当)。
              // 再配送されるが receiver が dedup するので二重にならない。
              // FAULT POINT: after-deliver-before-mark — throw after delivery, before mark (crash sim).
              // Re-delivery is safe: receiver deduplicates via processed_messages.
              if (FAULT_POINT === 'after-deliver-before-mark') throw new Error('[fault] after-deliver-before-mark');
              await pollerOrdersRepo.markOutboxDelivered(r.msg_id);
            } catch (e) {
              app.log.warn({ msg_id: r.msg_id, err: e }, 'outbox poller: delivery failed, will retry');
            }
          }
        } catch (e) {
          app.log.warn({ err: e }, 'outbox poller: claim failed');
        }
      }, 2000);
    }

    // Lv23 choreography orders ポーラー: choreo_outbox(orders-db) の未配送を items へ配送。
    // Lv23 choreography orders poller: deliver undelivered OrderCreated events to items-service.
    if (ORDER_TX_MODE === 'saga' && SAGA_STYLE === 'choreography' && ITEMS_SERVICE_URL) {
      const pollerOrdersRepo = ordersRepo;
      const pollerItemsUrl = ITEMS_SERVICE_URL;

      choreoOrdersPoller = setInterval(async () => {
        try {
          const rows = await pollerOrdersRepo.claimUndeliveredChoreoOutbox(50);
          for (const r of rows) {
            try {
              const payload = r.payload as { orderId: number; itemId: number; qty: number };
              // items が consume する OrderCreated を items の受信 endpoint へ。
              const res = await fetch(`${pollerItemsUrl}/internal/choreo/order-events`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ msgId: r.msg_id, eventType: r.event_type, payload }),
                signal: AbortSignal.timeout(5000),
              });
              if (!res.ok) throw new Error(`items choreo endpoint returned ${res.status}`);
              await pollerOrdersRepo.markChoreoOutboxDelivered(r.msg_id);
            } catch (e) {
              app.log.warn({ msg_id: r.msg_id, err: e }, 'choreo orders poller: delivery failed, will retry');
            }
          }
        } catch (e) {
          app.log.warn({ err: e }, 'choreo orders poller: claim failed');
        }
      }, 2000);
    }

    // Lv21 2PC 起動時リゾルバ: 2pc モード + orders ロード時のみ。ここでは thunk を格納するだけで、
    // 実行は main() が initDb()(tx_journal 作成)完了後・listen 前に await する(schema レース回避 +
    // 将来 coordinator を多重化しても live traffic の in-flight 2pc とレースしない)。
    // Lv21 2PC startup resolver: only in 2pc mode with orders loaded. Store the thunk here; main()
    // awaits it AFTER initDb() (tx_journal created) and BEFORE listen (avoids the schema race and,
    // if the coordinator is ever replicated, the live-traffic in-flight-2pc race).
    if (ORDER_TX_MODE === '2pc') {
      const resolverOrdersRepo = ordersRepo;
      const resolverItemsPort = itemsPort;
      run2pcResolver = () => resolveInDoubt(resolverOrdersRepo, resolverItemsPort, app.log);
    }
  }
}

// Lv21 2PC 起動時リゾルバ: インダウト prepared xact を決定ジャーナルに基づいて自動解決。
// Lv21 2PC startup resolver: auto-resolve in-doubt prepared txns using the decision journal.
//
// correctness の核心: items 未達なら deleteJournal しない (deferred)。
// ジャーナルを先に消すと次回起動で「決定なし→abort」と誤判定し orders(commit済) と items(rollback)
// が食い違う。items が到達可能になった次回起動で再度解決する。
// Correctness core: do NOT deleteJournal when items is unreachable (deferred).
// Deleting the journal early would cause the next boot to misread "no decision → abort", leaving
// orders committed and items rolled back — a split-brain. Defer until both sides are confirmed.
async function resolveInDoubt(
  ordersRepo: import('./domains/orders/infra/pg-orders-repo.js').PgOrdersRepo,
  itemsPort: import('./ports/items-port.js').InProcessItemsAdapter | import('./ports/items-port.js').DualWriteItemsAdapter,
  log: import('fastify').FastifyBaseLogger,
): Promise<void> {
  const localPrepared = new Set(await ordersRepo.listPreparedGids());
  let itemsReachable = true;
  let itemsPrepared = new Set<string>();
  try {
    itemsPrepared = new Set(await itemsPort.listPreparedTx());
  } catch (e) {
    itemsReachable = false;
    log.warn({ err: e }, 'resolver: items unreachable; orders-side only this pass');
  }
  const commits = new Set(await ordersRepo.listJournalCommits());
  const all = new Set([...localPrepared, ...itemsPrepared, ...commits]);
  let committed = 0, aborted = 0, deferred = 0;
  for (const gid of all) {
    try {
      if (commits.has(gid)) {
        // 決定=COMMIT → 両者を commit (冪等)。
        // Decision=COMMIT → commit both sides (idempotent).
        if (localPrepared.has(gid)) await ordersRepo.commitPrepared(gid);
        if (itemsReachable) {
          if (itemsPrepared.has(gid)) await itemsPort.commitTx(gid);
          // 両者 done を確認できた時だけジャーナルを消す。
          // Delete journal only after confirming both sides are done.
          await ordersRepo.deleteJournal(gid);
          committed++;
        } else {
          // items 未達 → ジャーナルを残し次回起動へ持ち越す。
          // items unreachable → keep journal; defer to next boot.
          deferred++;
        }
      } else {
        // 決定なし → ABORT (両者 rollback・冪等)。ジャーナル行は無いので消す物なし。
        // No decision → ABORT (rollback both, idempotent). No journal row to delete.
        if (localPrepared.has(gid)) await ordersRepo.rollbackPrepared(gid);
        if (itemsReachable && itemsPrepared.has(gid)) await itemsPort.rollbackTx(gid);
        aborted++;
      }
    } catch (e) {
      log.warn({ gid, err: e }, 'resolver: failed to resolve gid, will retry next restart');
    }
  }
  log.info({ committed, aborted, deferred, scanned: all.size }, '2pc resolver done');
}

// initDb は ARCH ごとに異なるスキーマを初期化する。
// initDb initializes the schema appropriate for the current ARCH.
async function initDb(): Promise<void> {
  if (ARCH === 'mud') {
    // mud モード: 3 ドメインを FK で繋いだ一体型スキーマ。
    // mud mode: monolithic schema with cross-domain FKs.
    await mudInitSchema(writePool);
    if (SEED_DEMO) await mudSeed(writePool);
    return;
  }

  // hex モード: SERVICE env で制御された個別スキーマ初期化（既存動作そのまま）。
  // hex mode: per-domain schema init controlled by SERVICE env (existing behavior).
  const loadItems = SERVICE === 'all' || SERVICE === 'items';
  const loadUsers = SERVICE === 'all' || SERVICE === 'users';
  const loadOrders = SERVICE === 'all' || SERVICE === 'orders';

  if (loadItems) {
    const repo = new PgItemsRepo(writePool, readPool);
    await repo.initSchema();
    // SEED_DEMO が設定されている場合のみシード実行（旧ステージとの後方互換を守る）。
    // Seed only when SEED_DEMO is set — preserves empty-list baseline for old stages.
    if (SEED_DEMO) await repo.seed();
  }
  if (loadUsers) {
    const repo = new PgUsersRepo(writePool);
    await repo.initSchema();
    if (SEED_DEMO) await repo.seed();
  }
  if (loadOrders) {
    const repo = new PgOrdersRepo(writePool);
    await repo.initSchema();
    // Lv19 saga: saga_log は orders ドメインが所有するテーブル。
    // ORDER_TX_MODE に関係なく orders がロードされれば作成 — コーディネーターのモード設定と分離。
    // Lv19 saga: saga_log is owned by the orders domain.
    // Create whenever orders is loaded, regardless of ORDER_TX_MODE — decouple from coordinator config.
    await repo.initSagaSchema();
    // Lv21 2PC 決定ジャーナル: saga_log と同様に常に作成 (MODE 非依存)。
    // Lv21 2PC decision journal: always create alongside saga_log (mode-independent).
    await repo.initTxJournalSchema();
    // Lv22 outbox: outbox テーブルを常に作成 (MODE 非依存)。
    // Lv22 outbox: always create outbox table (mode-independent).
    await repo.initOutboxSchema();
    // Lv23 choreography: choreo_outbox + processed_messages(orders-db) を常に作成 (MODE 非依存)。
    // Lv23 choreography: always create choreo_outbox + orders-db processed_messages (mode-independent).
    await repo.initChoreoSchema();
    await repo.initOrdersInboxSchema();
  }
  if (loadItems) {
    // Lv19 saga: reservations は items ドメインが所有するテーブル。
    // items-service (SERVICE=items) は ORDER_TX_MODE を持たないが、
    // saga プロトコルの参加者として reservations を準備しておく必要がある。
    // Lv19 saga: reservations is owned by the items domain.
    // items-service (SERVICE=items) has no ORDER_TX_MODE; it must still provision reservations
    // so it is a ready protocol participant when the coordinator calls /internal/reserve.
    const repo = new PgItemsRepo(writePool, readPool);
    await repo.initSagaSchema();
    // Lv22 outbox: processed_messages は items ドメインが所有するテーブル (冪等 receiver 用)。
    // Lv22 outbox: processed_messages is owned by the items domain (idempotent receiver inbox).
    await repo.initInboxSchema();
    // Lv23 choreography: choreo_outbox を items-db にも作成 (MODE 非依存)。
    // Lv23 choreography: create choreo_outbox on items-db too (mode-independent).
    await repo.initChoreoSchema();
  }
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
  // items ドメインをロードしているときだけレプリカ可視性チェックを行う (mud も items を持つ)。
  // Replica visibility check only when items is present (mud always has items; hex checks SERVICE).
  const itemsVisible = ARCH === 'mud' || SERVICE === 'all' || SERVICE === 'items';
  if (itemsVisible) {
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

  // Lv21 2PC 起動時リゾルバを serve 前に await 完走させる。initDb() 済(tx_journal あり)なので
  // schema レースが無く、listen 前なので live traffic の in-flight 2pc とも競合しない。
  // Run the 2PC startup resolver to completion BEFORE serving: initDb() has created tx_journal
  // (no schema race) and listen hasn't started (no live-traffic race).
  if (run2pcResolver) {
    try {
      await run2pcResolver();
    } catch (err) {
      app.log.warn({ err }, '2pc resolver: unexpected error');
    }
  }

  await app.listen({ port: PORT, host: '0.0.0.0' });
}

// Graceful shutdown so k8s rolling updates / SIGTERM drain cleanly.
for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, async () => {
    app.log.info(`${sig} received, shutting down...`);
    // Lv19 saga ポーラーを停止。
    // Lv19: stop the saga recovery poller before closing connections.
    if (sagaPoller !== null) clearInterval(sagaPoller);
    // Lv22 outbox ポーラーを停止。
    // Lv22: stop the outbox poller before closing connections.
    if (outboxPoller !== null) clearInterval(outboxPoller);
    // Lv23 choreography ポーラーを停止。
    // Lv23: stop choreography pollers before closing connections.
    if (choreoOrdersPoller !== null) clearInterval(choreoOrdersPoller);
    if (choreoItemsPoller !== null) clearInterval(choreoItemsPoller);
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
