import Fastify from 'fastify';
import os from 'node:os';
import pg from 'pg';
import client from 'prom-client';

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
  const { rows } = await readPool.query(
    'SELECT id, name, created_at FROM items ORDER BY id DESC LIMIT 100',
  );
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
  reply.code(201);
  return { instance: INSTANCE, item: rows[0] };
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
  endpoints: ['/health', '/ready', '/metrics', '/work?ms=&cpu=', 'GET/POST /items', '/replication'],
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
      await Promise.all([writePool.end(), readPool.end()]);
    } finally {
      process.exit(0);
    }
  });
}

main().catch((err) => {
  app.log.error(err);
  process.exit(1);
});
