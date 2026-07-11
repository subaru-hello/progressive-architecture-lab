// consumer.ts — Lv8 write-behind consumer（別プロセス）
//
// Redis Stream `items:writes` から enqueue されたメッセージを読み出し、
// Postgres に INSERT してから XACK+XDEL する。
// at-least-once 保証: INSERT 失敗時は XACK/XDEL しない（PEL に残して再配送）。
// 重複 INSERT しうる（冪等化はしない）= これは教材。
//
// 起動:
//   node dist/consumer.js
//   環境変数: DATABASE_URL, REDIS_URL, CONSUMER_CONCURRENCY(default 10),
//             CONSUMER_PORT(default 3001), INSTANCE_ID

import Fastify from 'fastify';
import os from 'node:os';
import pg from 'pg';
import client from 'prom-client';
import { Redis } from 'ioredis';

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
const REDIS_URL = process.env.REDIS_URL;

if (!DATABASE_URL) throw new Error('DATABASE_URL is required');
if (!REDIS_URL) throw new Error('REDIS_URL is required');

const CONSUMER_CONCURRENCY = Math.max(1, Math.trunc(Number(process.env.CONSUMER_CONCURRENCY ?? 10)));
const CONSUMER_PORT = Number(process.env.CONSUMER_PORT ?? 3001);
const INSTANCE = process.env.INSTANCE_ID ?? os.hostname();

// Consumer name must be unique per process so multiple replicas don't clash in the group.
const CONSUMER_NAME = `consumer-${os.hostname()}-${process.pid}`;
const STREAM_NAME = 'items:writes';
const GROUP_NAME = 'writers';
const CACHE_KEY = 'items:latest100';
// stale PEL 回収: この時間（ms）以上 ACK されていないエントリを XAUTOCLAIM で奪う。
// worst-case のバッチ処理時間より十分大きく取る。短いと、まだ処理中（または INSERT 済み
// XACK 前）のエントリを XAUTOCLAIM が奪い返して再 INSERT し、定常状態でも重複が増える。
const AUTOCLAIM_IDLE_MS = 60000;

const writePool = new Pool({ connectionString: DATABASE_URL, max: 5 });

const redis = new Redis(REDIS_URL, {
  lazyConnect: true,
  maxRetriesPerRequest: 1,
  connectTimeout: 2000,
  enableOfflineQueue: false,
});
redis.on('error', () => {
  // Suppress unhandled error events.
});

// --- Prometheus metrics (consumer 独自 registry) ---
const register = new client.Registry();
register.setDefaultLabels({ instance: INSTANCE });
client.collectDefaultMetrics({ register });

const commitLagHistogram = new client.Histogram({
  name: 'write_queue_commit_lag_seconds',
  help: 'Time from Stream enqueue to Postgres commit (seconds)',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30],
  registers: [register],
});

const writeQueueDepth = new client.Gauge({
  name: 'write_queue_depth',
  help: 'Number of pending entries in the write-behind Redis Stream (items:writes)',
  registers: [register],
});

// ループ継続フラグ: SIGINT/SIGTERM で false にしてループを止める。
let running = true;

// 起動時に Consumer Group を作成する（BUSYGROUP エラーは無視）。
async function ensureGroup(): Promise<void> {
  try {
    // MKSTREAM: Stream が存在しない場合は作成する。
    await redis.xgroup('CREATE', STREAM_NAME, GROUP_NAME, '$', 'MKSTREAM');
  } catch (err: unknown) {
    if (err instanceof Error && err.message.includes('BUSYGROUP')) {
      // グループ既存は正常。
    } else {
      throw err;
    }
  }
}

// DB テーブルが見えるまで待つ（consumer 単独起動時の安全策）。
// server.ts が CREATE TABLE を行う前提だが、起動順が逆になることもある。
async function waitForTable(): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      const { rows } = await writePool.query(`SELECT to_regclass('public.items') AS t`);
      if (rows[0].t) return;
    } catch {
      // DB 未接続ならリトライ。
    }
    if (attempt >= 30) throw new Error('items table not visible after 30 attempts');
    console.warn(`[consumer] waiting for items table (attempt ${attempt}/30)...`);
    await new Promise((r) => setTimeout(r, 2000));
  }
}

// 1件の Stream entry を処理する。
// INSERT 成功 → XACK + XDEL + cache DEL + lag observe。
// INSERT 失敗 → XACK/XDEL しない（PEL に残す）。
async function processEntry(id: string, fields: Record<string, string>): Promise<void> {
  const { name, enqueued_at } = fields;
  if (!name) {
    // 壊れたエントリは捨てる（XDEL→XACK 順、下記の理由と同じ）。redis blip は握る。
    console.warn(`[consumer] malformed entry ${id}: missing name, discarding`);
    await ackAndDelete(id);
    return;
  }

  try {
    await writePool.query('INSERT INTO items(name) VALUES($1)', [name]);
  } catch (err) {
    console.error(`[consumer] INSERT failed for entry ${id}:`, err);
    // PEL に残す（再配送）。
    return;
  }

  // commit 成功 → XDEL してから XACK する。
  // 順序が重要: XACK を先にすると PEL から外れ、XACK↔XDEL 間で consumer が死ぬと
  // エントリは Stream に残ったまま「ACK 済み」になり、XREADGROUP('>') も XAUTOCLAIM
  // (PEL のみ走査) も二度と拾えず、XLEN が恒久的に膨らむ → depth/背圧が壊れる。
  // XDEL を先にすれば、XDEL↔XACK 間で死んでも XAUTOCLAIM が「削除済み PEL」として
  // 自動 evict するので depth 不変量が自己修復する（redis 7 で確認済）。
  await ackAndDelete(id);

  // enqueue からの lag を Histogram に記録。
  if (enqueued_at) {
    const lagMs = Date.now() - Number(enqueued_at);
    commitLagHistogram.observe(lagMs / 1000);
  }

  // cache 無効化（ベストエフォート）。commit した consumer が行う（API 側では DEL しない）。
  try {
    await redis.del(CACHE_KEY);
  } catch {
    // ignore
  }
}

// XDEL → XACK の順で Stream からエントリを片付ける。redis の一過性エラーは握って
// 続行する（例外を consumeLoop まで伝播させるとループごと死んで restart ループに陥るため。
// 最悪ケースは再配送＝at-least-once の範囲内）。
async function ackAndDelete(id: string): Promise<void> {
  try {
    await redis.xdel(STREAM_NAME, id);
    await redis.xack(STREAM_NAME, GROUP_NAME, id);
  } catch (err) {
    console.warn(`[consumer] xdel/xack failed for ${id} (will be redelivered):`, err);
  }
}

// ioredis の xreadgroup は以下の型で返す:
//   Array<[streamName: string, entries: Array<[id: string, fields: string[]]>]> | null
type XReadGroupResult = Array<[string, Array<[string, string[]]>]> | null;

// メインポーリングループ。
async function consumeLoop(): Promise<void> {
  while (running) {
    let result: XReadGroupResult;
    try {
      result = (await redis.xreadgroup(
        'GROUP', GROUP_NAME, CONSUMER_NAME,
        'COUNT', String(CONSUMER_CONCURRENCY),
        'BLOCK', '1000',
        'STREAMS', STREAM_NAME, '>',
      )) as XReadGroupResult;
    } catch {
      // Redis 一時障害: 少し待ってリトライ。
      await new Promise((r) => setTimeout(r, 500));
      continue;
    }

    if (!result || result.length === 0) continue;

    const [, entries] = result[0];
    for (const [id, rawFields] of entries) {
      // ioredis は fields を [key, value, key, value, ...] のフラット配列で返す。
      const fields: Record<string, string> = {};
      for (let i = 0; i + 1 < rawFields.length; i += 2) {
        fields[rawFields[i]] = rawFields[i + 1];
      }
      await processEntry(id, fields);
    }
  }
}

// stale PEL 回収ループ（10s 毎に XAUTOCLAIM で idle なエントリを自分に移譲して再 INSERT）。
// XAUTOCLAIM は Redis 6.2+ の機能。ioredis では redis.call() 経由で呼ぶ。
// 戻り値: ["0-0", [[id, fields], ...], [...deleted]]
async function autoclaimLoop(): Promise<void> {
  while (running) {
    await new Promise((r) => setTimeout(r, 10000));
    if (!running) break;

    try {
      const result = await redis.call(
        'XAUTOCLAIM',
        STREAM_NAME,
        GROUP_NAME,
        CONSUMER_NAME,
        String(AUTOCLAIM_IDLE_MS),
        '0-0',
        'COUNT',
        '100',
      ) as [string, Array<[string, string[]]>, string[]];

      const [, entries] = result;
      if (!entries || entries.length === 0) continue;

      for (const [id, rawFields] of entries) {
        const fields: Record<string, string> = {};
        for (let i = 0; i + 1 < rawFields.length; i += 2) {
          fields[rawFields[i]] = rawFields[i + 1];
        }
        await processEntry(id, fields);
      }
    } catch (err) {
      // XAUTOCLAIM が使えない Redis バージョン（<6.2）でもエラーを握って続行。
      console.warn('[consumer] XAUTOCLAIM failed (Redis <6.2?):', err);
    }
  }
}

// 簡易 metrics endpoint（CONSUMER_PORT で公開）。
const metricsApp = Fastify({ logger: false });

metricsApp.get('/metrics', async (_req, reply) => {
  // XLEN で現在の Stream depth を取得して Gauge に反映。
  try {
    const depth = await redis.xlen(STREAM_NAME);
    writeQueueDepth.set(depth);
  } catch {
    // ignore
  }
  reply.header('Content-Type', register.contentType);
  return register.metrics();
});

metricsApp.get('/health', async () => ({ status: 'ok', instance: INSTANCE }));

// consumeLoop の Promise を掴んでおき、shutdown で in-flight バッチの完了を待つ。
let loopPromise: Promise<void> | null = null;

async function main(): Promise<void> {
  // lazyConnect: true + enableOfflineQueue: false の組み合わせでは、接続確立前に
  // コマンドを発行するとエラーになる。明示的に connect() して接続を先に確立する。
  // Redis が本当に未達なら fail-fast する（consumer にとって Redis は本線。繋がらない
  // まま生かすと空回りするだけ）。「既に connecting/connected」の良性 reject だけ握る。
  try {
    await redis.connect();
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (!/already (connect|connecting|connected)/i.test(msg)) {
      throw new Error(`[consumer] Redis connect failed: ${msg}`);
    }
  }

  await waitForTable();
  await ensureGroup();

  console.log(`[consumer] ${CONSUMER_NAME} starting on group=${GROUP_NAME} stream=${STREAM_NAME}`);

  // metrics server 起動。
  await metricsApp.listen({ port: CONSUMER_PORT, host: '0.0.0.0' });
  console.log(`[consumer] metrics available at http://0.0.0.0:${CONSUMER_PORT}/metrics`);

  // autoclaim と consume を並行起動。
  autoclaimLoop().catch((err) => console.error('[consumer] autoclaimLoop error:', err));
  loopPromise = consumeLoop();
  await loopPromise;
}

// Graceful shutdown: running=false でループに停止を指示し、**現在のバッチが XDEL/XACK まで
// 完了するのを待ってから**接続を閉じる。待たずに閉じると INSERT 済み↔XACK 前の宙吊りが
// 増える（再配送で重複）。未読み出しの Stream entry は残り、再起動で継続する（AOF 有効時）。
for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, async () => {
    console.log(`[consumer] ${sig} received, draining...`);
    running = false;
    try {
      if (loopPromise) await loopPromise; // 現在の BLOCK/バッチが抜けるまで待つ（最大 ~1s + 1バッチ）
      await metricsApp.close();
      await Promise.all([writePool.end(), redis.quit()]);
    } finally {
      process.exit(0);
    }
  });
}

main().catch((err) => {
  console.error('[consumer] fatal:', err);
  process.exit(1);
});
