// consumer.ts — Lv8/Lv9 write-behind consumer（別プロセス）
//
// Redis Stream `items:writes` から enqueue されたメッセージを読み出し、
// Postgres に INSERT してから XACK+XDEL する。
// at-least-once 保証: INSERT 失敗時は XACK/XDEL しない（PEL に残して再配送）。
// 重複 INSERT しうる（冪等化はしない）= これは教材。
//
// 起動:
//   node dist/consumer.js
//   環境変数: DATABASE_URL, REDIS_URL, CONSUMER_CONCURRENCY(default 10),
//             CONSUMER_PORT(default 3001), INSTANCE_ID,
//             BATCH_WRITES(default off; "1" でバッチ経路),
//             WRITE_BATCH_SIZE(default 100; BATCH_WRITES=1 時の XREADGROUP COUNT),
//             SYNC_COMMIT_OFF(default off; "1" で write セッションの synchronous_commit=off),
//             COMMITTERS(default 1; >1 で並列 committer coroutine を N 本回す = Lv11),
//             COMMIT_DELAY_US(default 0; >0 で write セッションに commit_delay マイクロ秒を設定),
//             COMMIT_SIBLINGS(default 5; commit_delay を発火させる同時アクティブ tx の下限)

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

// BATCH_WRITES=1 のときバッチ経路（multi-row INSERT）を使う。未設定/0 のとき Lv8 と同一挙動。
const BATCH_WRITES = process.env.BATCH_WRITES === '1';
// BATCH_WRITES on のときの XREADGROUP COUNT。off のときは CONSUMER_CONCURRENCY を使う（後方互換）。
const WRITE_BATCH_SIZE = Math.max(1, Math.trunc(Number(process.env.WRITE_BATCH_SIZE ?? 100)));
// SYNC_COMMIT_OFF=1 で write セッションの synchronous_commit を off にする（Lv10）。未設定なら
// Lv9 と同一（既定 on）。commit が WAL の fsync 完了を待たずに返る＝commit「コスト」を削る。
// 代償: OS/DB クラッシュで直近数百 ms 分の commit 済みトランザクションが失われうる（WAL 未 flush）。
// ※ ロスト範囲は wal_writer_delay（既定 200ms）程度。データ「破損」ではなく最新分の消失。
const SYNC_COMMIT_OFF = process.env.SYNC_COMMIT_OFF === '1';

// COMMITTERS>1 で並列 committer を N 本回す（Lv11）。既定 1 = Lv9/Lv10 と同一（consumeLoop 1本）。
// Lv9/10 の consumeLoop は processBatch を await してから次を読むので commit は常に 1本ずつ = 単一
// committer。commit_delay（group commit）は「commit record を書いた瞬間に他に commit_siblings 本の
// アクティブ tx がある」ときだけ待ちを入れて fsync を束ねる仕組みなので、単一 committer では原理的に
// 発火しない（Lv10 の教訓）。N 本の committer が別々の pg コネクションで同時に commit へ到達して初めて
// group commit が効く。各 coroutine は同一 CONSUMER_NAME で XREADGROUP('>') するが、Redis は 1 entry を
// group 内の 1 呼び出しにしか配らない（同一 consumer への並行呼び出しでも disjoint）ので重複配送しない。
// ※ 各 committer には専用の read コネクション（redis.duplicate()）を割り当てる。単一 ioredis コネクション
//   共有だと 1 本の BLOCK 付き XREADGROUP がコマンドキュー（厳密 FIFO）先頭で最大 1s 居座り、他の committer
//   の read も INSERT 後の xack すら送信できず直列化して「同時 commit」が崩れるため（QA 指摘）。
// NaN ガード: 非数値を渡すと Math.max(1, NaN)=NaN → Array.from({length:NaN}) が空配列 → committer が 1 本も
// 起動せず drain がサイレント停止する。COMMITTERS だけは 0 本起動が致命的なので明示的に 1 へフォールバック。
const COMMITTERS_RAW = Math.trunc(Number(process.env.COMMITTERS ?? 1));
const COMMITTERS = Number.isFinite(COMMITTERS_RAW) ? Math.max(1, COMMITTERS_RAW) : 1;
// COMMIT_DELAY_US>0 で write セッションに commit_delay=<us> を設定（Lv11）。既定 0 = 無効。
// 注意: commit_delay は synchronous_commit=on のときだけ意味を持つ（off だと backend は fsync を
// 待たないので束ねる対象が無い）。Lv11 は durability を保ったまま fsync を amortize する実験なので
// SYNC_COMMIT_OFF は付けずに使う。
const COMMIT_DELAY_US = Math.max(0, Math.trunc(Number(process.env.COMMIT_DELAY_US ?? 0)));
// commit_delay を発火させる同時アクティブ tx の下限（PostgreSQL の commit_siblings）。
const COMMIT_SIBLINGS = Math.max(0, Math.trunc(Number(process.env.COMMIT_SIBLINGS ?? 5)));

// Consumer name must be unique per process so multiple replicas don't clash in the group.
const CONSUMER_NAME = `consumer-${os.hostname()}-${process.pid}`;
const STREAM_NAME = 'items:writes';
const GROUP_NAME = 'writers';
const CACHE_KEY = 'items:latest100';
// stale PEL 回収: この時間（ms）以上 ACK されていないエントリを XAUTOCLAIM で奪う。
// worst-case のバッチ処理時間より十分大きく取る。短いと、まだ処理中（または INSERT 済み
// XACK 前）のエントリを XAUTOCLAIM が奪い返して再 INSERT し、定常状態でも重複が増える。
const AUTOCLAIM_IDLE_MS = 60000;

// SYNC_COMMIT_OFF のときは libpq の startup option で接続時点から synchronous_commit=off を効かせる。
// `options: '-c synchronous_commit=off'` は各物理接続の確立時にサーバへ渡るので、pool.on('connect')
// で SET を撃つ方式のような「SET 完了前に最初のクエリが走る」レースが原理的に無い。
// write 経路（この consumer の writePool）だけに効き、read 経路（api 側 readPool）には影響しない。
// libpq の startup option（`-c key=val`）を接続確立時にサーバへ渡す。pool.on('connect') で SET を
// 撃つ方式のような「SET 完了前に最初のクエリが走る」レースが原理的に無く、write 経路（この writePool）
// だけに効く。SYNC_COMMIT_OFF（Lv10）と COMMIT_DELAY_US（Lv11）を 1つの options 文字列に合成する。
const startupOptions: string[] = [];
if (SYNC_COMMIT_OFF) startupOptions.push('-c synchronous_commit=off');
if (COMMIT_DELAY_US > 0) {
  startupOptions.push(`-c commit_delay=${COMMIT_DELAY_US}`);
  startupOptions.push(`-c commit_siblings=${COMMIT_SIBLINGS}`);
}
const writePool = new Pool({
  connectionString: DATABASE_URL,
  // 並列 committer が同時に別コネクションで commit へ到達できるよう、COMMITTERS 本 + 予備を張れる
  // だけの max を確保する（既定 1 のときは従来どおり 5）。
  max: Math.max(5, COMMITTERS + 1),
  ...(startupOptions.length > 0 ? { options: startupOptions.join(' ') } : {}),
});

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

// バッチ経路のみ: 1 commit あたり束ねた valid 行数を観測する（N-per-commit 可視化用）。
const batchSizeHistogram = new client.Histogram({
  name: 'write_queue_batch_size',
  help: 'Number of valid rows bundled into a single multi-row INSERT commit (batch path only)',
  buckets: [1, 5, 10, 25, 50, 100, 200, 500],
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

// バッチ経路: entries（生 ioredis 形式）を parse して 1本の multi-row INSERT にまとめ、
// XDEL/XACK を pipeline で一括する。consumeLoop / autoclaimLoop の両方から呼ばれる。
//
// 失敗時リスク（poison batch）:
//   valid entries のうち 1件でも INSERT を落とすと、バッチ全体が XDEL/XACK されないまま
//   PEL に残り、同一バッチが XAUTOCLAIM によって無限に再配送される可能性がある。
//   name は TEXT 単純 INSERT なので、実際は DB ダウン時以外ほぼ発生しない。
//   増幅係数 = batch size: per-row（Lv8）なら poison 1件の巻き添えは自身のみだが、batch では
//   1回の INSERT 失敗が同一バッチ最大 WRITE_BATCH_SIZE 件の正常行の再 INSERT（重複）を誘発する。
//   DB デッドロック / too many connections / statement timeout は「DB ダウン時のみ」の想定より
//   現実に起きうる点に注意（バッチ化が耐障害性とトレードオフする面）。
//   poison 対策が必要な場合はエントリ単位のリトライ or dead-letter queue を別途実装すること。
async function processBatch(rawEntries: Array<[string, string[]]>): Promise<void> {
  // 1. parse して valid / malformed に仕分け。
  type Entry = { id: string; name: string; enqueued_at: string | undefined };
  const valid: Entry[] = [];
  const malformedIds: string[] = [];

  for (const [id, rawFields] of rawEntries) {
    const fields: Record<string, string> = {};
    for (let i = 0; i + 1 < rawFields.length; i += 2) {
      fields[rawFields[i]] = rawFields[i + 1];
    }
    if (!fields['name']) {
      console.warn(`[consumer] malformed entry ${id}: missing name, discarding`);
      malformedIds.push(id);
    } else {
      valid.push({ id, name: fields['name'], enqueued_at: fields['enqueued_at'] });
    }
  }

  // 2. malformed は個別に XDEL→XACK して破棄（バッチをブロックさせない）。
  for (const id of malformedIds) {
    await ackAndDelete(id);
  }

  if (valid.length === 0) {
    console.log('[consumer] processBatch: no valid entries in this batch, skipping INSERT');
    return;
  }

  // 3. multi-row INSERT: INSERT INTO items(name) VALUES ($1),($2),...
  //    1 query = 1 commit でコミット回数を N→1 に削減する。
  const placeholders = valid.map((_, i) => `($${i + 1})`).join(',');
  const names = valid.map((e) => e.name);
  try {
    await writePool.query(`INSERT INTO items(name) VALUES ${placeholders}`, names);
  } catch (err) {
    // INSERT 失敗時: XDEL/XACK を一切しない → バッチ全体が PEL に残り再配送（at-least-once）。
    // poison batch リスクについては関数冒頭のコメントを参照。
    console.error(`[consumer] batch INSERT failed (${valid.length} rows), will be redelivered:`, err);
    return;
  }

  // 4. INSERT 成功 → 全 valid id を XDEL（全件）→ XACK（全件）の順で pipeline 一括処理。
  //    XDEL を先にする理由は processEntry / ackAndDelete のコメントと同一（depth 不変量の
  //    自己修復）。pipeline で往復を削減しつつ、XDEL 全件が XACK 全件より必ず先になるよう
  //    コマンド順序を維持する。
  try {
    const pipe = redis.pipeline();
    for (const { id } of valid) {
      pipe.xdel(STREAM_NAME, id);
    }
    for (const { id } of valid) {
      pipe.xack(STREAM_NAME, GROUP_NAME, id);
    }
    // ioredis の pipeline.exec() は個別コマンドのエラーでは reject せず、[err, result] の
    // 配列で resolve する（reject は接続断など transport レベルのみ）。だから戻り値を走査して
    // 個別 XDEL/XACK 失敗を検出しないと、「XACK 済みだが XDEL 未」= XLEN 恒久リーク（この
    // consumer の肝である depth 不変量の破れ）や、XDEL/XACK 漏れによる 60s 後の重複再 INSERT が
    // warn すら出ず完全に不可視で進む。per-row 経路（ackAndDelete）は逐次 await で各コマンドの
    // エラーが catch されるので、この可視化は batch 経路だけに必要。
    const results = await pipe.exec();
    let failed = 0;
    for (const [err] of results ?? []) {
      if (err) failed++;
    }
    if (failed > 0) {
      console.warn(
        `[consumer] pipeline xdel/xack: ${failed}/${results?.length ?? 0} commands failed ` +
          `(entries may be redelivered → duplicate INSERT, or XLEN leak)`,
      );
    }
  } catch (err) {
    console.warn('[consumer] pipeline xdel/xack failed at transport level (entries may be redelivered):', err);
  }

  // 5. lag を observe + batch size を record。
  const now = Date.now();
  for (const { enqueued_at } of valid) {
    if (enqueued_at) {
      const lagMs = now - Number(enqueued_at);
      commitLagHistogram.observe(lagMs / 1000);
    }
  }
  batchSizeHistogram.observe(valid.length);

  // 6. cache DEL を 1回だけ（ベストエフォート）。
  try {
    await redis.del(CACHE_KEY);
  } catch {
    // ignore
  }
}

// ioredis の xreadgroup は以下の型で返す:
//   Array<[streamName: string, entries: Array<[id: string, fields: string[]]>]> | null
type XReadGroupResult = Array<[string, Array<[string, string[]]>]> | null;

// メインポーリングループ。
// readConn: この committer 専用の Redis 読み取りコネクション。BLOCK 付き XREADGROUP を撃つので、
// 他 committer と共有すると FIFO キューで相互ブロックする → committer ごとに分ける。xdel/xack/del など
// 非ブロッキング系は共有 `redis` のまま（BLOCK しないので相互ブロックしない）。COMMITTERS=1 のときは
// 呼び出し側が `redis` 自身を渡す（Lv9/Lv10 とコネクション構成もバイト同一に保つ）。
async function consumeLoop(readConn: Redis): Promise<void> {
  // BATCH_WRITES on のときは WRITE_BATCH_SIZE を COUNT に使う。off は従来どおり CONSUMER_CONCURRENCY。
  const readCount = BATCH_WRITES ? WRITE_BATCH_SIZE : CONSUMER_CONCURRENCY;

  while (running) {
    let result: XReadGroupResult;
    try {
      result = (await readConn.xreadgroup(
        'GROUP', GROUP_NAME, CONSUMER_NAME,
        'COUNT', String(readCount),
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

    if (BATCH_WRITES) {
      // バッチ経路: 読んだ全 entries を 1本の multi-row INSERT にまとめる。
      await processBatch(entries);
    } else {
      // per-row 経路（Lv8 と同一）: 1件ずつ processEntry。
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
}

// stale PEL 回収ループ（10s 毎に XAUTOCLAIM で idle なエントリを自分に移譲して再 INSERT）。
// XAUTOCLAIM は Redis 6.2+ の機能。ioredis では redis.call() 経由で呼ぶ。
// 戻り値: ["0-0", [[id, fields], ...], [...deleted]]
// running=false になったら早期に抜ける中断可能 sleep。素朴な 10s sleep を shutdown で
// await すると毎回最大 10s stall する（docker stop grace を食い潰し SIGKILL 誘発）ため、
// 250ms 刻みで running を見る。
async function sleepInterruptible(totalMs: number): Promise<void> {
  const step = 250;
  for (let waited = 0; waited < totalMs && running; waited += step) {
    await new Promise((r) => setTimeout(r, step));
  }
}

async function autoclaimLoop(): Promise<void> {
  while (running) {
    await sleepInterruptible(10000);
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

      if (BATCH_WRITES) {
        // バッチ経路: 回収分もまとめて multi-row INSERT。
        await processBatch(entries);
      } else {
        // per-row 経路（Lv8 と同一）。
        for (const [id, rawFields] of entries) {
          const fields: Record<string, string> = {};
          for (let i = 0; i + 1 < rawFields.length; i += 2) {
            fields[rawFields[i]] = rawFields[i + 1];
          }
          await processEntry(id, fields);
        }
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

// consumeLoop / autoclaimLoop の Promise を掴んでおき、shutdown で in-flight バッチの完了を待つ。
// batch 経路では 1 バッチ = 最大 WRITE_BATCH_SIZE 件なので、INSERT 済み↔pipeline XDEL/XACK 前で
// 打ち切ると宙吊り（再配送→重複）が最大 batch size 件に増幅する。両ループを await して減らす。
let loopPromise: Promise<void> | null = null;
let autoclaimPromise: Promise<void> | null = null;
// COMMITTERS>1 のときだけ各 committer 専用の read コネクションを張る（1 のときは空＝共有 `redis` を使う）。
const readConns: Redis[] = [];

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
  console.log(
    `[consumer] mode: BATCH_WRITES=${BATCH_WRITES} WRITE_BATCH_SIZE=${WRITE_BATCH_SIZE} ` +
      `synchronous_commit=${SYNC_COMMIT_OFF ? 'off' : 'on'} COMMITTERS=${COMMITTERS} ` +
      `commit_delay=${COMMIT_DELAY_US > 0 ? `${COMMIT_DELAY_US}us(siblings=${COMMIT_SIBLINGS})` : 'off'}`,
  );

  // metrics server 起動。
  await metricsApp.listen({ port: CONSUMER_PORT, host: '0.0.0.0' });
  console.log(`[consumer] metrics available at http://0.0.0.0:${CONSUMER_PORT}/metrics`);

  // autoclaim と consume を並行起動。autoclaim の Promise も掴んで shutdown で待てるようにする。
  autoclaimPromise = autoclaimLoop().catch((err) => console.error('[consumer] autoclaimLoop error:', err));

  // COMMITTERS>1 のとき、各 committer 専用の read コネクションを張る（BLOCK の相互ブロック回避）。
  // COMMITTERS=1 のときは張らず、共有 `redis` をそのまま read に使う（Lv9/Lv10 とコネクション構成一致）。
  if (COMMITTERS > 1) {
    for (let i = 0; i < COMMITTERS; i++) {
      const conn = redis.duplicate();
      conn.on('error', () => {
        // Suppress unhandled error events（本体の `redis` と同じ扱い）。
      });
      readConns.push(conn);
    }
    // lazyConnect: true なので明示 connect。「already connecting/connected」の良性 reject だけ握る。
    await Promise.all(
      readConns.map(async (conn) => {
        try {
          await conn.connect();
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          if (!/already (connect|connecting|connected)/i.test(msg)) {
            throw new Error(`[consumer] Redis read-conn connect failed: ${msg}`);
          }
        }
      }),
    );
  }

  // COMMITTERS 本の consumeLoop coroutine を並行起動する。各 coroutine は自身の processBatch を
  // await してから次を読む（＝1本ずつ commit）が、N 本あれば別々の pg コネクションで commit が
  // 同時に in-flight になり、commit_delay の group commit が効く母集団になる。全 coroutine を
  // Promise.all で束ねて shutdown（loopPromise の await）で in-flight バッチの完了を待つ。
  // read コネクション: COMMITTERS=1 は共有 `redis`、>1 は committer 専用 readConns[i]。
  const loops = Array.from({ length: COMMITTERS }, (_, i) =>
    consumeLoop(COMMITTERS === 1 ? redis : readConns[i]),
  );
  loopPromise = Promise.all(loops).then(() => undefined);
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
      // consume と autoclaim 両方の in-flight バッチが XDEL/XACK まで抜けるのを待つ
      // （最大 ~1s + 1バッチ）。sleepInterruptible により autoclaim の 10s sleep は即抜ける。
      await Promise.all([loopPromise, autoclaimPromise].filter(Boolean) as Promise<void>[]);
      await metricsApp.close();
      await Promise.all([writePool.end(), redis.quit(), ...readConns.map((c) => c.quit())]);
    } finally {
      process.exit(0);
    }
  });
}

main().catch((err) => {
  console.error('[consumer] fatal:', err);
  process.exit(1);
});
