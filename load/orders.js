// 分解軸(Lv13-15) — POST /orders を 3 形態に投げて cross-context 依存の対価を測る。
//
// order 作成は 2 本の cross-context 依存を踏む:  auth(→users) + 在庫デクリメント(→items)。
//   Lv13 monolith : in-process 直 SQL JOIN     → 最小 latency（ベースライン）
//   Lv14 modular  : in-process ポート呼び出し   → Lv13 とほぼ一致（runtime 無料）
//   Lv15 micro    : HTTP hop ×2 + DB-per-service → network 税が p95 に乗る
//
// scenario は env SCENARIO で切替:
//   SCENARIO=steady : 一定レートで POST /orders → order_latency p95 を 3 形態で背中合わせ（既定）。
//   SCENARIO=chaos  : orders と items-read を同時に流す。計測中に運用者が users を落とす:
//                       docker compose -f stages/15-microservices/... stop users-api
//                     → micro は /items が 200 継続・/orders だけ 5xx（隔離）を数値で見る。
//                       monolith は users を落とす=プロセスごと死 → 両方全滅（対比）。
//
// 実行例:
//   # monolith/modular（:3000）
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:3000 grafana/k6 run - < load/orders.js
//   # microservices（gateway :8080）
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 grafana/k6 run - < load/orders.js
//   # blast radius:
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 -e SCENARIO=chaos -e DURATION=90s \
//     grafana/k6 run - < load/orders.js   # 起動 ~30s 後に別ターミナルで users-api を stop

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:3000';
const SCENARIO = __ENV.SCENARIO || 'steady';
const RATE = Number(__ENV.RATE || 100);        // orders/sec（steady）
const DURATION = __ENV.DURATION || '60s';
const ITEM_MAX = Number(__ENV.ITEM_MAX || 50); // seed 済デモ item は 1..50
const TOKEN = __ENV.TOKEN || 'demo-token-1';   // seed 済デモユーザ

// order 作成の end-to-end latency（auth + 在庫 + INSERT の合算）。形態間の主指標。
const order_latency = new Trend('order_latency', true);
// items 読取の latency（chaos で blast radius の生存側を測る）。
const items_read_latency = new Trend('items_read_latency', true);
// 失敗を種類別に分離（http_req_failed だけだと 401/409/5xx が混ざる）。
const order_5xx = new Counter('order_5xx');      // 依存サービス障害（cross-context の伝搬）
const order_401 = new Counter('order_401');      // 認証失敗
const order_409 = new Counter('order_409');      // 在庫不足
const items_5xx = new Counter('items_5xx');      // 読取側の障害（micro では 0 のはず）

const orderParams = {
  headers: { 'Content-Type': 'application/json', 'x-auth-token': TOKEN },
  tags: { op: 'order' },
};
const readParams = { tags: { op: 'read' } };

const scenarios = {};
if (SCENARIO === 'chaos') {
  // orders と reads を同時に一定レートで流す。運用者が途中で users を落とす。
  scenarios.chaos_orders = {
    executor: 'constant-arrival-rate',
    rate: Math.max(1, Math.round(RATE / 2)),
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: RATE,
    maxVUs: RATE * 2,
    exec: 'placeOrder',
  };
  scenarios.chaos_reads = {
    executor: 'constant-arrival-rate',
    rate: Math.max(1, Math.round(RATE / 2)),
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: RATE,
    maxVUs: RATE * 2,
    exec: 'readItems',
  };
} else {
  scenarios.steady = {
    executor: 'constant-arrival-rate',
    rate: RATE,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: RATE,
    maxVUs: RATE * 2,
    exec: 'placeOrder',
  };
}

export const options = {
  scenarios,
  thresholds: {
    // chaos は意図的に落とすので緩め。steady は依存が生きている前提で 5xx は低いはず。
    http_req_failed: [SCENARIO === 'chaos' ? 'rate<0.60' : 'rate<0.10'],
  },
};

// item は seed 済 1..ITEM_MAX から選ぶ。__VU で散らしてホット行集中を緩和（3 形態で同条件）。
function pickItem() {
  return 1 + ((__VU + __ITER) % ITEM_MAX);
}

export function placeOrder() {
  const body = JSON.stringify({ itemId: pickItem(), qty: 1 });
  const res = http.post(`${BASE_URL}/orders`, body, orderParams);
  check(res, { 'order 201': (r) => r.status === 201 });
  if (res.status === 201) order_latency.add(res.timings.duration);
  else if (res.status >= 500) order_5xx.add(1);
  else if (res.status === 401) order_401.add(1);
  else if (res.status === 409) order_409.add(1);
}

export function readItems() {
  const res = http.get(`${BASE_URL}/items`, readParams);
  const ok = check(res, { 'items 200': (r) => r.status === 200 });
  if (ok) items_read_latency.add(res.timings.duration);
  else if (res.status >= 500) items_5xx.add(1);
}
