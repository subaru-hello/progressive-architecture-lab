// 引っ越し軸(Lv16/Lv18) — 移行の最中に常時流し、ダウンタイム(=error 率)を測る負荷。
//
// POST /orders(主) + GET /orders(cross-domain JOIN) + GET /items を一定レートで叩き、
// endpoint 別に成否を分離計上する。用途:
//   Lv16 mud : items を「引っ越そう」とした瞬間に /orders(共有tx) と /orders 一覧(JOIN)が
//              壊れる = error スパイクを観測（泥団子は live で動かせない）。
//   Lv18 hex : strangler の各 phase(expand/dual-write/backfill/shadow/cutover/contract)を
//              またいで error≈0 が保たれることを観測（PHASE でラベルしてファイル保存）。
//
// 実行例:
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:3000 -e PHASE=baseline \
//     -e RATE=50 -e DURATION=60s grafana/k6 run - < load/migration.js
//
// PHASE はタグ付けのみ（k6 の summary は phase で自動分割しないので、phase ごとに実行し
// 出力ファイル名で区別する運用。cutover の一瞬を見るなら DURATION を短く連射）。

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:3000';
const PHASE = __ENV.PHASE || 'steady';
const RATE = Number(__ENV.RATE || 50);       // orders/sec
const DURATION = __ENV.DURATION || '60s';
const ITEM_MAX = Number(__ENV.ITEM_MAX || 50);
const TOKEN = __ENV.TOKEN || 'demo-token-1';
const LIST_EVERY = Number(__ENV.LIST_EVERY || 5); // N 回に 1 回 GET /orders + GET /items も叩く

const order_latency = new Trend('order_latency', true);       // POST /orders 成功の end-to-end
const items_read_latency = new Trend('items_read_latency', true);
// endpoint 別・種類別のエラー分離（http_req_failed だけだと混ざる）
const order_create_5xx = new Counter('order_create_5xx');     // 共有tx の破綻（引っ越し中の壊れ）
const order_create_409 = new Counter('order_create_409');     // 在庫不足（正常系）
const order_create_401 = new Counter('order_create_401');     // 認証失敗
const order_list_5xx = new Counter('order_list_5xx');         // cross-domain JOIN の破綻
const items_5xx = new Counter('items_5xx');

const tag = { phase: PHASE };
const orderParams = { headers: { 'Content-Type': 'application/json', 'x-auth-token': TOKEN }, tags: { ...tag, op: 'order_create' } };
const listParams = { tags: { ...tag, op: 'order_list' } };
const itemsParams = { tags: { ...tag, op: 'items_read' } };

export const options = {
  scenarios: {
    migrate: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: RATE,
      maxVUs: RATE * 2,
      exec: 'run',
    },
  },
  thresholds: {
    // 引っ越しが「ダウンタイムゼロ」なら低いはず。破綻アーム(Lv16 mud)では跳ねる。
    http_req_failed: ['rate<0.20'],
  },
};

function pickItem() {
  return 1 + ((__VU + __ITER) % ITEM_MAX);
}

export function run() {
  // 主: order 作成（mud では 3 テーブル共有 tx / hex では usecase 経由）
  const body = JSON.stringify({ itemId: pickItem(), qty: 1 });
  const res = http.post(`${BASE_URL}/orders`, body, orderParams);
  check(res, { 'order 201': (r) => r.status === 201 });
  if (res.status === 201) order_latency.add(res.timings.duration);
  else if (res.status >= 500) order_create_5xx.add(1);
  else if (res.status === 409) order_create_409.add(1);
  else if (res.status === 401) order_create_401.add(1);

  // 副: 一覧(JOIN)と items 読み（引っ越しで壊れる側を監視）
  if (__ITER % LIST_EVERY === 0) {
    const lr = http.get(`${BASE_URL}/orders`, listParams);
    if (lr.status >= 500) order_list_5xx.add(1);

    const ir = http.get(`${BASE_URL}/items`, itemsParams);
    if (ir.status === 200) items_read_latency.add(ir.timings.duration);
    else if (ir.status >= 500) items_5xx.add(1);
  }
}
