// Lv19 — 分散tx(none/2pc/saga)の比較負荷。POST /orders(在庫 items-db + order orders-db の2DB跨ぎ)。
//
// SCENARIO=steady : 一定レートで POST /orders → mode 別の order_latency を比較(正常時)。
// SCENARIO=faults : 固定 N 回だけ叩く → 障害注入(FAULT_POINT)時の失敗数を数える。
//                   不整合(在庫だけ減って order 無し 等)は psql で DB を突き合わせて確認する(README 参照)。
//
// 実行例:
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:3000 -e SCENARIO=steady \
//     -e RATE=40 -e DURATION=15s grafana/k6 run - < load/order-tx.js
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:3000 -e SCENARIO=faults -e N=100 \
//     grafana/k6 run - < load/order-tx.js

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:3000';
const SCENARIO = __ENV.SCENARIO || 'steady';
const RATE = Number(__ENV.RATE || 40);
const DURATION = __ENV.DURATION || '15s';
const N = Number(__ENV.N || 100);
const ITEM_MAX = Number(__ENV.ITEM_MAX || 50);
const TOKEN = __ENV.TOKEN || 'demo-token-1';

const order_latency = new Trend('order_latency', true);   // 正常時 mode 別 latency
const order_201 = new Counter('order_201');
const order_409 = new Counter('order_409');               // 在庫不足(正常系)
const order_5xx = new Counter('order_5xx');               // 障害注入で coordinator が落ちた回数

const params = { headers: { 'Content-Type': 'application/json', 'x-auth-token': TOKEN }, tags: { op: 'order' } };

const scenarios = {};
if (SCENARIO === 'faults') {
  scenarios.faults = { executor: 'per-vu-iterations', vus: 1, iterations: N, maxDuration: '60s', exec: 'placeOrder' };
} else {
  scenarios.steady = {
    executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: DURATION,
    preAllocatedVUs: RATE, maxVUs: RATE * 2, exec: 'placeOrder',
  };
}

export const options = {
  scenarios,
  // 障害注入時は 5xx が出るのが正常なので閾値は緩め。
  thresholds: { http_req_failed: [SCENARIO === 'faults' ? 'rate<1.01' : 'rate<0.10'] },
};

function pickItem() { return 1 + ((__VU + __ITER) % ITEM_MAX); }

export function placeOrder() {
  const body = JSON.stringify({ itemId: pickItem(), qty: 1 });
  const res = http.post(`${BASE_URL}/orders`, body, params);
  check(res, { 'order 201': (r) => r.status === 201 });
  if (res.status === 201) { order_201.add(1); order_latency.add(res.timings.duration); }
  else if (res.status === 409) order_409.add(1);
  else if (res.status >= 500) order_5xx.add(1);
}
