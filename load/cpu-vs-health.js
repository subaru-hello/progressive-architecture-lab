// CPU バーン vs /health 生存確認の2シナリオ分離負荷試験。
//
// 狙い:
//   wall  (WORKER_POOL_SIZE 未設定): /work?cpu=N がイベントループを塞ぎ、
//         同居する /health が飢えて p95 が悪化・失敗が出る。
//   fix   (WORKER_POOL_SIZE=4):      CPU バーンが worker thread に offload され、
//         イベントループが解放される → /health は平坦（p95 < 50ms）。
//         キューが詰まると /work は 503 を返す（背圧）が /health は影響を受けない。
//
// 実行例:
//   docker run --rm -i \
//     -e BASE_URL=http://host.docker.internal:8080 \
//     -e PEAK_VUS=200 \
//     -e CPU=10 \
//     grafana/k6 run - < load/cpu-vs-health.js

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8080';
const PEAK_VUS = Number(__ENV.PEAK_VUS || 200);
const CPU = __ENV.CPU || '10';

// /health 専用 Trend（burn 側の 503 と混ぜない）
const health_latency = new Trend('health_latency', true);
// 背圧で 503 になった /work の数。失敗ではなく「正しく落とした」件数として計上。
const work_backpressure = new Counter('work_backpressure');

export const options = {
  scenarios: {
    // CPU バーン: ランプアップ → ホールド → ランプダウン
    burn: {
      executor: 'ramping-vus',
      exec: 'burn',
      stages: [
        { duration: '10s', target: PEAK_VUS }, // ramp up
        { duration: '30s', target: PEAK_VUS }, // hold
        { duration: '5s',  target: 0 },        // ramp down
      ],
    },
    // 定常 /health プローブ: 全期間を通じて 10 req/s
    probe: {
      executor: 'constant-arrival-rate',
      exec: 'probe',
      rate: 10,
      timeUnit: '1s',
      duration: '45s',
      preAllocatedVUs: 5,
      maxVUs: 20,
    },
  },
  thresholds: {
    // /health の p95 が 50ms 以内に収まること（wall で赤・fix で緑）
    'http_req_duration{scenario:probe}': ['p(95)<50'],
    health_latency: ['p(95)<50'],
    // /health の失敗率が 1% 未満（wall で赤になることを期待）
    'http_req_failed{scenario:probe}': ['rate<0.01'],
    // burn 側は 503（背圧）込みなので失敗率は緩め
    'http_req_failed{scenario:burn}': ['rate<0.80'],
  },
};

// burn シナリオ関数: 200 と 503（背圧）はどちらも期待通り。
// 503 を check の失敗に数えると checks_failed が水増しされるので Counter で別計上する。
// 「想定外」は 200/503 以外の 5xx だけ、というのを check の合否条件にする。
export function burn() {
  const res = http.get(`${BASE_URL}/work?cpu=${CPU}`);
  const saturated = res.status === 503;
  if (saturated) work_backpressure.add(1);
  check(res, {
    'work 200 or 503 (expected)': (r) => r.status === 200 || r.status === 503,
  });
}

// probe シナリオ関数: /health を 1s タイムアウト付きで叩く
export function probe() {
  const res = http.get(`${BASE_URL}/health`, { timeout: '1s' });
  check(res, { 'health 200': (r) => r.status === 200 });
  health_latency.add(res.timings.duration);
}
