// write-heavy 負荷試験。write 90% / read 10% の偏った比率で /items を叩く。
// Lv8 の「write-behind キュー」効果測定用:
//   wall (ASYNC_WRITE 未設定): 同期 INSERT が律速となり write p95 が高止まりする。
//   fix  (ASYNC_WRITE=1):      202 で即返しになり write p95 が激減するが、
//         /metrics の write_queue_depth が増加し commit_lag も積み上がる。
//         → バッファであって乗数でない点を queue depth + lag の監視で確認できる。
//
// 503（背圧）は失敗に数えず Counter で別計上する（cpu-vs-health.js の作法に倣う）。
// depth は `curl -s localhost:8080/metrics | grep write_queue_depth` で watch すること。
//
// 実行例:
//   # wall（同期INSERT）
//   docker compose -f stages/08-async-queue/docker-compose.wall.yml up --build -d
//   docker run --rm -i \
//     -e BASE_URL=http://host.docker.internal:8080 \
//     -e PEAK_VUS=100 \
//     grafana/k6 run - < load/write-heavy.js
//   docker compose -f stages/08-async-queue/docker-compose.wall.yml down -v
//
//   # queue（write-behind, ASYNC_WRITE=1）
//   docker compose -f stages/08-async-queue/docker-compose.queue.yml up --build -d
//   docker run --rm -i \
//     -e BASE_URL=http://host.docker.internal:8080 \
//     -e PEAK_VUS=100 \
//     grafana/k6 run - < load/write-heavy.js
//   # 別ターミナルで depth を watch:
//   watch -n1 'curl -s localhost:8080/metrics | grep write_queue_depth'
//   docker compose -f stages/08-async-queue/docker-compose.queue.yml down -v

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8080';
// PEAK_VUS: 最大同時 VU 数（既定 100）
const PEAK_VUS = Number(__ENV.PEAK_VUS || 100);
// WRITE_RATIO: write の割合（既定 0.9）
const WRITE_RATIO = Number(__ENV.WRITE_RATIO || 0.9);

// write p95（体感レイテンシ）と read p95 を独立して見るための Trend。
const write_latency = new Trend('write_latency', true);
const read_latency = new Trend('read_latency', true);
// 背圧 503 は失敗でなく「正しく落とした」件数として別計上。
const write_backpressure = new Counter('write_backpressure');

export const options = {
  // queue depth の単調増加を見せるためホールドを長めに取る。
  stages: [
    { duration: '10s', target: PEAK_VUS }, // ramp up
    { duration: '60s', target: PEAK_VUS }, // hold: write 90%
    { duration: '5s',  target: 0 },        // ramp down
  ],
  thresholds: {
    // write 側は壁ではなくキューの性能を見るので厳しくしすぎない。
    write_latency: ['p(95)<2000'],
    read_latency:  ['p(95)<500'],
    // 503（背圧）は write_backpressure Counter で別計上しているので
    // http_req_failed は 503 込みで緩め（背圧が多発しても試験を落とさない）。
    http_req_failed: ['rate<0.50'],
  },
};

export default function () {
  if (Math.random() < WRITE_RATIO) {
    // write パス
    const res = http.post(
      `${BASE_URL}/items`,
      JSON.stringify({ name: `k6-${__VU}-${__ITER}` }),
      {
        headers: { 'Content-Type': 'application/json' },
        tags: { op: 'write' },
      },
    );
    // wall では 201、fix（queue）では 202 を期待。503（背圧）は check の失敗に数えない。
    // 503 は速い reject なので write_latency に混ぜると headline の p95 を不当に下げる。
    // レイテンシは受理された 201/202 のみ記録する。
    if (res.status === 503) {
      write_backpressure.add(1);
    } else {
      check(res, {
        'write 201/202': (r) => r.status === 201 || r.status === 202,
      });
      write_latency.add(res.timings.duration);
    }
  } else {
    // read パス（残り 10%）
    const res = http.get(`${BASE_URL}/items`, { tags: { op: 'read' } });
    check(res, { 'GET 200': (r) => r.status === 200 });
    read_latency.add(res.timings.duration);
  }
  sleep(0.05);
}
