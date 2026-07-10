// read-heavy 負荷試験。read 95% / write 5% の偏った比率で /items を叩く。
// Lv5 の「read/write 分離」の効果測定用: replica が read を引き受けると primary の負荷が下がる。
//
// 実行:
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 grafana/k6 run - < load/read-heavy.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8080';
// PEAK_VUS lets us re-run the same script at a harder load to saturate the primary.
//   default 150 = moderate (primary not yet saturated, latency parity)
//   e.g. 400    = stress   (wall primary hits ~100% CPU, write p95 diverges)
const PEAK_VUS = Number(__ENV.PEAK_VUS || 150);
// WRITE_RATIO controls the write fraction (default 5%). Lower it (e.g. 0.005)
// to make the workload read-dominant — used in Lv6 to show that a cache's payoff
// depends on how often writes invalidate it.
const WRITE_RATIO = Number(__ENV.WRITE_RATIO || 0.05);

// Separate Trend metrics so write p95 can be sliced independently from read p95.
const write_latency = new Trend('write_latency', true);
const read_latency = new Trend('read_latency', true);

export const options = {
  stages: [
    { duration: '10s', target: PEAK_VUS }, // ramp up
    { duration: '30s', target: PEAK_VUS }, // hold: 95% reads, 5% writes
    { duration: '5s', target: 0 },         // ramp down
  ],
  thresholds: {
    // エラー率を先に見る: 1% 超えたら throughput の数字に意味がない。
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
    write_latency: ['p(95)<500'],
    read_latency: ['p(95)<500'],
  },
};

export default function () {
  if (Math.random() < WRITE_RATIO) {
    // write → primary (writePool). In Lv6, each write DELs the cache key.
    const res = http.post(
      `${BASE_URL}/items`,
      JSON.stringify({ name: `k6-${__VU}-${__ITER}` }),
      {
        headers: { 'Content-Type': 'application/json' },
        tags: { op: 'write' },
      },
    );
    check(res, { 'POST 201': (r) => r.status === 201 });
    write_latency.add(res.timings.duration);
  } else {
    // 95%: read → replica (readPool)
    const res = http.get(`${BASE_URL}/items`, { tags: { op: 'read' } });
    check(res, { 'GET 200': (r) => r.status === 200 });
    read_latency.add(res.timings.duration);
  }
  sleep(0.05);
}
