// k6 スモーク負荷試験。Docker で実行するので host.docker.internal でホストの API を叩く。
//   docker run --rm -i grafana/k6 run - < load/smoke.js
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 grafana/k6 run - < load/smoke.js
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:3000';

export const options = {
  stages: [
    { duration: '10s', target: 10 }, // ramp up
    { duration: '20s', target: 10 }, // hold  (DAU 相当の同時接続を模擬)
    { duration: '5s', target: 0 },   // ramp down
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],       // エラー率 1% 未満
    http_req_duration: ['p(95)<500'],     // p95 500ms 未満
  },
};

export default function () {
  // ms=20 で軽い I/O 待ち。cpu= を足すと CPU 律速にして水平スケールの効果が見える。
  const res = http.get(`${BASE_URL}/work?ms=20`);
  check(res, { 'status is 200': (r) => r.status === 200 });
  sleep(0.1);
}
