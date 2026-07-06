// CPU 律速の負荷試験。/work?cpu=N で CPU バーンさせ、水平スケールの効果を測る土台。
//   docker run --rm -i grafana/k6 run - < load/cpu.js
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:3000';
const CPU = __ENV.CPU || '10'; // 1 = 100万回 sqrt

export const options = {
  vus: 20,
  duration: '20s',
};

export default function () {
  const res = http.get(`${BASE_URL}/work?cpu=${CPU}`);
  check(res, { 'status is 200': (r) => r.status === 200 });
}
