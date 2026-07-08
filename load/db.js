// DB I/O 律速の負荷試験。/items を叩いて「単一DBへの接続」を奪い合わせる。
// Lv4 の壁（コネクション枯渇）と、PgBouncer での解消を同一スクリプトで測るための土台。
//   直結(壁):  docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 grafana/k6 run - < load/db.js
//   pgbouncer: 同上（compose を pgbouncer 版にして再実行するだけ）
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:3000';

export const options = {
  stages: [
    { duration: '10s', target: 100 }, // 一気に同時接続を上げて接続スロットを奪い合わせる
    { duration: '25s', target: 100 }, // hold
    { duration: '5s', target: 0 },
  ],
  thresholds: {
    // 壁フェーズでは赤くなる想定。PgBouncer フェーズで満たせるかを見る指標。
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

export default function () {
  // 8 割は読み取り、2 割は書き込み。どちらも DB 接続を必要とする I/O。
  if (Math.random() < 0.2) {
    const res = http.post(`${BASE_URL}/items`, JSON.stringify({ name: `k6-${__VU}-${__ITER}` }), {
      headers: { 'Content-Type': 'application/json' },
    });
    check(res, { 'POST 201': (r) => r.status === 201 });
  } else {
    const res = http.get(`${BASE_URL}/items`);
    check(res, { 'GET 200': (r) => r.status === 200 });
  }
  sleep(0.05);
}
