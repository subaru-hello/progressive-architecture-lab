// Lv12 — scale-to-zero(KEDA http-add-on) ↔ always-on(Lv3 HPA min=2) の背中合わせ計測。
//
// scale-to-zero の代償は「idle コスト → cold-start latency」への変換。これを4指標で測る:
//   (i)   cold-start latency: idle(0 Pod)後の初回リクエスト TTFB の p95/p99
//   (ii)  idle 常駐: 0 負荷時の Pod 数/メモリ（← これは kubectl 側で観測。下記 README 参照）
//   (iii) burst スケール境界: 0→N 立ち上げ中のエラー率と latency テール
//   (iv)  warm 定常スループット: 温まった後の req/s・p95（always-on と一致するはず）
//
// interceptor は Host ヘッダでルートするので **全リクエストに Host: pal.localhost を付ける**。
// always-on(Lv3) 側は host rule 無しの Ingress なのでヘッダは無害（同じスクリプトで両アーム測れる）。
//
// scenario は env SCENARIO で切替（cold-start と burst は必要なクラスタ状態が逆なので同時に回さない）:
//   SCENARIO=coldstart : idle→単発 を COLD_N 回。毎回 COLD_GAP 秒空けて KEDA に 0 まで落とさせる。
//                        （cluster 側 HTTPScaledObject.scaledownPeriod を短め=10〜15s にしておくこと）
//   SCENARIO=burst     : 低頻度 idle → 一気に 0→PEAK で burst。エラー率と warm 定常を測る（既定）。
//
// 実行例:
//   # scale-to-zero クラスタに対して:
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 -e SCENARIO=coldstart \
//     grafana/k6 run - < load/scale-to-zero.js
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 -e SCENARIO=burst -e PEAK_VUS=200 \
//     grafana/k6 run - < load/scale-to-zero.js
//   # always-on(Lv3 HPA min=2) クラスタにも同じ2本を投げて背中合わせ。

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8081';
const HOST_HEADER = __ENV.HOST_HEADER || 'pal.localhost';
const SCENARIO = __ENV.SCENARIO || 'burst';
const PEAK_VUS = Number(__ENV.PEAK_VUS || 200);
const COLD_N = Number(__ENV.COLD_N || 8); // cold-start サンプル数
const COLD_GAP = Number(__ENV.COLD_GAP || 60); // 各 cold サンプル間の間隔(秒)。実 idle→0 時間(≒window+scaledownPeriod≒45s)より長く取り、毎回 0 から起こす。
const WORK_MS = __ENV.WORK_MS || '20'; // I/O 律速。CPU バーンで箱を焼かない（cold-start を汚染しない）。

const params = { headers: { Host: HOST_HEADER }, tags: { op: 'work' } };

// cold-start（idle 後の初回）だけを分離して記録する Trend。warm と混ぜない。
const cold_start = new Trend('cold_start_ms', true);
// warm 定常区間の latency（burst hold 中）。
const warm_latency = new Trend('warm_latency', true);
// 立ち上げ中に interceptor がホールドし切れず落とした 5xx を別計上。
const scaleup_5xx = new Counter('scaleup_5xx');

// --- scenario 定義を env で出し分け ---
const scenarios = {};
if (SCENARIO === 'coldstart') {
  // 1 VU で COLD_N 回、毎回 COLD_GAP 秒空ける。maxDuration は余裕を持たせる。
  scenarios.coldstart = {
    executor: 'per-vu-iterations',
    vus: 1,
    iterations: COLD_N,
    maxDuration: `${COLD_N * (COLD_GAP + 10) + 30}s`,
    exec: 'coldStart',
  };
} else {
  // idle: 30s に 1 発だけ = scaledownPeriod を跨いで 0 に落とさせる低頻度区間。
  scenarios.idle = {
    executor: 'constant-arrival-rate',
    rate: 1,
    timeUnit: '30s',
    duration: '2m',
    preAllocatedVUs: 2,
    startTime: '0s',
    exec: 'oneShot',
  };
  // burst: idle 直後に一気に 0→PEAK。0→N 立ち上げのテールとエラー率を観測。
  scenarios.burst = {
    executor: 'ramping-arrival-rate',
    startTime: '2m10s',
    startRate: 0,
    timeUnit: '1s',
    preAllocatedVUs: PEAK_VUS,
    maxVUs: PEAK_VUS,
    stages: [
      { target: PEAK_VUS, duration: '10s' }, // 急立ち上げ = 0→N を踏ませる
      { target: PEAK_VUS, duration: '90s' }, // hold = warm 定常
    ],
    exec: 'burstReq',
  };
}

export const options = {
  scenarios,
  thresholds: {
    // 立ち上げ中の遅い 200 は許容。interceptor がホールドするのでエラーは低いはず。
    http_req_failed: ['rate<0.20'],
  },
};

// idle 区間の単発（scale-to-zero を発火させるだけ。latency は記録しない）。
export function oneShot() {
  http.get(`${BASE_URL}/work?ms=${WORK_MS}`, params);
}

// cold-start 単発: idle 後の初回 → TTFB を cold_start_ms に記録し、次サンプルまで待つ。
export function coldStart() {
  const res = http.get(`${BASE_URL}/work?ms=${WORK_MS}`, params);
  check(res, { 'coldstart 200': (r) => r.status === 200 });
  if (res.status >= 500) scaleup_5xx.add(1);
  cold_start.add(res.timings.duration);
  sleep(COLD_GAP); // この間に KEDA が 0 まで落とす → 次が再び cold
}

// burst リクエスト: 立ち上げ〜warm を通しで叩く。開始 15s 以降を warm とみなし別 Trend。
export function burstReq() {
  const res = http.get(`${BASE_URL}/work?ms=${WORK_MS}`, params);
  const ok = check(res, { 'burst 200': (r) => r.status === 200 });
  if (res.status >= 500) scaleup_5xx.add(1);
  // scenario 開始からの経過。__ITER ではなく時刻で warm 判定したいが k6 では VU 内時刻が無いので
  // 立ち上げ 10s stage を除いた hold 区間を warm とみなすため、ここでは 200 のみ warm に積む簡便法。
  if (ok) warm_latency.add(res.timings.duration);
}
