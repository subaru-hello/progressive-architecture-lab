# Lv7 — worker_threads（CPU 律速の解放）

Node.js のイベントループは**シングルスレッド**。`/work?cpu=N` のような重い計算が走ると
ループが塞がり、同居する `/health` や他のリクエストが文字通り「飢える」。

> **結論（先に）**: Lv2 で診断した probe 雪崩の根本原因（`docs/learning-log/02-lv2.md`：
> 同期 CPU バーンがイベントループを独占 → `/health`・`/ready` が応答できず probe がタイムアウト
> → k8s が健全な Pod を Evict/再起動）を、ここで根治する。`worker_threads` で CPU ジョブを
> 別スレッドに offload するとイベントループが解放され、`/health` は高負荷下でも平坦を保つ。
> キューが詰まった場合は 503（背圧）を返すが、`/health` 自体は影響を受けない。

## 仕組み

```
                          ┌────────────────────────────────┐
                          │  Node.js プロセス              │
client ──> /work?cpu=N ──>│  event loop (JS thread)        │
                          │    └─ WorkerPool.run()          │
                          │         ↓ postMessage           │
                          │  Worker[0] ─ sqrt loop ─ done ─┤
                          │  Worker[1] ─ sqrt loop          │
                          │  Worker[2] ─ (idle)             │
                          │  Worker[3] ─ (idle)             │
client ──> /health ──────>│  event loop → 即応 ✓           │
                          └────────────────────────────────┘
```

- `WORKER_POOL_SIZE` が未設定の場合は **Lv0-6 の同期パスのまま**（後方互換）。
- 常駐プール: リクエスト毎の Worker 生成コスト無し。
- 背圧: キュー長が `WORKER_QUEUE_MAX`（デフォルト `WORKER_POOL_SIZE × 100`）を超えたら 503。

## 実測（8 コア Mac / cpu-vs-health.js / PEAK_VUS=100・CPU=10 / wall↔worker を背中合わせで A/B）

`WORKER_POOL_SIZE=4`。`/health` を 10 req/s で叩き続けながら `/work?cpu=10` を 100 VU まで
ランプさせ、CPU バーン中に `/health` がどれだけ生き残るかを比べた。

| | wall（同期バーン） | worker（offload） | 差 |
|---|---:|---:|---:|
| **/health p95** | **1s（1s タイムアウト上限に張り付き）** | **7.71ms** | イベントループが解放 |
| /health 中央値 | 827ms | 2.32ms | −99.7% |
| **/health 失敗率** | **46.38%**（205/442 が 1s で死亡） | **0%** | probe 雪崩の有無 |
| throughput（全体 iter/s） | 83 | **360**（≈4.3×） | 4 スレッドが 4 コアを並列使用 |
| /work 503（背圧） | 0（同期は落とさず全部詰まる） | 0（この負荷ではキュー未飽和） | — |

- **wall**: `/health` の p95 が 1s（k6 のタイムアウト上限）に張り付き、**46% が死ぬ**。
  これが Lv2 の probe 雪崩の正体そのもの——単一イベントループが CPU バーンに占有され、
  同居する probe が応答できない。k8s ならここで readiness 失敗 → Evict の連鎖に入る。
- **worker**: `/health` p95 **7.71ms**・失敗 **0%**。CPU バーンは 4 スレッドに逃げ、
  イベントループは空いたまま。ついでに throughput も 4 コア並列で約 4.3 倍。

**注記**: 単一 Mac 上の相対 A/B として読むこと（計測時 load avg 3.6〜8.9 と変動、wall 側の方が
むしろ高負荷で測れており不利な条件）。絶対値は CPU 速度・他プロセスの load avg・コンテナへの
割り当てコア数で変わる。`PEAK_VUS` を上げる（例 200〜）と worker 側でも 503 背圧が出はじめる。

## Lv2/3 との関係（根本原因の回収）

| | Lv2/3（probe 雪崩） | Lv7（root cause fix） |
|---|---|---|
| 症状 | HPA が probe 失敗を検知し Evict → スケールアウトでも雪崩が続く | worker offload で probe が即応 |
| 根本 | 同期 CPU バーンがイベントループを占有 | worker thread がループを解放 |
| 対処 | Pod 追加（回避策） | offload（根本解決） |

## 使い方

### フェーズA — 壁（同期バーン）
```bash
docker compose -f stages/07-worker-threads/docker-compose.wall.yml up --build -d
curl -s localhost:8080/ready
# 負荷: CPU バーンと /health プローブを同時に走らせる
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e PEAK_VUS=100 -e CPU=10 \
  grafana/k6 run - < load/cpu-vs-health.js
docker compose -f stages/07-worker-threads/docker-compose.wall.yml down -v
```

### フェーズB — worker_threads offload
```bash
docker compose -f stages/07-worker-threads/docker-compose.worker.yml up --build -d
curl -s localhost:8080/ready
# offloaded: true が返ることを確認
curl -s 'localhost:8080/work?cpu=5'
# worker_pool_busy / worker_pool_queue_depth Gauge を確認
curl -s localhost:8080/metrics | grep worker_pool
# 同じ負荷で /health p95 が改善することを確認
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e PEAK_VUS=100 -e CPU=10 \
  grafana/k6 run - < load/cpu-vs-health.js
docker compose -f stages/07-worker-threads/docker-compose.worker.yml down -v
```

## 測るもの

| 指標 | 見方 |
|---|---|
| **`health_latency` p95** | 主指標。wall で悪化・worker で平坦。イベントループの解放度合い。 |
| **`/health` 失敗率** | wall で 1% 超え（k6 の probe threshold が赤）・worker でほぼ 0%。 |
| **`/work` 503 率** | worker 時のみ出る。キューが詰まった際の背圧。`PEAK_VUS` を上げると増える。 |
| **`worker_pool_busy`** | Prometheus Gauge。スレッドの使用率。`WORKER_POOL_SIZE` のチューニング指標。 |
| **`worker_pool_queue_depth`** | 待機中ジョブ数。増え続けるなら `WORKER_POOL_SIZE` を増やすサイン。 |

## 設計の割り切り（教材ポイント）

- **スレッド数 = コア数がベストとは限らない**: CPU バーンはコア律速だが、コンテナの
  CPU 割り当て・OS スケジューリング・hyperthreading の影響で最適値は変わる。
  `WORKER_POOL_SIZE` を env で外出しにしているのはそのため。
- **replicas: 1 のまま**: Lv3 のように Pod を増やさずに解決するのが Lv7 の核心。
  「水平スケール」と「スレッド並列」は直交する手段。
- **背圧の重要性**: プールが詰まった際に 503 を返すことで、上位（LB/クライアント）が
  リトライタイミングを制御できる。無限キューは OOM の原因になる。
