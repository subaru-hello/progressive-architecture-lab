# Lv8 — write-behind 非同期キュー（Redis Stream）

## 結論（先に）

キャッシュ（Lv6）も worker_threads（Lv7）も read/write latency の**片方だけ**を助けた。
write は常に DB commit 待ちで律速されていた。write-behind で `POST` を 202 即返しにして
DB commit から切り離す——が、実測すると単純な「速くなる」話ではなかった。

> **write-behind はバッファであって乗数ではない。** 202 の即返しは client の体感 latency を
> DB commit から切り離すが、**持続的な耐久 throughput は consumer の drain レート（DB commit 律速）
> のまま**で、enqueue の速さとは無関係。sustained な enqueue レートが drain を超えると depth は
> 際限なく伸び（＝ commit lag という負債）、上限を付ければ 503 背圧に変わる。実測の要点:
>
> - **DB が飽和していない負荷（100VU）では write-behind は「負け」**。同期 INSERT p95 13.7ms に対し
>   queue は 20.7ms（XADD の 1 ホップぶん遅い）。しかも depth が 0→41,562 に伸び commit lag ~19s の
>   負債が溜まる。**4 つの api が同期 INSERT を 40 コネクションで並列に捌けるうちは、キューは overhead でしかない。**
> - **DB が飽和する負荷（400VU）で初めて 202 の即返しが効く**。同期 wall は write p95 120ms まで悪化するが、
>   queue は受理された write の p95 72ms。ただし単一 consumer は drain し切れず depth は上限 100k に張り付き、
>   **write の 51% が 503 で弾かれた**（背圧）。
> - **耐久 throughput を上げる本当のレバーは「キュー追加」でなく「consumer の並列化/バッチ化」**。
>   consumer を 1→3 に増やすと背圧が 152k→81k（失敗 51%→35%）に減った＝ drain はほぼ線形にスケールする。
>   だが enqueue(XADD) は INSERT より桁違いに軽いので、固定数の consumer は高負荷では producer に必ず抜かれる。

## 仕組み

```
                             Redis Stream
POST /items ──> api ──XADD──> items:writes ──XREADGROUP──> consumer ──INSERT──> Postgres
          202（即返し）         depth += 1                   XACK+XDEL
                                                             cache DEL
```

- **201 = 永続化済み**（同期パス、Lv0-7 の挙動）
- **202 = 受理のみ**（非同期パス、永続化はまだ）
- **at-least-once**: consumer が INSERT 後・XACK 前に crash すると再配送 → 重複 INSERT しうる（冪等化はしない＝教材。INSERT 前 crash なら再配送のみで重複なし）
- **cache DEL は consumer 側**: commit 後に consumer が `items:latest100` を DEL する

## エラーモデル

| 状況 | 挙動 |
|---|---|
| XADD 失敗（Redis 障害） | 503（同期 INSERT にフォールバックしない） |
| Stream depth ≥ WRITE_QUEUE_MAX | 503（背圧）|
| INSERT 失敗 | XACK/XDEL せず PEL に残す → 再配送 |
| 重複 INSERT | 起きうる（冪等化なし）|

## 使い方

### フェーズA — 壁（同期 INSERT）
```bash
docker compose -f stages/08-async-queue/docker-compose.wall.yml up --build -d
curl -s localhost:8080/ready
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e PEAK_VUS=100 \
  grafana/k6 run - < load/write-heavy.js
docker compose -f stages/08-async-queue/docker-compose.wall.yml down -v
```

### フェーズB — write-behind キュー
```bash
docker compose -f stages/08-async-queue/docker-compose.queue.yml up --build -d
curl -s localhost:8080/ready

# 202 が返ることを確認
curl -s -X POST localhost:8080/items \
  -d '{"name":"lv8-test"}' -H 'Content-Type: application/json'

# 数秒後に consumer が DB に INSERT したことを確認（結果整合）
curl -s localhost:8080/items | grep lv8-test

# depth Gauge は API の /metrics（caddy 経由）で watch
watch -n1 'curl -s localhost:8080/metrics | grep write_queue_depth'
# consumer の commit lag Histogram（consumer は expose のみ＝host バインドしないので compose 経由で）
docker compose -f stages/08-async-queue/docker-compose.queue.yml \
  exec consumer wget -qO- localhost:3001/metrics | grep write_queue_commit_lag_seconds

# 負荷（depth の単調増加を観測）
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e PEAK_VUS=100 \
  grafana/k6 run - < load/write-heavy.js

docker compose -f stages/08-async-queue/docker-compose.queue.yml down -v
```

### A/B: consumer replicas 1→2 で drain が追いつく
```bash
# queue compose 起動後、consumer を 2 に増やす（consumer は expose なのでポート衝突しない）
docker compose -f stages/08-async-queue/docker-compose.queue.yml \
  up -d --scale consumer=2

# depth が減り始めることを watch で確認
watch -n1 'curl -s localhost:8080/metrics | grep write_queue_depth'
```

### WRITE_QUEUE_MAX を絞ると 503 背圧
```yaml
# docker-compose.queue.yml の api environment に追加:
WRITE_QUEUE_MAX: "100"
```
```bash
# depth が 100 を超えると 503 が返り始める
# k6 の write_backpressure Counter で背圧件数を確認
```

### AOF off + crash でエントリが消えるデモ（消える可怖さを体感）
```bash
# docker-compose.queue.yml の redis コマンドを以下に変更:
#   command: redis-server  # --appendonly yes を外す
# 負荷中に `docker compose ... kill redis && docker compose ... up -d redis` すると、
# AOF 無効なので未 drain の Stream entry（202 で受理済みだが未 INSERT）が丸ごと消える。
# ＝「202 は受理であって永続化ではない」を体感するデモ。
# 対して Lv8 の既定は AOF on + XDEL→XAC（commit 後）なので、consumer が落ちても
# 未処理 entry は AOF から復元され、in-flight は PEL 経由で再配送される（重複しうる）。
```

## 測るもの

| 指標 | 見方 |
|---|---|
| **`write_latency` p95** | wall は DB commit 律速。queue の 202 即返しが勝つのは**DB が飽和する高負荷のときだけ**（低負荷では XADD ぶん負ける）。503 は除外して受理分のみ計測。 |
| **`write_queue_depth`** | `/metrics` の Gauge。queue 時は増加する。consumer 追加で下がる。 |
| **`write_queue_commit_lag_seconds`** | consumer `/metrics` の Histogram。lag が積み上がると 99th が悪化する。 |
| **`write_backpressure`** | k6 Counter。503 背圧の件数。WRITE_QUEUE_MAX を絞ると増える。 |
| **`read_latency` p95** | queue でも read は影響を受けない（write-behind は読み取りパスを変えない）。 |

## 実測（8 コア Mac / write-heavy.js 90%write / wall↔queue を背中合わせ A/B）

api 4 replicas / consumer 1（明記した行のみ 3）。depth は API の `/metrics` を 3s 間隔で watch。

### 低負荷（PEAK_VUS=100 ≒ 1,400 write/s）— DB は飽和しない
| | wall（同期 INSERT） | queue（write-behind, consumer=1） |
|---|---:|---:|
| write p95 | **13.7ms** | 20.7ms（**負け**：XADD の 1 ホップ） |
| write p50 | 3.8ms | 3.7ms |
| read p95 | 11.2ms | 24.0ms |
| write_queue_depth（peak） | — | **0 → 41,562**（単調増加＝負債） |
| commit lag（平均） | 0（即永続化） | **~19s**（<5s は全体の 11% のみ） |
| throughput | 1,594 iter/s | 1,550 iter/s |

→ **4 api が同期 INSERT を 40 コネクションで並列に捌けるので DB は詰まらない。** queue は overhead
でしかなく、depth と lag に負債だけ残す。write-behind が無条件に速いわけではない。

### 高負荷（PEAK_VUS=400 ≒ 3,100+ write/s）— DB commit が飽和する
| | wall（同期） | queue（consumer=1） | queue（consumer=3） |
|---|---:|---:|---:|
| write p95（受理分） | **120.1ms** | **72.1ms**（202 即返しが効く） | 141.7ms※ |
| write_queue_depth | — | 100k に張り付き（上限） | 100k に張り付き（上限） |
| write_backpressure（503） | 0 | **152,076（51%）** | **80,824（35%）** |
| throughput（全 req） | 3,486 iter/s | 3,978 iter/s | 3,066 iter/s |

※consumer=3 の p95 が上がったのは、3 consumer＋4 api＋db＋redis＋k6 が 8 コアに乗り load avg 28 まで
上がった単一ホスト競合のノイズ（絶対値でなく背圧 152k→81k の相対差で読む）。

→ 高負荷では 202 即返しが同期 wall に勝つ（72ms vs 120ms）が、単一 consumer は drain し切れず
depth は上限 100k に張り付き **write の半分が 503**。consumer を 3 に増やすと背圧が 152k→81k に減る
（drain はほぼ線形にスケール）。だが enqueue は INSERT より桁違いに軽いので、depth の立ち上がりは
consumer 数を増やしても止まらない＝ **producer は固定数の consumer を必ず抜く。**

**注記**: 単一 Mac 上の相対 A/B。計測中 load avg は 5〜28 と変動（特に consumer=3 は自己競合で上振れ）。
絶対値でなく wall↔queue・consumer 1↔3 の相対差で読むこと。

## 設計の割り切り（教材ポイント）

- **202 ≠ 書き込み保証**: クライアントは「受理された」ことしか知らない。
  Redis 障害 → consumer 停止 → DB 障害が重なると消える可能性がある。
  本番では「acknowledged but not durable」を API ドキュメントで明示する必要がある。
- **重複 INSERT しうる**: at-least-once。INSERT 後 XACK 前に crash すると再配送で重複する。
  冪等化（UNIQUE 制約 + ON CONFLICT DO NOTHING 等）は Lv8 の範囲外——
  「冪等でないと at-least-once は at-least-some-writes でもある」を体感するのが狙い。
- **XDEL の理由**: XACK だけでは Stream length は減らない。XDEL することで
  `XLEN items:writes` が「未 INSERT エントリ数」として機能し、depth メトリクスが正確になる。
- **AOF 永続化**: `--appendonly yes` なしでは Redis 再起動で Stream が消える。
  AOF on でも fsync タイミング次第で最後の数件は失われうる（`appendfsync always` で解決可）。
