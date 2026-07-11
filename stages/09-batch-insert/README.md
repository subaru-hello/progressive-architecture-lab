# Lv9 — Batched INSERT: commit 回数 N→1 で drain レートを上げる

## 狙い

Lv8 の consumer は XREADGROUP で N 件まとめて読んでも、`for` ループで 1件ずつ INSERT + commit していた。  
Lv9 では読んだバッチを **1本の multi-row INSERT（`VALUES ($1),($2),...`）＝ 1 commit** に束ねる。  
commit 回数を N→1 に落とすことで drain レート（Stream depth の減速・lag の改善）がどれだけ変わるかを観測する。

```
before (Lv8): N entries → N × (INSERT + commit + XDEL + XACK)
after  (Lv9): N entries → 1 × (multi-row INSERT + commit) + 1 × pipeline(XDEL×N, XACK×N)
```

A/B の差はバッファが溢れる高負荷時（depth が積み上がる状態）に最も顕著に現れる。  
consumer replicas は両面とも 1 に固定して「consumer 数を変えずに吞吐量を改善できるか」を測る。

## 起動

```bash
# Lv9（batched）
docker compose -f stages/09-batch-insert/docker-compose.queue.yml up --build -d

# 停止
docker compose -f stages/09-batch-insert/docker-compose.queue.yml down -v
```

## A/B 比較手順

同じ負荷スクリプト（`load/write-heavy.js @400VU`）を両面で流す。consumer replicas は 1 固定。

```bash
# --- Baseline: Lv8 per-row （stage08 queue compose） ---
docker compose -f stages/08-async-queue/docker-compose.queue.yml up --build -d
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e PEAK_VUS=400 \
  grafana/k6 run - < load/write-heavy.js
docker compose -f stages/08-async-queue/docker-compose.queue.yml down -v

# --- Lv9 batched ---
docker compose -f stages/09-batch-insert/docker-compose.queue.yml up --build -d
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e PEAK_VUS=400 \
  grafana/k6 run - < load/write-heavy.js
docker compose -f stages/09-batch-insert/docker-compose.queue.yml down -v
```

## 観測方法

### Stream depth（Caddy 経由でどの API からも取れる）

```bash
curl -s localhost:8080/metrics | grep write_queue_depth
```

### commit lag & batch size（consumer プロセス内 metrics）

```bash
docker compose -f stages/09-batch-insert/docker-compose.queue.yml \
  exec consumer wget -qO- localhost:3001/metrics | grep write_queue
```

主要メトリクス:

| metric | 説明 |
|---|---|
| `write_queue_depth` | Stream の未 ACK 残件数（depth が増えると lag も増える） |
| `write_queue_commit_lag_seconds` | enqueue → Postgres commit までの時間 |
| `write_queue_batch_size` | 1 commit あたりに束ねた行数（Lv9 のみ） |

## 実測結果（8 コア Mac / write-heavy 90%write @400VU / consumer 1 固定・背中合わせ A/B）

| 指標 | Lv8 per-row（baseline） | Lv9 batched |
|---|---|---|
| ピーク write_queue_depth | **100,059**（上限 100k に張り付き） | **283**（詰まらない） |
| write 503 背圧（http_req_failed） | **183,915（54.84%）** | **0（0.00%）** |
| consumer drain 実績（commit_lag count） | 45,153 行 | **288,969 行（6.4×）** |
| 平均 commit lag | **~54.8s** | **~20.1ms**（≈2,700× 低） |
| 平均バッチサイズ（行/commit） | 1 | **32.9**（8,776 commit で 288,969 行） |
| write p95（受理された write のみ） | 67.7ms | 75.5ms |

**読み方**: commit を N→1 に束ねただけで、consumer 数を 1 に固定したまま drain throughput が **6.4×** に増えた。  
結果、per-row では depth が上限 100k に張り付き **write の半分以上（54.84%）が 503** で弾かれていたのが、batched では  
depth がほぼ 0 のまま（ピーク 283）流れ **503 ゼロ**。lag も ~55s → ~20ms へ崩壊。  
write p95（受理分）は batched の方がわずかに高いが、per-row の 67.7ms は「受理された 45% のみ」の値で、  
残り 55% は 503 即弾き。batched は **全 write を受理しながら**同等レイテンシを保っている点がフェアな比較。

## 実装ポイント

- `BATCH_WRITES` 未設定（off）のとき consumer は **Lv8 と完全同一に動く**（後方互換）。
- `BATCH_WRITES=1` のとき `processBatch()` が valid entries を `INSERT INTO items(name) VALUES ($1),...` に束ね、XDEL/XACK を ioredis pipeline で一括送信する。
- malformed（`name` 欠落）は INSERT 対象から除外し個別 XDEL/XACK して破棄。バッチをブロックしない。
- INSERT 失敗時は XDEL/XACK せず PEL に残す（at-least-once）。poison batch リスクは `consumer.ts` のコメントを参照。
