# Lv10 — synchronous_commit=off：commit の「回数」でなく「コスト」を削る

## 狙い

Lv9 で「DB write の律速は INSERT の行数でなく **commit の回数**（= 1 commit ごとの fsync/WAL flush 固定費）」と分かった。
Lv10 はその fsync を正面から攻める。consumer は Lv9 の multi-row INSERT のまま、**write セッションだけ
`synchronous_commit=off`** にして、commit が WAL の fsync 完了を待たずに返るようにする。

```
Lv9  : batched INSERT → 1 commit で N 行、ただし各 commit は fsync 完了を待つ（synchronous_commit=on）
Lv10 : batched INSERT → 1 commit で N 行、commit は fsync を待たず即返る（synchronous_commit=off）
       → commit「回数」は Lv9 と同じ、1 回あたりの「コスト」を削る
```

- `SYNC_COMMIT_OFF=1` のとき consumer の `writePool` に libpq startup option `-c synchronous_commit=off` を渡す。
  接続確立時点でサーバ側セッションに効くので「SET 完了前に最初のクエリが走る」レースが無い。
- **write 経路（consumer）だけ**に効く。read 経路（api 側）には影響しない。
- 単一 consumer = 単一 committer なので `commit_delay`（group commit）は効かない（並列 committer がいて初めて fsync をまとめられる）。
  ここでは触らない——「group commit は並列がないと効かない」も学びのうち。

## 代償（durability）

`synchronous_commit=off` は commit をクライアントに返した後で WAL を fsync する。**OS/DB クラッシュ時、直近
数百 ms 分（`wal_writer_delay` 既定 200ms 程度）の commit 済みトランザクションが失われうる。**
データ「破損」ではなく最新分の消失。Lv8 の 202（受理≠永続化）に続く「速さと引き換えに何を失うか」の系譜。

## 起動

```bash
docker compose -f stages/10-sync-commit/docker-compose.queue.yml up --build -d
docker compose -f stages/10-sync-commit/docker-compose.queue.yml down -v
```

## A/B 比較手順

`synchronous_commit=on` の Lv9 が飽和する高負荷（例 PEAK_VUS=800）で背中合わせに回す。consumer は両面 1 固定。

```bash
# --- Baseline: Lv9 batched, sync_commit=on ---
docker compose -f stages/09-batch-insert/docker-compose.queue.yml up --build -d
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e PEAK_VUS=800 \
  grafana/k6 run - < load/write-heavy.js
docker compose -f stages/09-batch-insert/docker-compose.queue.yml down -v

# --- Lv10 batched, sync_commit=off ---
docker compose -f stages/10-sync-commit/docker-compose.queue.yml up --build -d
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e PEAK_VUS=800 \
  grafana/k6 run - < load/write-heavy.js
docker compose -f stages/10-sync-commit/docker-compose.queue.yml down -v
```

## 観測方法

```bash
# depth（どの API からでも取れる）
curl -s localhost:8080/metrics | grep write_queue_depth
# commit lag + batch size（consumer プロセス内）
docker compose -f stages/10-sync-commit/docker-compose.queue.yml \
  exec consumer wget -qO- localhost:3001/metrics | grep write_queue
```

## 実測結果（8 コア Mac / write-heavy 90%write / consumer 1 固定・背中合わせ A/B）

fsync の効果は「バッチ化で amortize されているか」で大きく変わるので、**2 つの領域**で測った。

### ① batched（Lv9 のまま）@800VU — fsync は既に ~59:1 に amortize 済み

| 指標 | sync_commit=on（Lv9 baseline） | sync_commit=off（Lv10） |
|---|---|---|
| ピーク write_queue_depth | 1,203 | **362** |
| write 503（http_req_failed） | 0% | 0% |
| consumer drain 実績 | 223,294 行 / 3,783 commit（avg 59 行） | 261,003 行 / 11,664 commit（avg 22 行） |
| 平均 commit lag | ~110ms | **~27ms** |

→ depth 3.3× 低・lag 4× 低・drain +17%。効果は「ある」が **modest**。**Lv9 のバッチ化が既に fsync を 59:1 に
薄めた後**なので、sync-off が奪える残りは少ない。両面とも 0% 503＝consumer は fsync-on でも詰まっていない。

### ② per-row（BATCH_WRITES off）@400VU — fsync が amortize されない領域

| 指標 | sync_commit=on | sync_commit=off |
|---|---|---|
| ピーク write_queue_depth | 100,067（上限張り付き） | 100,085（上限張り付き） |
| write 503（http_req_failed） | 43.17% | 38.04% |
| consumer drain 実績 | 31,143 行 | **47,241 行（+52%）** |
| 平均 commit lag | ~59s | ~43s |

→ fsync を外すと per-row の drain が **+52%**。fsync が per-row commit コストの相当部分だった証拠。
**ただし depth は上限 100k に張り付いたまま・38% はなお 503** ＝ sync-off 単独では per-row を飽和から救い出せない
（producer が単一 per-row consumer を、fsync の有無に関わらず抜く）。fsync 以外の per-row 固定費
（parse/plan・トランザクション・往復）は残るため。

### 結論

- **バッチ化（commit の回数を N→1）は sync_commit=off（commit の cost を削る）より桁違いに強いレバー。**
  同じ fsync という壁を、amortize（回数削減）の方が cost 削減より遥かに効率よく攻める。
- sync_commit=off は実在する二次レバー（per-row +52% / batched +17%）だが、**regime を変えるのはバッチ化**。

> ⚠️ **Docker-on-Mac の注意**: Docker Desktop の Postgres は Linux VM の仮想ディスクに書くため fsync の効き方が
> ベアメタルと異なり、`synchronous_commit=off` の効果は本番 SSD/HDD より小さく出ている可能性が高い。
> また @800VU では箱全体が CPU 飽和（load avg 30–48）し producer 側も律速される——**数値は相対差で読み、
> 絶対値は環境依存**として扱うこと。
