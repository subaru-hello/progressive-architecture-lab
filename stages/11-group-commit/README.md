# Lv11 — 並列 committer + group commit：durability を保ったまま fsync を束ねる

## 狙い

Lv9 で「DB write の律速は **commit の回数**（1 commit ごとの fsync 固定費）」、Lv10 で「fsync の *コスト* は
`synchronous_commit=off` で削れるが **durability を失う**。しかも **単一 committer では `commit_delay` が発火しない**」
と分かった。Lv11 はその宿題を回収する——**durability を保ったまま（`synchronous_commit=on` のまま）fsync を
amortize できるか？**

PostgreSQL の group commit（`commit_delay` / `commit_siblings`）は、commit record を書いた瞬間に他に
`commit_siblings` 本のアクティブ tx があれば `commit_delay` マイクロ秒だけ待ち、その間に揃った複数 commit の
WAL flush を **1 回の fsync に束ねる**。効かせる条件は「複数の committer が同時に commit へ到達していること」。

```
Lv9/10 : consumeLoop は processBatch を await してから次を読む
         → commit は常に 1 本ずつ = 単一 committer → commit_delay は発火しない
Lv11   : consumer 内に COMMITTERS 本の committer coroutine を並行起動
         → 別々の pg コネクションで commit が同時 in-flight → group commit の母集団ができる
```

- `COMMITTERS=N`（既定 1）: `main()` で `consumeLoop` coroutine を N 本 `Promise.all` で回す。各 coroutine は
  同一 `CONSUMER_NAME` で `XREADGROUP('>')` する。Redis は 1 entry を group 内の 1 呼び出しにしか配らない
  （同一 consumer への並行呼び出しでも disjoint）ので**重複配送しない**。
- `COMMIT_DELAY_US=<us>`（既定 0）/ `COMMIT_SIBLINGS=<n>`（既定 5）: `writePool` の libpq startup option
  `-c commit_delay=.. -c commit_siblings=..` として渡す（Lv10 の `synchronous_commit=off` と同じ options 合成に統合）。
- **`commit_delay` は `synchronous_commit=on` のときだけ意味を持つ**（off だと backend は fsync を待たないので
  束ねる対象が無い）。だから Lv11 は `SYNC_COMMIT_OFF` を付けない＝**Lv10 と違い durability を落とさない**。

## Lv10 との対比

| | Lv10（sync_commit=off） | Lv11（group commit） |
|---|---|---|
| fsync への攻め方 | 1 commit の fsync 待ちを**外す** | 複数 commit の fsync を**束ねる** |
| durability | 落ちる（クラッシュで直近数百 ms 消失） | **保つ**（commit は fsync 完了を待つ） |
| 必要な前提 | なし | **並列 committer**（単一だと発火しない） |

## 起動

```bash
docker compose -f stages/11-group-commit/docker-compose.queue.yml up --build -d
docker compose -f stages/11-group-commit/docker-compose.queue.yml down -v
```

環境変数（compose 既定）: `COMMITTERS=8` / `COMMIT_DELAY_US=200` / `COMMIT_SIBLINGS=2`、`synchronous_commit=on`。

## A/B 比較手順

`synchronous_commit=on` の Lv9（batched・単一 committer・commit_delay=0）を baseline に、飽和する高負荷で背中合わせに回す。

```bash
# --- Baseline: Lv9 batched, sync_commit=on, 1 committer, commit_delay=0 ---
docker compose -f stages/09-batch-insert/docker-compose.queue.yml up --build -d
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e PEAK_VUS=800 \
  grafana/k6 run - < load/write-heavy.js
docker compose -f stages/09-batch-insert/docker-compose.queue.yml down -v

# --- Lv11 batched, sync_commit=on, N committers + commit_delay ---
docker compose -f stages/11-group-commit/docker-compose.queue.yml up --build -d
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e PEAK_VUS=800 \
  grafana/k6 run - < load/write-heavy.js
docker compose -f stages/11-group-commit/docker-compose.queue.yml down -v
```

切り分けのため「並列 committer だけ（commit_delay=0）」も測ると、効いているのが *並列化* か *group commit* かを分離できる。

## 観測方法

```bash
# depth（どの API からでも取れる）
curl -s localhost:8080/metrics | grep write_queue_depth
# commit lag + batch size（consumer プロセス内）
docker compose -f stages/11-group-commit/docker-compose.queue.yml \
  exec consumer wget -qO- localhost:3001/metrics | grep write_queue
```

## 実測結果

_（測定は docs/learning-log/11-lv11.md に記録。box の load が落ち着いてから背中合わせ A/B で実測する。）_

> ⚠️ **Docker-on-Mac の注意**: Docker Desktop の Postgres は Linux VM の仮想ディスクに書くため fsync の効き方が
> ベアメタルと異なる。group commit の効果も本番 SSD とはズレて出うる。また高 VU では箱全体が CPU 飽和し
> producer 側も律速される——**数値は相対差で読み、絶対値は環境依存**として扱うこと。
