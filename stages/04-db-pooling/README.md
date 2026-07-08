# Lv4 — 単一DBの壁：コネクション枯渇 → PgBouncer

Lv0→Lv3 で「アプリは水平スケールできる」を登り切った。だが**DB はずっと 1 台**のまま。
この段は、アプリを増やすほど**単一 DB の接続スロット**を食い潰して頭打ちになる壁を再現し、
**コネクションプーラ（PgBouncer）**で解く。

接続の変数を切り分けるため、あえて **compose に戻す**（k8s だと `max_connections` の制御と観測が煩雑）。
本番 k8s なら PgBouncer は Deployment/Service になるだけで、考え方は同じ。

## 壁の算数
- アプリのプールは 1 レプリカあたり `PG_POOL_MAX=15` 本まで DB へ接続を張る。
- レプリカ 4 本 → 最大 **4 × 15 = 60 本**の接続を単一 DB に要求。
- DB は `max_connections=20`（うち予約3で実質 ~17）。60 ≫ 17 → **枯渇**。
- Postgres は溢れた接続に `FATAL: sorry, too many clients already` を返し、アプリは 5xx。

## フェーズA — 壁を再現（直結）
```console
$ docker compose -f stages/04-db-pooling/docker-compose.wall.yml up --build -d
$ docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 grafana/k6 run - < load/db.js
```
`caddy(:8080) → api×4（各 pool 15, db 直結）→ db(max_connections=20)`

## フェーズB — PgBouncer で解く
```console
$ docker compose -f stages/04-db-pooling/docker-compose.wall.yml down
$ docker compose -f stages/04-db-pooling/docker-compose.pgbouncer.yml up --build -d
$ docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 grafana/k6 run - < load/db.js
```
`caddy(:8080) → api×4（各 pool 15）→ pgbouncer(:6432, transaction)→ db(max_connections=20)`

PgBouncer が 60 本の *client 接続* を受け止め（`max_client_conn=1000`）、
DB への *server 接続* は `default_pool_size=15` に集約する（15 ≤ 20 → 枯渇しない）。
`pool_mode=transaction` = トランザクション単位で server 接続を貸し出すので、短い query 多数に最適。

## 実測（8 コア Mac / 100 VUs / 40s / load/db.js）
| | フェーズA 直結（壁） | フェーズB PgBouncer |
|---|---:|---:|
| エラー率 | **7.61%**（3742 / 49134） | **0.00%**（0 / 49283）✓ |
| スループット | 1227 req/s | 1232 req/s |
| p95 | 51ms | 44ms |
| DB `too many clients` | **3751 件** | **0 件** |
| DB への実接続数 | 20 で頭打ち→枯渇 | ~15（default_pool_size）に集約 |

**注目点: スループットはほぼ同じ。** この箱では DB はまだ CPU/IO に余裕があり、
壁は「速度」ではなく**可用性（接続枯渇による 5xx）**だった。PgBouncer が消したのはエラーで、
速度を足したわけではない。＝スケールの壁は throughput とは限らない（Lv0-1 とは別種の壁）。

## リードレプリカを今回やらない理由
読み取りスケール（リードレプリカ）は「実ハードが増える」前提で効く。
このラボは同一 Mac の 1 台なので、レプリカを足しても**同じコアを共有**する（Lv3 の k3d 3ノードと同じ罠）。
接続枯渇の壁は 1 台でも忠実に再現・解消できるので、まず PgBouncer を扱った。
リードレプリカ＋read/write 分離は**実ノードが増える環境（自宅Linux等）で Lv5** として扱う。

## 片付け
```console
$ docker compose -f stages/04-db-pooling/docker-compose.pgbouncer.yml down -v
```
