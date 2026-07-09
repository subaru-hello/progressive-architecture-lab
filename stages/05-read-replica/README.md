# Lv5 — リードレプリカ + read/write 分離

同じアプリを **primary + read replica**（Postgres ストリーミングレプリケーション）に載せ、
`GET`(SELECT) を replica、`POST`/DDL を primary に振り分ける。狙いは「read を逃がして primary をオフロードする」こと。

> **結論（先に）**: 分離は primary を確実にオフロードする（CPU 130%→22%）が、**単一ホストでは
> client 視点のレイテンシ・スループットは改善しない**。read の CPU 負荷が同じコアを共有する replica に
> 移るだけで、壁は消えず「移動」する。詳細な実測は [`docs/learning-log/05-lv5.md`](../../docs/learning-log/05-lv5.md)。

## 構成
- **アプリ**: `writePool`→primary / `readPool`→replica の 2 pool（`app/src/server.ts`）。
  `DATABASE_URL_RO` 未設定なら `DATABASE_URL` に fallback するので Lv0-4 の compose はそのまま動く。
- **primary**: `postgres:16-alpine`。`wal_level=replica` 等 + replication ロール/`pg_hba`（`primary/init-replication.sh`）。
- **replica**: `pg_basebackup -R` で standby 初期化（`replica/entrypoint.sh`、`gosu` で postgres 降格）。
- **caddy**: api レプリカ群を round_robin する HTTP 前段（DB 前段には LB を置かない）。

## 使い方

### フェーズA — 壁（分離なし）
read も write も single primary に集中させる。
```bash
docker compose -f stages/05-read-replica/docker-compose.wall.yml up --build -d
docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 grafana/k6 run - < load/read-heavy.js
docker compose -f stages/05-read-replica/docker-compose.wall.yml down -v
```

### フェーズB — 分離（replica に read を逃がす）
```bash
docker compose -f stages/05-read-replica/docker-compose.replica.yml up --build -d
# streaming 成立を確認（1 行返れば OK）
docker compose -f stages/05-read-replica/docker-compose.replica.yml exec -T db-primary \
  psql -U pal -d pal -c "SELECT client_addr, state FROM pg_stat_replication;"
# 負荷（PEAK_VUS=400 で primary を飽和させると壁の「移動」が見える）
docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 -e PEAK_VUS=400 \
  grafana/k6 run - < load/read-heavy.js
docker compose -f stages/05-read-replica/docker-compose.replica.yml down -v
```

## 測るもの（主指標は write の p95 — total throughput ではない）
| 指標 | 見方 |
|---|---|
| **write p95** | 分離しても単一ホストでは改善しない（コア共有）。主指標。 |
| **primary CPU / 接続数** | `docker stats` + `pg_stat_activity`。分離で 130%→22%、67→28 にオフロード（本物の効果）。 |
| **replica CPU** | 分離時、read 負荷を引き受けて飽和する（＝負荷が「移動」した証拠）。 |
| **レプリケーションラグ** | `curl localhost:8080/replication`。**負荷中**に測る（アイドル時の値は青天井に増えるアーティファクト）。 |

## 注意（コア共有の罠）
単一 Mac では primary も replica も同じ物理コアを共有する。read の水平スケール（分離の真価）を
実測したいなら replica を**別マシン**に置くしかない。これは Lv3 の「k3d 複数ノードは実コアを増やさない」
（教訓11）が DB 層で再来しただけ。分離という**パターンは正しい**が、**効果はトポロジで決まる**。
