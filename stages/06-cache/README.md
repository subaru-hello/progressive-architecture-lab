# Lv6 — Redis キャッシュ（read パスの高速化）

単一 DB に read 負荷を集中させていた壁に **Redis キャッシュ（cache-aside）** を追加し、
`GET /items` のヒット時に DB クエリ自体を消す。**Lv5 のリードレプリカとの対比が核心。**

> **結論（先に）**: Lv5 の read replica は read 負荷を「別の DB コンテナに移動」するだけで、
> 同じコアを共有する単一ホストではレイテンシが改善しなかった。Redis キャッシュは**ヒット時に
> DB クエリ自体を消す**（Redis GET ≪ Postgres の sort/scan）ため、単一ホストでも DB の read 負荷を
> 実際に削減する。**hit 率 ~76% でも、飽和していた DB が余裕を取り戻し read p95 が激減した。**

## 実測（8 コア Mac / read95:write5 / read-heavy.js、wall↔cache を背中合わせで A/B）
| | wall（cache 無） | cache | 差 |
|---|---:|---:|---:|
| 150 VU read p95 | 161ms | **98ms** | −39% |
| 150 VU throughput | 1037 req/s | **1581 req/s** | +53% |
| 400 VU read p95 | **636ms** | **99ms** | **−84%** |
| 400 VU throughput | 1207 req/s | **3499 req/s** | **+190%** |
| cache hit 率（Redis keyspace） | — | **~76%** | — |

- 400 VU で wall は DB が飽和し read p95 が 636ms まで崩れる。cache は hit がその読み取りを
  肩代わりするので read p95 は 99ms で**ほぼ平坦**、throughput は約 3 倍。
- **注記**: 計測時マシンは他プロセス（ブラウザ）で load avg 6〜14 と変動。**絶対値でなく
  wall↔cache の相対 A/B** として読むこと（cache 側の方がむしろ高負荷時に測れており不利な条件）。
  数字の絶対値は再現時に環境で振れる。

## Lv5（リードレプリカ）との対比
| | Lv5 リードレプリカ | Lv6 Redis キャッシュ |
|---|---|---|
| read 負荷の行き先 | replica（DB クエリは走る） | Redis ヒット時は DB を叩かない |
| 単一ホストでの効果 | 改善なし（コア共有で負荷が「移動」するだけ） | read p95 −84% / throughput +190%（負荷が「消える」） |
| write の扱い | primary のみ | DB へ INSERT + キャッシュ DEL |
| 複雑さ | レプリケーション設定（basebackup 等） | redis サービス 1 個追加 |

## 使い方
### フェーズA — 壁（キャッシュなし）
```bash
docker compose -f stages/06-cache/docker-compose.wall.yml up --build -d
curl -s localhost:8080/ready
docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 -e PEAK_VUS=400 grafana/k6 run - < load/read-heavy.js
docker compose -f stages/06-cache/docker-compose.wall.yml down -v
```
### フェーズB — Redis キャッシュ追加
```bash
docker compose -f stages/06-cache/docker-compose.cache.yml up --build -d
curl -s localhost:8080/ready
# 1回目 miss / 2回目 hit（/cache はプロセス内カウンタなので複数回叩くと動く）
curl -s localhost:8080/items > /dev/null && curl -s localhost:8080/cache
# POST → DEL → 次の GET に新行が反映されることを確認（逐次なら反映される）
curl -s -X POST localhost:8080/items -d '{"name":"lv6-test"}' -H 'content-type: application/json'
curl -s localhost:8080/items | grep lv6-test
# 負荷（PEAK_VUS=400 で DB を飽和させると wall との差が最大化）
docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 -e PEAK_VUS=400 grafana/k6 run - < load/read-heavy.js
# 全体の hit 率は Redis 側で正確に取れる（アプリの /cache は 1 プロセス分のみ）
docker compose -f stages/06-cache/docker-compose.cache.yml exec -T redis redis-cli info stats | grep keyspace
docker compose -f stages/06-cache/docker-compose.cache.yml down -v
```

## 測るもの
| 指標 | 見方 |
|---|---|
| **read p95** | 主指標。wall→cache で大幅低下。飽和した DB を hit が肩代わりする。 |
| **throughput** | ヒット時は DB を経由しないため大きく上がる。 |
| **DB CPU / 接続数** | `docker stats` + `pg_stat_activity`。cache は DB を飽和させない。 |
| **cache hit 率** | **`redis-cli info stats` の keyspace_hits/misses が全体の正確値**。アプリの `/cache` は round-robin で 1/4 しか見えないプロセス内カウンタなので過信しない。 |

## キャッシュ設計の割り切り（＝この段の教材ポイント）
- **hit 率が 0.95 に届かない理由**: read95/write5 でも、write ごとに `DEL items:latest100` で
  単一キーを全消しするため、高負荷では ~7ms に 1 回キャッシュが飛ぶ。実測 hit 率は **~76%**。
  DEL 直後は全レプリカが同時に miss して DB に殺到する（**キャッシュスタンピード**）。
  → キャッシュの効果は「read 比率」でなく「**write による無効化の頻度**」で決まる。
- **cache invalidation の落とし穴（read-after-write 競合）**: `POST` 後の `DEL` はベストエフォート。
  並行 GET が INSERT 前の DB を読み、その古いスナップショットを DEL の**後**に SET すると、
  TTL 切れまで古い値が残る。逐次操作なら反映されるが、並行下では保証されない。
  → 「DEL すれば即整合」は**嘘**。これがキャッシュの一番の難所で、この段が見せたいこと。
- **TTL 30s**: 上記の取りこぼしや Redis 残留に対する安全網（最終的な収束保証）。
- **フォールバック**: Redis 接続エラー時はキャッシュを素通りして DB 直。Redis が死んでも 500 にしない。
- **/ready に Redis を含めない**: Redis はベストエフォート層。DB が生きていれば動くべき。
