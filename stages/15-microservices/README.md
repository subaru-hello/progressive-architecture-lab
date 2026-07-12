# Lv15 — microservices（分解の対価を回収する）

## 狙い
プロセスを 3 つに割り（items/orders/users-api）、**DB-per-service**（各サービス専用 Postgres）にし、
`ItemsPort`/`UsersPort` を **HTTP アダプタ**に差し替える。orders-api は order 作成のたびに
users-api へ認証 / items-api へ在庫を **HTTP で網越し**に呼ぶ。ドメインコードは Lv13/14 と**同一 image**。
分解の対価（network 税・原子性喪失・blast radius 隔離）を数値で回収する段。

## 構成
```
[ localhost:8080 ] --> caddy(gateway) --> items-api  --> items-db
                                     \--> orders-api --> orders-db
                                     \--> users-api  --> users-db
      orders-api --HTTP--> items-api(/internal/...) / users-api(/internal/auth)   ← cross-context が網越し
      DB-per-service なので orders は items を JOIN できない（在庫は items-db・order は orders-db の別 tx）
```

## 起動 / 停止
```bash
docker compose -f stages/15-microservices/docker-compose.yml up --build -d
docker compose -f stages/15-microservices/docker-compose.yml down -v
```

## 動作確認
```bash
TOKEN=demo-token-1
curl -s -XPOST localhost:8080/orders -H "x-auth-token: $TOKEN" -H 'content-type: application/json' -d '{"itemId":3,"qty":4}'
curl -s localhost:8080/items/3         # items-db の在庫が網越しに減る
# blast radius:
docker compose -f stages/15-microservices/docker-compose.yml stop users-api
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/items     # 200（items-api 独立→生存）
curl -s -o /dev/null -w '%{http_code}\n' -XPOST localhost:8080/orders -H "x-auth-token: $TOKEN" -d '{"itemId":3,"qty":1}'  # 500（auth 依存喪失）
docker compose -f stages/15-microservices/docker-compose.yml start users-api   # 復帰で 201 に戻る
```

## 実測（Lv13-15 背中合わせ集約）
| 指標 | Lv13 monolith | Lv14 modular | Lv15 micro |
|---|---|---|---|
| order_latency med | 5.34ms | 5.38ms | **5.95ms** |
| order_latency p95 | 7.89ms | 8.13ms | **10.71ms** |
| idle 常駐メモリ | ~53MiB | ~53MiB | **~177MiB(3.3×)** |
| 原子性(在庫↔order) | 1 tx 原子的 | 1 tx 原子的 | **別 DB=非原子** |
| blast radius | 全滅 | 全滅 | **隔離(/items 生存)** |

**latency 階段は出るが小さい**（micro は 2 HTTP hop で med +0.6ms）——**loopback なので network 税は下限値**、
実 network なら各 hop 1-10ms で 10-100×。**本当の対価は latency でなく idle 3.3×・原子性喪失（別 DB 跨ぎの
在庫リーク窓）・運用複雑性**。見返りが **blast radius 隔離**。分解の是非は「latency」でなく
「独立性がその運用コストに見合うか」で決まる。

## 実装メモ / 罠
- **Docker-on-Mac 歪み**: loopback HTTP は実 network より速い。数値は「差の向きと一貫性」で読み絶対値は割り引く。
  **load<5 の窓で背中合わせ**（box load でテールが荒れる）。
- gateway は外向き path だけ公開・`/internal/*` は晒さない。
- `ORDERS_CROSS_CONTEXT=join`+`*_SERVICE_URL` は起動時 throw（micro の orders-db に items 表は無い＝fail-fast）。
- **本ラボは補償(saga)を意図的に書いていない**——別 DB 跨ぎの非原子性という痛みを見せるため。
