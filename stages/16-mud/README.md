# Lv16 — 泥団子(big ball of mud) monolith と「動かせなさ」

## 狙い
「引っ越し軸」の出発点。故意に絡ませた**本物の泥団子**(`ARCH=mud`)を作り、**「引っ越しやすさ＝境界の結合度」**を実測する。
外部 HTTP は hex と同一・**違うのは内部構造だけ**。cross-domain の共有 tx / JOIN / 跨ぎ FK が、items のライブ抽出を
物理的に阻む——「泥団子は綺麗にする前には live で動かせない」を数字で確定する。

## 構成
```
[ localhost:3000 ] --> api (ARCH=mud, SERVICE=all) --> db (共有 1 Postgres・全ドメイン同居)
  POST /orders : items在庫 + orders + users.last_order_at を 1 tx で 3テーブル横断（共有 tx）
  GET  /orders : orders JOIN items JOIN users（cross-domain JOIN）
  schema       : orders.item_id/user_id が items(id)/users(id) への跨ぎ FK
```

## 起動 / 停止
```bash
docker compose -f stages/16-mud/docker-compose.yml up --build -d
docker compose -f stages/16-mud/docker-compose.yml down -v    # 実演Bは破壊的なので必ず -v
```

## 動作確認 & 結合度カウント
```bash
TOKEN=demo-token-1
curl -s -XPOST localhost:3000/orders -H "x-auth-token: $TOKEN" -H 'content-type: application/json' -d '{"itemId":1,"qty":2}'
curl -s localhost:3000/orders     # item_name/user_name まで JOIN で埋まる（結合の証拠）
grep -nE "JOIN (items|users)|REFERENCES (items|users)" app/src/mud/*.ts   # 静的な結合度
```

## 「動かせなさ」の実演（破壊的・使い捨てDB）
```bash
PSQL="docker compose -f stages/16-mud/docker-compose.yml exec -T db psql -U pal -d pal"
# 実演A: 跨ぎ FK が items の除去/移動を拒否
$PSQL -c "DROP TABLE items;"        # ERROR: constraint orders_item_id_fkey depends on it
# 実演B: items を別DBへ「引っ越した」状況を再現 → 直SQL/JOIN が即死
$PSQL -c "ALTER TABLE orders DROP CONSTRAINT orders_item_id_fkey; ALTER TABLE items RENAME TO items_relocated;"
curl -s -o /dev/null -w '%{http_code}\n' -XPOST localhost:3000/orders -H "x-auth-token: $TOKEN" -d '{"itemId":1,"qty":1}'  # 500
# k6 でダウンタイム定量化:
docker run --rm -i -e BASE_URL=http://host.docker.internal:3000 -e PHASE=broken -e RATE=40 -e DURATION=15s grafana/k6 run - < load/migration.js
```

## 実測（RATE=40, 15s）
| | baseline(正常) | broken(items 移動後) |
|---|---|---|
| http_req_failed | 0.00% | **100.00%（843/843）** |
| order_create_5xx | 0 | 601（全 order 5xx） |
| order_latency med | 5.85ms | — |

結合度: 共有 tx **1** / cross-domain JOIN **1** / 跨ぎ FK **2**。items 除去は**不可**（FK が拒否）。

## この段の限界（次に進む理由）
- mud は遅くない・壊れてもいない、**ただ移行可能性がゼロ**。結合が新 DB を跨げない縫い目になっている。
- → **Lv17** で mud を hexagonal(DB/Domain/Usecase/Infra)に解きほぐし、結合度を seam 1 本に落とす。
  Lv18 で「Lv16 の 100% ダウンと同じ引っ越しが、今度はダウンタイムゼロで通る」ことを示す。
