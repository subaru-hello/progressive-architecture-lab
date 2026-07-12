# Lv13 — monolith（多ドメイン app のベースライン）

## 狙い
分解軸（monolith ↔ modular ↔ micro）を「本物」に測るため、単一 `items` だった app を
**items + orders + users の 3 ドメイン**に拡張し、**cross-context 依存 2 本**（orders→users 認証 /
orders→items 在庫）を作る。Lv13 は **1 プロセス・共有 1 DB・直 SQL JOIN** の monolith = **税ゼロの基準点**。
ドメインは**ポート&アダプタで一度だけ**書き、以降 Lv14/15 は**配線（アダプタ+env）だけ**差し替える。

## 構成
```
[ localhost:3000 ] --> api (SERVICE=all, ORDERS_CROSS_CONTEXT=join) --> db (共有 postgres:16)
                       orders は items テーブルを直 SQL で JOIN/UPDATE（in-process・同一 tx で原子的）
```

## 起動 / 停止
```bash
docker compose -f stages/13-monolith/docker-compose.yml up --build -d
docker compose -f stages/13-monolith/docker-compose.yml down -v
```

## 動作確認
```bash
TOKEN=demo-token-1
curl -s localhost:3000/items            # 後方互換 list（stock 無し）
curl -s localhost:3000/items/1          # :id で stock 可視
curl -s -XPOST localhost:3000/orders -H "x-auth-token: $TOKEN" -H 'content-type: application/json' -d '{"itemId":1,"qty":3}'
curl -s localhost:3000/items/1          # stock が 3 減る
# エラーパス: no token→401 / qty 過大→409(在庫不足) / qty=1.5→400
```

## 実測（RATE=50, 30s, POST /orders）
| order_latency med | p95 | idle mem | 失敗率 |
|---|---|---|---|
| **5.34ms** | 7.89ms | ~53MiB(api+db) | 0.00% |

## この段の限界（次に進む理由）
- orders が items テーブルの schema を直接知っている（**結合**）。items を作り変えると orders が壊れる。
- 1 プロセスなので独立デプロイ/スケール不可・障害は全ドメイン全滅。
- → **Lv14** で、同じ 1 プロセスのまま in-process ポート越しに付け替え、**結合を runtime 無料で切れるか**を測る。
