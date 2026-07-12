# Lv14 — modular monolith（env 1 個差で結合を切る）

## 狙い
Lv13 と**同一トポロジ**（1 プロセス・共有 1 DB・同一 image）。差は env 1 個 `ORDERS_CROSS_CONTEXT: join→port`。
orders は items テーブルを直接触らず **in-process の `ItemsPort`（関数呼び出し）越し**に在庫を引く。
この `ItemsPort` interface は **Lv15 micro と同一**（Lv14=in-process アダプタ / Lv15=HTTP アダプタを挿すだけ）。
**「境界を引くコストは runtime でなく設計規律で払う」**を数値で確かめる段。

## 構成
```
[ localhost:3000 ] --> api (SERVICE=all, ORDERS_CROSS_CONTEXT=port) --> db (共有 postgres:16)
                       orders → ItemsPort.decrementStock()（in-process 関数呼び出し・items 表は触らない）
```

## 起動 / 停止
```bash
docker compose -f stages/14-modular-monolith/docker-compose.yml up --build -d
docker compose -f stages/14-modular-monolith/docker-compose.yml down -v
```

## 動作確認
```bash
curl -s -XPOST localhost:3000/orders -H 'x-auth-token: demo-token-2' -H 'content-type: application/json' -d '{"itemId":5,"qty":2}'
curl -s localhost:3000/items/5     # stock -2（ポート越しでも在庫は正しく減る）
```

## 実測（Lv13 と同条件・背中合わせ）
| 指標 | Lv13(join) | Lv14(port) | 差 |
|---|---|---|---|
| order_latency med | 5.34ms | **5.38ms** | +0.04ms（＝ノイズ） |
| order_latency p95 | 7.89ms | **8.13ms** | +0.24ms |

**latency は Lv13 と実質一致**——port でも同一プロセス内の関数呼び出しなので network も直列化も無い。
**modular 化は runtime コストがゼロ**。なのに orders は items の schema を知らなくなった（契約 `ItemsPort` だけ）＝
items 内部を作り変えても無傷（monolith(join) は同じ変更で壊れる）。**この非対称が対価回収**。

## この段の限界（次に進む理由）
- 独立デプロイ/スケール/障害隔離はまだ無い（1 プロセスのまま）。境界はレビューでしか守れない（物理的な壁が無い）。
- → **Lv15** でプロセスを 3 つに割り、ポートを HTTP アダプタに差し替え、DB-per-service にして、
  **network 税 + 分散データ整合の痛み + blast radius 隔離**を初めて数値で回収する。
