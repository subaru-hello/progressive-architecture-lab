# Lv19 — 分散トランザクション（none / 2PC / saga）

## 狙い
Lv18 で items が別サービス+別DBになった後、POST /orders は在庫(items-db)+order(orders-db)の **2DB 跨ぎ**。
orders を coordinator に、`ORDER_TX_MODE=none|2pc|saga` の3方式を**障害注入**で比較し、**タダの分散tx は無い**を実測する。

## 構成
```
api(coordinator: orders+users, DB=db) --HTTP--> items-service(SERVICE=items, DB=items-db)
両 Postgres に max_prepared_transactions=50（2PC の PREPARE TRANSACTION 用）。
mode/fault は env で差し替えて api を作り直す（Lv19 は zero-downtime が目的でないので再起動可）。
```

## 起動 / 計測
```bash
docker compose -f stages/19-distributed-tx/docker-compose.yml up --build -d
CF=stages/19-distributed-tx/docker-compose.yml

# happy path latency（1 VU 逐次＝衝突なしで測る。2PC は高競合で orphan 雪だるま化するため）
for M in none 2pc saga; do
  ORDER_TX_MODE=$M docker compose -f $CF up -d --no-deps --force-recreate api; sleep 7
  docker run --rm -i -e BASE_URL=http://host.docker.internal:3000 -e SCENARIO=faults -e N=60 grafana/k6 run - < load/order-tx.js
done

# 障害注入
FAULT_POINT=after-first-write ORDER_TX_MODE=none  docker compose -f $CF up -d --no-deps --force-recreate api  # → 恒久リーク
FAULT_POINT=after-prepare-all ORDER_TX_MODE=2pc   docker compose -f $CF up -d --no-deps --force-recreate api  # → in-doubt
FAULT_POINT=after-first-write ORDER_TX_MODE=saga  docker compose -f $CF up -d --no-deps --force-recreate api  # → poller 補償
# in-doubt 確認: docker compose -f $CF exec items-db psql -U pal -d pal -c "SELECT gid FROM pg_prepared_xacts;"
# 掃除: ROLLBACK PREPARED '<gid>';
```

## 実測（3方式の比較）
| 指標 | none | 2pc(本物 PG prepared-tx) | saga(orchestration+outbox) |
|---|---|---|---|
| happy order_latency med | **3.42ms** | 5.46ms(最遅) | 5.04ms |
| 障害注入時の一貫性 | **恒久リーク**(在庫-20/order0) | **in-doubt**(両DB prepared 宙吊り) | **結果整合**(補償で自己回復~6s) |
| ロック/ブロッキング | 無し | **行ロック保持** | **無し** |
| 回復 | 無し | **手動**(ROLLBACK PREPARED) | **自動**(poller) |
| 中間状態の可視性 | — | 不可視 | **可視**(reserved) |
| 実装複雑度 | 最小 | 中 | 大(冪等鍵+補償+poller) |

## 結論（引っ越し軸の締め）
- **2PC は原子性(C)をブロッキング+in-doubt で買う（A を捨てる）／saga は可用性(A)を中間状態の可視化+補償の実装コストで
  買う（I を捨てる）**。切った後に残る cross-DB 原子性はこの二択で、タダの正解は無い。
- 2PC の脆さを実地で踏んだ: commit の空 body で Fastify 400 → prepared が orphan 化 → 行ロックが溜まり全滅（雪だるま）。
  高競合でも prepared ロック衝突で `max_prepared_transactions` を食い潰す＝**2PC はスループットと相性が悪い**。
- 未消化: 実 network 上のコスト・saga choreography・2PC 自動 crash-recovery・outbox exactly-once。
