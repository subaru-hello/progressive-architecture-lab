# Lv23 — choreography saga

## Thesis
Lv19 の saga は **orchestration**（`createOrderSaga()` 1 関数に reserve→confirm/compensate の全フロー・中央指揮）。
Lv23 は **choreography**（`SAGA_STYLE=choreography`）：中央指揮者を消し、各サービスがイベントを発行し合って状態遷移する。
Lv22 の outbox/inbox を**両サービスに**置き、双方向にする。

```
POST /orders → orders: order(status=pending) + OrderCreated を emit（同一tx）
  → orders poller → items /internal/choreo/order-events (OrderCreated)
    → items: 在庫チェック → StockReserved | StockRejected を emit（同一tx・補償の起点）
      → items poller → orders /internal/choreo/stock-events (StockReserved/Rejected)   ★ items→orders コールバック=双方向結合
        → orders: status pending → confirmed | cancelled
```

**フローがどの単一関数にも無い**（server.ts + orders{repo,ports,routes,usecase} + items{repo,ports,routes} の 8 箇所に散る）。
これが orchestration との定義的な差。`SAGA_STYLE=orchestration` は Lv19 の挙動を完全に維持。
> 双方向配線: items-service に `ORDERS_EVENTS_URL=http://api:3000` を渡す（choreography 専用）。api は host:3023 に publish。

---

## 起動
```bash
docker compose -f stages/23-choreography/docker-compose.yml up --build -d
```

## デモ① happy（OrderCreated → StockReserved → confirmed）
```bash
CF=stages/23-choreography/docker-compose.yml
curl -s -X POST localhost:3023/orders -H 'x-auth-token: demo-token-1' \
     -H 'content-type: application/json' -d '{"itemId":3,"qty":5}'      # → 201 {status:"pending"}
# 数秒後: status=confirmed・stock -5
docker compose -f $CF exec -T db psql -U pal -d pal -c "SELECT id,status FROM orders;"
docker compose -f $CF exec -T db       psql -U pal -d pal -c "SELECT event_type,delivered_at FROM choreo_outbox;"  # OrderCreated
docker compose -f $CF exec -T items-db psql -U pal -d pal -c "SELECT event_type,delivered_at FROM choreo_outbox;"  # StockReserved
```

## デモ② reject/補償（在庫不足 → StockRejected → cancelled）
```bash
curl -s -X POST localhost:3023/orders -H 'x-auth-token: demo-token-1' \
     -H 'content-type: application/json' -d '{"itemId":3,"qty":2000000}'   # 在庫 < qty
# 数秒後: status=cancelled・stock 不変（減算せず reject＝補償）
```

## 対比: orchestration 版
```bash
# 片方向（items→orders コールバック無し・全フローが createOrderSaga() の1関数）
SAGA_STYLE=orchestration docker compose -f $CF up -d --no-deps api
```

## 実測サマリ（機能テスト・この box）
| シナリオ | イベントフロー | 結果 |
|---|---|---|
| happy | OrderCreated → StockReserved → confirmed | status=confirmed・stock -5 |
| reject | OrderCreated → StockRejected → cancelled | status=cancelled・stock 不変（補償） |
| SERVICE=all 起動 | 受信 endpoint を order-events/stock-events に分離 | ✅ 重複ルート衝突なし |

詳細・トレードオフ考察は [`docs/learning-log/23-lv23.md`](../../docs/learning-log/23-lv23.md) / 教訓42。

## 撤収
```bash
docker compose -f stages/23-choreography/docker-compose.yml down -v
```
