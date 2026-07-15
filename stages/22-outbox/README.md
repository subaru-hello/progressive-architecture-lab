# Lv22 — transactional outbox（exactly-once）

## Thesis
`none` モード = decrement(items へ HTTP)→ order insert(orders-db) の**別々の2書込**＝ dual-write。
障害で「stock 減・order 無し(lost)」or「order 有・decrement 無し(ghost)」——「ローカル DB commit と外部通知」に原子性が無い。

Lv22 は **transactional outbox** でこれを潰す（`ORDER_TX_MODE=outbox`・app コードのみ・外部契約不変）:
1. **原子的 emit**: order 作成 = 同一 orders-db tx で `orders INSERT` + `outbox INSERT(msg_id)`。dual-write 窓が消える。
2. **at-least-once 配送**: poller(2s)が `delivered_at IS NULL` の outbox を items receiver へ HTTP 配送、成功で mark。未配送は残り再送。
3. **冪等 receiver**: items 側 `applyDecrementIdempotent` = 同一 items-db tx で `INSERT processed_messages ON CONFLICT DO NOTHING`
   → 新規なら stock 更新・既処理なら no-op。

**exactly-once 効果 = at-least-once 配送 × 冪等消費者**（配送は何度でも起こりうるが適用は 1 回）。
> host:3000 が他プロセスに使われることがあるため、api は **host:3022** に publish している。

---

## 起動
```bash
docker compose -f stages/22-outbox/docker-compose.yml up --build -d
```

## デモ① happy（原子的 emit → poller が配送 → stock 1回だけ減）
```bash
CF=stages/22-outbox/docker-compose.yml
curl -s -X POST localhost:3022/orders -H 'x-auth-token: demo-token-1' \
     -H 'content-type: application/json' -d '{"itemId":3,"qty":5}'          # → 201。即時は stock 未変化
# ~2s 後: 配送されて stock -5・outbox delivered・processed_messages 1件
docker compose -f $CF exec -T db       psql -U pal -d pal -c "SELECT msg_id,delivered_at FROM outbox;"
docker compose -f $CF exec -T items-db psql -U pal -d pal -c "SELECT stock FROM items WHERE id=3;"
docker compose -f $CF exec -T items-db psql -U pal -d pal -c "SELECT count(*) FROM processed_messages;"
```

## デモ② exactly-once（配送後・mark前にクラッシュ → 再配送されても二重引きしない）
```bash
FAULT_POINT=after-deliver-before-mark docker compose -f $CF up -d --no-deps api   # ready 待ち
curl -s -X POST localhost:3022/orders -H 'x-auth-token: demo-token-1' \
     -H 'content-type: application/json' -d '{"itemId":3,"qty":5}'                # → 201
# poller が配送→throw→再配送を繰り返す(logs に after-deliver-before-mark warn が複数)
# しかし stock は 5 だけ減る(再配送は processed_messages で dedup)・outbox は未配送のまま stuck
docker compose -f $CF logs api | grep -c after-deliver-before-mark               # 複数回
FAULT_POINT= docker compose -f $CF up -d --no-deps api                           # 再起動→再配送→dedup→mark
# → outbox 全配送・stock は動かない(据置)。
```

## 実測サマリ（機能テスト・この box）
| シナリオ | 挙動 | stock(item3) | 判定 |
|---|---|---|---|
| happy | emit 原子的 → poller 配送 | 1000000 → 999995（-5・1回） | ✅ |
| after-deliver-before-mark | poller が **3回再配送**・全て dedup | 999995 → 999990（-5・**1回のみ**） | ✅ 二重引きなし |
| 再起動(fault 解除) | stuck message が 再配送→dedup→mark に収束 | 999990（据置） | ✅ |

詳細は [`docs/learning-log/22-lv22.md`](../../docs/learning-log/22-lv22.md) / 教訓41。

## 撤収
```bash
docker compose -f stages/22-outbox/docker-compose.yml down -v
```
