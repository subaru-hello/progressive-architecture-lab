# Lv21 — 2PC 自動 crash-recovery

## Thesis
Lv19/Lv20 の 2PC は、coordinator が「両者 prepare 済」の後・commit/rollback を出す前に死ぬと、
両DBに prepared txn が**行ロックを握って宙吊り(in-doubt)**になり、**手動 `ROLLBACK PREPARED`**(DBA 介入)が要った。

Lv21 は coordinator に **決定ジャーナル(commit point) + 起動時リゾルバ** を入れ、**再起動だけで in-doubt を自動再解決**する。
古典的 2PC coordinator recovery を手作りして、XA / DB 内蔵 2PC coordinator が内部でやっている機構を可視化する。

- **commit point**: 両者 prepare 後、coordinator DB の `tx_journal` に `decision='commit'` を**永続化してから** commit を出す。
  行あり=commit / 行なし=abort(**presumed-abort**: abort レコードは書かない)。
- **起動時リゾルバ**: 再起動で一度だけ、`pg_prepared_xacts`(orders + items は HTTP `/internal/tx/prepared`)と
  `tx_journal` を突合 → journal にあれば両者 commit・無ければ両者 rollback(冪等)。**listen の前に完走**する。
- **deferred-delete**: journal は「両者 commit 確認後だけ」削除。items 未達なら残し次回起動へ持ち越す(split-brain 回避)。

`app/` コードのみ変更(外部 HTTP 契約は不変・内部 endpoint `/internal/tx/prepared` を 1 本追加)。既定 `ORDER_TX_MODE=2pc`。
> host:3000 が他プロセスに使われることがあるため、api は **host:3021** に publish している。

---

## 起動
```bash
docker compose -f stages/21-2pc-recovery/docker-compose.yml up --build -d
# ready: curl -sf localhost:3021/ready
```

## デモ① COMMIT 側リカバリ（journal 書込後にクラッシュ → 再起動で自動 commit）
```bash
CF=stages/21-2pc-recovery/docker-compose.yml
# 1) journal 書込後・commit 前で落とす
FAULT_POINT=after-journal docker compose -f $CF up -d --no-deps api      # ready 待ち
curl -s -X POST localhost:3021/orders -H 'x-auth-token: demo-token-1' \
     -H 'content-type: application/json' -d '{"itemId":3,"qty":1}'       # → HTTP 500
# 宙吊り確認: 両DB prepared=1 / journal に decision=commit
docker compose -f $CF exec -T db       psql -U pal -d pal -c "SELECT gid,decision FROM tx_journal;"
docker compose -f $CF exec -T items-db psql -U pal -d pal -c "SELECT gid FROM pg_prepared_xacts;"
# 2) 再起動（fault 解除）= 起動時リゾルバが走る
FAULT_POINT= docker compose -f $CF up -d --no-deps api
docker compose -f $CF logs api | grep "resolver done"                    # committed=1 aborted=0
# → prepared=0 / journal 空 / orders 増 / stock 減。手動 ROLLBACK PREPARED 不要。
```

## デモ② ABORT 側リカバリ（journal 書込前にクラッシュ → 再起動で自動 rollback）
```bash
FAULT_POINT=after-prepare-all docker compose -f $CF up -d --no-deps api  # ready 待ち
curl -s -X POST localhost:3021/orders -H 'x-auth-token: demo-token-1' \
     -H 'content-type: application/json' -d '{"itemId":3,"qty":1}'       # → HTTP 500・journal 無し
FAULT_POINT= docker compose -f $CF up -d --no-deps api
docker compose -f $CF logs api | grep "resolver done"                    # committed=0 aborted=1
# → prepared=0 / order 据置 / stock 復旧。
```

## 実測サマリ（機能テスト・この box）
| シナリオ | 注入後の状態 | 再起動リゾルバ | 結果 |
|---|---|---|---|
| after-journal（COMMIT 側） | prepared×2 + journal=commit | `committed=1` | order 確定・stock 減・journal 掃除 |
| after-prepare-all（ABORT 側） | prepared×2 + journal 無し | `aborted=1` | rollback・stock 復旧・order 据置 |

起動ログ順: `2pc resolver done` → `Server listening`（= リゾルバは serve より先に完走）。
詳細は [`docs/learning-log/21-lv21.md`](../../docs/learning-log/21-lv21.md) / 教訓40。

## 撤収
```bash
docker compose -f stages/21-2pc-recovery/docker-compose.yml down -v
```
