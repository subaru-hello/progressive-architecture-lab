# Lv18 — ダウンタイムゼロ抽出（strangler-fig）

## 狙い
Lv17 の hex seam(orders→items が ItemsPort 1 本)から **items を専用サービス+専用DBへライブ抽出**。
**Lv16 の泥団子で 100% ダウンした同じ引っ越しを、k6 常時流しで error≈0 で通す**。
phase は `DualWriteItemsAdapter` の **runtime mode**(admin endpoint 切替＝再起動ゼロ)で進める。

## 構成
```
monolith(:3000, SERVICE=all, port モード) --DualWriteItemsAdapter--> items-service(:3001, 別プロセス+別DB)
  monolith DB(db): items+orders+users(seed 済) / items-db: items-service 専用(空で起動→backfill)
  mode: primary_only → dual_write → dual_shadow → secondary_only(cutover)
```

## 起動 / 停止
```bash
docker compose -f stages/18-live-extraction/docker-compose.yml up --build -d
docker compose -f stages/18-live-extraction/docker-compose.yml down -v
```

## strangler playbook（k6 を常時流しながら・全て再起動なしの admin 切替）
```bash
MODE(){ curl -s -XPOST localhost:3000/admin/items-migration/mode -H 'content-type: application/json' -d "{\"mode\":\"$1\"}"; }
K6(){ docker run --rm -i -e BASE_URL=http://host.docker.internal:3000 -e RATE=40 -e DURATION=12s grafana/k6 run - < load/migration.js; }

MODE primary_only;  K6            # P1 baseline → 0%
MODE dual_write;    K6            # P2 両書き(backfill の前に有効化=drift 窓回避) → 0%
curl -s :3000/internal/items/all | curl -s -XPOST :3001/internal/items/bulk-upsert -d @-   # backfill(冪等)
MODE dual_shadow;   K6            # P3 影読み検証: curl :3000/metrics|grep shadow_mismatch → 0
MODE secondary_only; K6           # P4 cutover(再起動なし) → 0% / 以後 orders は items-service の在庫を操作
# naive 対比: 空の items-service に MODE secondary_only で即 cutover → 66.7% down(全 order 409)
```

## 実測（引っ越し中の失敗率＝ダウンタイム）
| アーム | http_req_failed | 備考 |
|---|---|---|
| Lv16 泥団子(naive 移動) | **100%** | 跨ぎ FK/直 JOIN が新 DB を跨げず全滅 |
| **Lv18 hex 規律(フル playbook)** | **0%(全 phase)** | shadow_mismatch=0 で整合確認後に cutover |
| Lv18 hex naive(手順飛ばし) | **66.7%** | 空の新ストアへ即 cutover=在庫あるのに全 order 409 |

## この段の限界（次に進む理由）
- cutover で items が別サービス+別DBになり、POST /orders の在庫(items-db)+order(orders-db)が**2DB跨ぎ**に。
  Lv15 教訓34「原子性が自前の分散tx問題に化ける」窓が本番化。
- → **Lv19** で `ORDER_TX_MODE=none|2pc|saga` を障害注入で比較し、**タダの分散tx は無い**を実測。
