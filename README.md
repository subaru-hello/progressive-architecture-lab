# progressive-architecture-lab

同じアプリを、**個人開発レベルの軽い環境から DAU 100+ の重いコンテナワークロードまで**、
段階的に基盤を乗せ替えながらスケールさせていく練習用リポジトリ。

> 方針: **セルフホスト / 実費ゼロ**（AWS 不使用）。ローカル Docker と k3s(=k3d) だけで全段を体験する。
> 自宅の Linux サーバーに移す場合は、k3d の代わりに本物の k3s (`curl -sfL https://get.k3s.io | sh`) を使えば同じマニフェストがそのまま動く。

## はしご（移行ロードマップ）

```
Lv0 docker-compose      Lv1 compose + proxy       Lv2 k3s 単一ノード        Lv3 k8s 複数ノード
────────────────────    ────────────────────      ────────────────────      ────────────────────
個人開発・1プロセス  →   複数レプリカ + LB     →   Kubernetes 入門       →   HPA / HA / DAU100+
最小構成                水平スケールの初歩         宣言的デプロイ            自動スケール・耐障害

                        … そして Lv4: アプリを増やしても単一 DB は接続数で頭打ち → PgBouncer で解く
                        … さらに Lv5: リードレプリカで read/write 分離 → 壁は消えず「移動」する（単一ホストの限界）
                        … そして Lv6: Redis キャッシュ → read 負荷を「消す」と単一ホストでも効く（p95 −84%）
                        … そして Lv7: worker_threads → CPU をイベントループから剥がし probe 雪崩を根治（/health 失敗 46%→0%）
                        … そして Lv8: write-behind キュー → write の壁は「消えず」バッファに移る（耐久 throughput は consumer drain 律速）
                        … そして Lv9: INSERT バッチ化 → commit を N→1 に畳み consumer 1 のまま drain を 6.4×（503 54.84%→0%）
                        … そして Lv10: synchronous_commit=off → fsync の「回数」でなく「cost」を削る二次レバー（per-row drain +52% だが regime は変えられず、durability を払う）
                        … そして Lv11: 並列 committer + group commit → durability を保ったまま fsync を束ねる試み（実装のみ・A/B 実測は延期）
                        … そして Lv12: scale-to-zero(KEDA) → serverless の肝をローカル再現。idle コストを cold-start latency に変換（cold 4.7s vs warm 41ms、warm 定常は一致）
```

各段は **同じ `app/`（Node/TS + Postgres API）** を使い回す。基盤だけが変わる。

## 共有アプリ `app/`

Fastify + node-postgres + prom-client の最小 API。スケール練習に必要な仕掛けを内蔵：

| エンドポイント | 用途 |
|---|---|
| `GET /health` | Liveness（依存なし・k8s livenessProbe 用） |
| `GET /ready` | Readiness（DB 疎通確認・k8s readinessProbe 用） |
| `GET /metrics` | Prometheus メトリクス（各レスポンスに `instance` ラベル） |
| `GET /work?ms=&cpu=` | 負荷シミュレータ（`ms`=待機, `cpu`=CPU バーン）→ スケール実験の駆動源。Lv7 では `WORKER_POOL_SIZE` 設定時 `cpu` を worker_threads へ offload（飽和時 503 背圧） |
| `GET/POST /items` | Postgres への CRUD（Lv6 では GET が Redis cache-aside、Lv8 で `ASYNC_WRITE` 時 POST は Redis Stream へ enqueue して 202） |
| `GET /replication` | レプリケーションラグ秒（Lv5・replica でのみ有意） |
| `GET /cache` | キャッシュ hit/miss（Lv6・プロセス内カウンタ） |
| すべてのレスポンス | `instance` フィールドに処理したコンテナ/Pod 名 → **ロードバランシングが目視できる** |

## 各ステージ

| Lv | ディレクトリ | 使う道具 | 学ぶこと | 状態 |
|---|---|---|---|---|
| 0 | [`stages/00-local-compose`](stages/00-local-compose) | docker compose | コンテナ化・DB 連携・ヘルスチェック | ✅ 動作確認済 |
| 1 | [`stages/01-compose-proxy`](stages/01-compose-proxy) | compose + Caddy | 複数レプリカ・LB・水平スケールの限界 | ✅ 動作確認済 |
| 2 | [`stages/02-k3s-single`](stages/02-k3s-single) | k3d + kubectl | Deployment/Service/Ingress・自己修復・probe 事故 | ✅ 動作確認済 |
| 3 | [`stages/03-k8s-multi`](stages/03-k8s-multi) | k3d 複数ノード + HPA | 自動スケール(2→10)・ノード分散・エラー0% | ✅ 動作確認済 |
| 4 | [`stages/04-db-pooling`](stages/04-db-pooling) | compose + PgBouncer | 単一DBの接続枯渇の壁・コネクションプーリング(7.61%→0%) | ✅ 動作確認済 |
| 5 | [`stages/05-read-replica`](stages/05-read-replica) | compose + streaming replication | read/write 分離・primary オフロード・壁は「移動」する（コア共有） | ✅ 動作確認済 |
| 6 | [`stages/06-cache`](stages/06-cache) | compose + Redis | cache-aside で read 負荷を「消す」(p95 −84%)・invalidation の難所 | ✅ 動作確認済 |
| 7 | [`stages/07-worker-threads`](stages/07-worker-threads) | compose + worker_threads | CPU を別スレッドへ offload・probe 雪崩を単一 replica で根治(/health 失敗 46%→0%)・背圧(503) | ✅ 動作確認済 |
| 8 | [`stages/08-async-queue`](stages/08-async-queue) | compose + Redis Stream | write-behind で 202 即返し・耐久 throughput は consumer drain 律速(バッファ≠乗数)・at-least-once・背圧(503) | ✅ 動作確認済 |
| 9 | [`stages/09-batch-insert`](stages/09-batch-insert) | 08 + multi-row INSERT | commit を N→1 に畳んで drain 6.4×(consumer 1 固定)・律速は行数でなく commit 回数・poison/宙吊りは batch size 倍に増幅 | ✅ 動作確認済 |
| 10 | [`stages/10-sync-commit`](stages/10-sync-commit) | 09 + synchronous_commit=off | fsync の「cost」を削る二次レバー(per-row drain +52% / batched +17%)・回数削減=バッチ化には敵わず regime は変えられない・durability を払う | ✅ 動作確認済 |
| 11 | [`stages/11-group-commit`](stages/11-group-commit) | 09 + 並列 committer + commit_delay | durability を保ったまま group commit で fsync を束ねる試み・並列 committer が要る(単一では commit_delay 不発) | ⏳ 実装のみ・A/B 実測は延期 |
| 12 | [`stages/12-scale-to-zero`](stages/12-scale-to-zero) | k3d + KEDA http-add-on | scale-to-zero ↔ always-on・idle コストを cold-start latency に変換(cold 4.7s vs 41ms・warm と burst は一致)・制御面は常駐固定費 | ✅ 動作確認済 |

## 負荷試験

各段のボトルネックを可視化するため、`grafana/k6` の Docker イメージで負荷をかける（インストール不要）：

```bash
docker run --rm -i grafana/k6 run - < load/smoke.js
```
（コンテナからホストの API は `host.docker.internal` で届く。ポートを変える場合は
`-e BASE_URL=http://host.docker.internal:PORT` を渡す）

## クイックスタート（Lv0）

```bash
docker compose -f stages/00-local-compose/docker-compose.yml up --build
# 別ターミナルで
curl localhost:3000/ready
curl -X POST localhost:3000/items -H 'content-type: application/json' -d '{"name":"hello"}'
curl localhost:3000/items
# 後片付け
docker compose -f stages/00-local-compose/docker-compose.yml down
```

## 学習ログ（ブログ用一次資料）

各段の実行コマンド・出力・気づきを [`docs/learning-log/`](docs/learning-log) に時系列で残す。
横断で効く教訓は [`LEARNINGS.md`](docs/learning-log/LEARNINGS.md) に段ごとに蓄積していく。

## 前提ツール

- Docker + Docker Compose v2（済）
- kubectl / helm（済）
- k3d … Lv2 以降で使用。未導入なら `brew install k3d`
- k6 は不要（Docker イメージで実行）
