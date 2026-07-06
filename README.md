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
```

各段は **同じ `app/`（Node/TS + Postgres API）** を使い回す。基盤だけが変わる。

## 共有アプリ `app/`

Fastify + node-postgres + prom-client の最小 API。スケール練習に必要な仕掛けを内蔵：

| エンドポイント | 用途 |
|---|---|
| `GET /health` | Liveness（依存なし・k8s livenessProbe 用） |
| `GET /ready` | Readiness（DB 疎通確認・k8s readinessProbe 用） |
| `GET /metrics` | Prometheus メトリクス（各レスポンスに `instance` ラベル） |
| `GET /work?ms=&cpu=` | 負荷シミュレータ（`ms`=待機, `cpu`=CPU バーン）→ スケール実験の駆動源 |
| `GET/POST /items` | Postgres への CRUD |
| すべてのレスポンス | `instance` フィールドに処理したコンテナ/Pod 名 → **ロードバランシングが目視できる** |

## 各ステージ

| Lv | ディレクトリ | 使う道具 | 学ぶこと | 状態 |
|---|---|---|---|---|
| 0 | [`stages/00-local-compose`](stages/00-local-compose) | docker compose | コンテナ化・DB 連携・ヘルスチェック | ✅ 動作確認済 |
| 1 | [`stages/01-compose-proxy`](stages/01-compose-proxy) | compose + Caddy | 複数レプリカ・LB・水平スケールの限界 | ✅ 動作確認済 |
| 2 | [`stages/02-k3s-single`](stages/02-k3s-single) | k3d + kubectl | Deployment/Service/Ingress・自己修復・probe 事故 | ✅ 動作確認済 |
| 3 | `stages/03-k8s-multi` | k3d 複数ノード + HPA | 自動スケール・HA・DAU100+ 想定の負荷試験 | ⏳ 未 |

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
