# Lv1 — リバースプロキシ + 複数レプリカ

## 狙い
Lv0 の「ポート衝突でレプリカを増やせない」を解決する。前段に **Caddy（リバースプロキシ / LB）** を置き、
1 つのホストポートで受けて背後の `api` 複数レプリカへラウンドロビンする。**水平スケールの初歩**。

## 構成
```
[ localhost:8080 ] --> caddy --round robin--> api x N --> db (postgres)
                                              ├ api-1
                                              ├ api-2
                                              └ api-3 ...
```
- `caddy`: `Caddyfile` の `dynamic a` で Docker DNS(127.0.0.11) を 2 秒ごとに引き直し、`api` の全レプリカIPを取得。**レプリカを増やすと自動で振り分け先が増える。**
- `api`: ホストにポートを公開せず（`expose` のみ）、Caddy だけが到達。`deploy.replicas` で台数指定。
- レスポンスの `instance`（コンテナID）で、どのレプリカが処理したか分かる。

## 起動 / スケール / 停止
```bash
docker compose -f stages/01-compose-proxy/docker-compose.yml up --build -d          # 3 レプリカで起動
docker compose -f stages/01-compose-proxy/docker-compose.yml up -d --scale api=6    # 6 レプリカに増やす
docker compose -f stages/01-compose-proxy/docker-compose.yml down                   # 停止
```

## 動作確認
```bash
# ロードバランシングの目視（instance がバラける）
for i in $(seq 1 12); do curl -s localhost:8080/health | jq -r .instance; done | sort | uniq -c

# 負荷試験（Lv0 と同じ cpu=10 を Caddy 経由で）
docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 -e CPU=10 grafana/k6 run - < load/cpu.js
```

## 実測（8コアマシン, cpu=10, 20VU）
| 構成 | スループット | p95 |
|---|---:|---:|
| Lv0 Node x1 | 98 req/s | 372ms |
| Lv1 Node x3 | 276 req/s | 187ms |
| Lv1 Node x6 | 390 req/s | 214ms（max 1.54s） |

- x3 で約2.8倍・p95半減。**「言語を変えず、レプリカを横に並べる」だけで多コアを使い切れる**（番外編の Go@8=303 req/s に x3 で並ぶ）。
- x6 はスループットは伸びるがテール悪化。**8コアに対しレプリカを積みすぎるとオーバーサブスクリプション**。

## この段の限界（次に進む理由）
- レプリカ数・再起動・ヘルスチェックが**手動 / 命令的**。落ちたレプリカの自動復旧や、負荷に応じた自動増減がない。
- スケールは**1台のマシンのコア数が上限**。これ以上は**マシンを増やす**しかない。
- → **Lv2**: 同じアプリを Kubernetes(k3s/k3d) に載せ、Deployment/Service/Ingress で**宣言的**に運用。**Lv3** で複数ノード + HPA による自動スケールへ。
