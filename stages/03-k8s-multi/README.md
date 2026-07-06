# Lv3 — Kubernetes 複数ノード + HPA（自動スケール）

## 狙い
Lv2 の「レプリカ手動固定・1 ノード上限・飽和で probe 事故」を解決する最終段。
複数ノードのクラスタに **HPA(HorizontalPodAutoscaler)** を載せ、CPU 利用率に応じて Pod を
**自動で増減**。DAU 100+ で負荷が急増しても「飽和する前に増設」できるかを検証する。

## 構成
```
host :8081 --> Traefik --> Service(api) --> Pod(2〜10, HPA が増減) --> Service(postgres) --> postgres
                                              ├ node: server-0
                                              ├ node: agent-0    ← topologySpread で 3 ノードに分散
                                              └ node: agent-1
HPA <── metrics-server（CPU 使用量）
```
- クラスタ: `k3d cluster create pal --servers 1 --agents 2`（3 ノード）
- HPA: `averageUtilization 50%`、`min 2 / max 10`、上げは即・下げは 60s 安定化
- `topologySpreadConstraints` で Pod をノード間に均等配置

> ⚠️ **正直な注記**: k3d の「複数ノード」は 1 台の Mac 上の 3 コンテナ＝**同じ 8 コアを共有**する。
> トポロジ・スケジューリング・HPA の挙動を学ぶには十分だが、**実ハードのコアは増えていない**。
> 自宅の実サーバー群（各ノード＝別マシン）なら、ノード追加がそのまま実コア増になる。

## セットアップ
```bash
k3d cluster create pal --servers 1 --agents 2 --port "8081:80@loadbalancer"
docker build -t pal-api:lv3 app
k3d image import pal-api:lv3 -c pal
kubectl apply -f stages/03-k8s-multi/k8s/
kubectl -n pal get hpa api -w            # スケールを監視
# 後片付け:  k3d cluster delete pal
```

## 負荷をかけて自動スケールを観測
```bash
# 持続的な CPU 負荷（別ターミナル）
docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 -e CPU=2 grafana/k6 run --vus 12 --duration 150s - < load/cpu.js
# 監視
watch -n5 'kubectl -n pal get hpa api; kubectl -n pal get pods -l app=api -o wide'
```

## 実測（8 コア機・3 ノード k3d）
### HPA のスケールアップ推移（負荷開始後）
| 経過 | CPU 利用率 | REPLICAS |
|---|---:|---:|
| t+15s | 11% | 2 |
| t+30s | 126% | 2→(5 起動中) |
| t+45s | 485% | 6→10 |
| t+60s | 328% | **10（上限到達）** |
| t+120s | 167% | 10 |
| 負荷終了+45s | 25% | 10→**5**（縮小開始） |

- **負荷開始から約 45 秒で 2 → 10 Pod に自動スケール。** 終了後は 60s 安定化を経て縮小。
- 10 Pod は **3 / 4 / 3 で 3 ノードに分散**（topologySpread）。

### 事故は起きたか？
| | Lv2（固定 3 Pod） | Lv3（HPA 2→10） |
|---|---|---|
| エラー率 | **98.8%**（probe 連鎖） | **0.00%** ✓ |
- **同種の CPU 負荷でも Lv3 はエラー 0%。** HPA が飽和前に Pod を足したので、Lv2 の probe 連鎖が起きなかった。これが最終段の成果。
- ただしレイテンシは高い（avg 513ms / p95 1.71s）。**8 コアに 10 Pod ＋ k6 を詰めたオーバーサブスクリプション**のため。実マシンを増やせば解消する方向。

## 残った限界（このラボの外にある本物の壁）
1. **Pod は増やせても、コア／ノードには上限がある。** max 到達後も利用率 166% のまま = 真の限界はノード数。
   実運用では **Cluster Autoscaler でノード自体を増やす**（自宅なら物理サーバー追加）。
2. **単一 DB は水平スケールの外に残る。** api は 10 Pod に増えても Postgres は 1。
   次の課題は **リードレプリカ / コネクションプーラ(PgBouncer) / 分散 DB**。
3. **CPU 律速を根本解決するなら worker_threads**（Node のイベントループを空ける）。

## まとめ
Lv0（1 プロセス）から Lv3（複数ノード自動スケール）まで、**同じアプリのまま基盤だけを登った**。
スケールの本質・LB・probe 事故・計測リテラシー・宣言的運用・自動スケールを一通り体験できた。
