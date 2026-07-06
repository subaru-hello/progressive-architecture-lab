# Lv2 — Kubernetes 単一ノード（k3s / k3d）

## 狙い
Lv1 の「レプリカ管理・復旧・スケールが手動／命令的」を、**宣言的**に置き換える。
同じアプリを k3s(k3d) に載せ、Deployment / Service / Ingress / probe で運用。
**自己修復**と、そして **Node×k8s の CPU 飽和で起きる事故**を体験する。

## 構成
```
host :8081 --> k3d loadbalancer --> Traefik(Ingress) --> Service(api) --> Pod x3 --> Service(postgres) --> postgres(PVC)
```
- k3s 標準の Ingress = **Traefik**、StorageClass = **local-path**（PVC で Postgres 永続化）
- `api` は Deployment(replicas:3)。`startupProbe`/`readinessProbe`/`livenessProbe` を宣言
- Pod 名を `INSTANCE_ID` にしているので、レスポンスでどの Pod が処理したか見える

## セットアップ（再現手順）
```bash
# 1. 単一ノードクラスタ作成（ホスト :8081 -> Traefik :80）
k3d cluster create pal --servers 1 --agents 0 --port "8081:80@loadbalancer"

# 2. アプリをビルドしてクラスタに取り込む（ローカルレジストリ不要）
docker build -t pal-api:lv2 app
k3d image import pal-api:lv2 -c pal

# 3. マニフェスト適用
kubectl apply -f stages/02-k3s-single/k8s/
kubectl -n pal rollout status deploy/api

# 後片付け（クラスタごと削除）
k3d cluster delete pal
```

## 動作確認
```bash
# ロードバランシング（Pod 名がバラける）
for i in $(seq 1 12); do curl -s localhost:8081/health | jq -r .instance; done | sort | uniq -c

# 自己修復: Pod を1つ消すと自動で作り直される
kubectl -n pal delete pod -l app=api --field-selector 'status.phase=Running' | head -1
kubectl -n pal get pods -l app=api -w

# 現実的な I/O 負荷（これは綺麗に通る）
docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 grafana/k6 run - < load/smoke.js
```

## 実測（8コアマシン）
| 負荷 | 結果 |
|---|---|
| I/O 律速 smoke (ms=20, 10VU) | **60.9 req/s, p95 37ms, エラー 0%** ✓ 正常 |
| CPU 律速 cpu=10, 20VU（**既定 probe**） | **98.8% エラー**（probe 失敗で Pod 退避→502連鎖） |
| CPU 律速 cpu=10, 20VU（**寛容 probe**） | エラー 4%・でも 6.4 req/s / p95 8.4s（単一スレッド飽和） |

## ⚠️ 学びの核心: Node×k8s の CPU 飽和事故
CPU 律速の負荷で **Node のイベントループが塞がる → probe(`/health`,`/ready`) が応答不能 →
k8s が Pod を不健全と判断して Service から外す・再起動 → バックエンド消滅で 502 連鎖**。
Lv1(Caddy) は probe が無いので詰まっても捌けていたのが、k8s では probe が能動的に Pod を抜くため雪崩れる。

**対処（このリポジトリで実施）**
1. `startupProbe` を分離 → 起動時(DB初期化待ち)の再起動ループを防ぐ
2. `readiness/liveness` の `timeoutSeconds`/`failureThreshold` を緩める → 一時的な詰まりで抜かない

**それでも根治しない理由と本当の解**
- 単一スレッドが本当に飽和すると、緩めても遅延は爆発する（6.4 req/s）。
- 本当の解は 2 つ: (a) **イベントループを塞がない**（CPU処理は `worker_threads` に逃がす）、
  (b) **飽和する前に容量を足す**（HPA で自動増設・ノードを増やす）＝ **Lv3**。

## この段の限界（次に進む理由）
- レプリカ数は手動固定。負荷に応じた**自動増減が無い**。
- 1 ノードなのでスケールは**そのノードのコア数が上限**。
- → **Lv3**: 複数ノード + **HPA(HorizontalPodAutoscaler)** で、CPU に応じて Pod を自動増減し、複数ノードのコアを使う。
