# Lv12 — scale-to-zero(serverless風) ↔ always-on k8s

## 狙い

「serverless→managed k8s(EKS)」移行を、**AWS 不使用・実費ゼロ**のまま k3d 上で本質だけ再現する。
serverless の肝は **scale-to-zero**（リクエストが無ければ Pod を 0 に落とし、来たら起こす）。これを Lv3 の
always-on k8s（HPA min=2 で常に温かい下限を保つ）と背中合わせで比べ、**何を得て何を払うか**を測る。

```
Lv3      : HPA min=2   → idle でも 2 Pod 常駐（常に warm、idle コストを払い続ける）
Lv12     : KEDA min=0  → idle で 0 Pod（idle コスト 0、初回リクエストが cold-start を丸ごと食う）
```

期待する結論：**scale-to-zero は「idle コストを cold-start latency に変換する取引」であって
throughput を増やす手段ではない。** 壁は消えず「idle 常駐コスト」→「初回 latency」へ移動する
（Lv5「壁は移動する」/ Lv8「latency 切り離し ≠ throughput 増」の系譜）。

## 採用: KEDA + http-add-on（Knative / OpenFaaS を却下）

k3d 上で「**既存の普通の HTTP Deployment を** min=0 まで scale-to-zero し、リクエストで 0→N 起こす」方式。
- **app 全段不変・Lv3 マニフェスト最小差分**: Lv3 の Deployment/Service をそのまま流用し、HPA を外して
  `HTTPScaledObject` を足すだけ。A/B の変数が「下限台数 0 vs 2」に純化する。
- **Mac Docker VM で最軽量**（operator + interceptor + external-scaler）。Knative の activator+autoscaler+
  Kourier は VM で重く計測を汚染する。
- **cold-start が素直に測れる**: interceptor が 0→1 の間リクエストをホールドし replica≥1 を待って転送 →
  初回が **503 でなく「遅い 200」** として latency に出る（Lv2 の probe 事故＝502 高速返しの轍を踏まない）。

> ⚠️ **正直な注記**: KEDA http-add-on は beta/experimental 扱い。production の scale-to-zero HTTP は
> **Knative が定石**。本ラボは「最小差分でフェアな A/B」目的で KEDA を選ぶ。

## 構成

```
host :8081 → Traefik(k3d LB) → KEDA interceptor-proxy → (0→N 起こす) → api Pod → Postgres(常時1)
                                    ↑ Host: pal.localhost で対象を判定
```
- **DB は scale-to-zero しない**（常時 1）。0 に落とすと api の cold-start に DB 起動まで乗って一変数の実験にならない。
- k6 は全リクエストに `Host: pal.localhost` を付ける（interceptor のルート条件）。

## セットアップ

```bash
# 1) クラスタ（Lv3 と同一）
k3d cluster create pal --servers 1 --agents 2 --port "8081:80@loadbalancer"
docker tag pal-api:lv3 pal-api:lv12   # 実体は Lv3 と同一 image。app は不変
k3d image import pal-api:lv12 -c pal

# 2) KEDA + http-add-on（helm）
helm repo add kedacore https://kedacore.github.io/charts && helm repo update kedacore
helm install keda kedacore/keda -n keda --create-namespace --wait
helm install http-add-on kedacore/keda-add-ons-http -n keda --wait

# 3) マニフェスト適用（scale-to-zero アーム）
kubectl apply -f stages/12-scale-to-zero/k8s/00-namespace.yaml \
              -f stages/12-scale-to-zero/k8s/10-postgres.yaml \
              -f stages/12-scale-to-zero/k8s/20-api.yaml \
              -f stages/12-scale-to-zero/k8s/40-scaledobject.yaml \
              -f stages/12-scale-to-zero/k8s/30-ingress.yaml

# 4) 動作確認: 初回は 0→1 起こすので遅い 200、2発目は warm
curl -s -o /dev/null -w "cold http=%{http_code} %{time_total}s\n" -H 'Host: pal.localhost' http://localhost:8081/health
curl -s -o /dev/null -w "warm http=%{http_code} %{time_total}s\n" -H 'Host: pal.localhost' http://localhost:8081/health
kubectl -n pal get pods -l app=api    # idle 放置 ~45s で 0 に戻る

# 後片付け
k3d cluster delete pal
```

interceptor Service 名/`HTTPScaledObject` スキーマは chart バージョンで変わる →
`kubectl -n keda get svc | grep interceptor` / `kubectl explain httpscaledobject.spec` で確認。

## A/B 比較手順

同一クラスタ・同一 image で「制御面だけ」を差し替える（変数を下限台数に純化）。

```bash
# --- Arm B: scale-to-zero（上記セットアップ済の状態）---
docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 -e SCENARIO=coldstart \
  grafana/k6 run - < load/scale-to-zero.js         # cold-start p95/p99
docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 -e SCENARIO=burst -e PEAK_VUS=200 \
  grafana/k6 run - < load/scale-to-zero.js         # burst エラー率 + warm throughput

# --- Arm A: always-on（HTTPScaledObject を外し replicas=2 固定 + 直 Ingress）---
kubectl -n keda delete ingress api-scaletozero
kubectl -n pal delete httpscaledobject api
# api Service へ直に向く Ingress(pal ns, Lv3 相当) を apply してから:
kubectl -n pal scale deploy api --replicas=2
#   同じ 2 本の k6 を投げて背中合わせ
```

観測（背中合わせで比べる）：
- cold-start: `cold_start_ms` p95/p99（idle→0 後の初回）
- idle 常駐: `kubectl -n pal get pods -l app=api` の running 数 + `kubectl top pods`
  （**scale-to-zero 側は KEDA 制御面の常駐メモリも正直に足す**）
- burst: `http_req_failed` / 立ち上げ中の `kubectl get pods -w`
- warm: `warm_latency` p95 と req/s（always-on と一致するはず）

## 実測結果（k3d 3ノード / 8コア Mac / load ~4-5 の静かな窓 / `/work?ms=20`）

| 指標 | always-on (min=2) | scale-to-zero (min=0) |
|---|---|---|
| idle app Pod | **2** | **0** |
| idle app メモリ | 126Mi (64+62) | 0（app） |
| cold-start avg / p95（idle後初回, n=8） | 41ms / 47ms | **4,730ms / 5,350ms** |
| burst 0→N エラー率 | 0%（0/18999） | **0%（0/18998）** ← interceptor が hold |
| warm throughput | 82.6 req/s | 82.6 req/s |
| warm p95 | 25.5ms | 27.6ms |
| 常駐 制御面（共有固定費） | metrics-server 等のみ | **KEDA interceptor×3+scaler×3+operator+apiserver ≈ 160Mi+** |

### 気づき
1. **cold-start 税 ≈ 115×**（4,730ms vs 41ms）。これが scale-to-zero の代償。ただし **warm 定常は完全一致**
   （82.6 req/s・p95 ~26ms・0% error が両アーム）＝ **scale-to-zero は定常性能を犠牲にしない。払うのは
   「idle 後の初回一発」だけ**。
2. **cold-start はエラーでなく latency に出る**。interceptor が 0→1 の間ホールドするので burst でも 503 ゼロ
   （18,998 req 全部 200）。Lv2 の「probe 事故で 502 高速返し → throughput が嘘をつく」とは逆に、遅さが
   正直に p95 へ現れる。
3. **scale-to-zero はタダではない（正直な控除）**。app Pod は 0 でも KEDA 制御面（interceptor/scaler/operator）
   が常駐し ~160Mi 食う。**1 workload なら always-on の 2 Pod(126Mi)より高くつく**。idle 節約が黒字化するのは
   「制御面固定費 ÷ workload あたり節約」の**損益分岐点を超える workload 数**から（教訓6「数字は控除まで見ろ」の再演）。
4. **Docker-Mac VM 歪み**: cold-start 4.7s には VM の scheduler ジッタ + `app` の起動処理（`CREATE TABLE`
   リトライ + DB 接続）が丸ごと乗る。実機/EKS ではもっと小さく出るはず。**相対差で読む**。

## 限界と次へ
- scale-to-zero は「散発アクセス・多数の休眠 workload」に効く（idle 0 + 制御面を共有）。**常時トラフィックが
  ある単一 workload には always-on(min≥1) が正解** ＝ どのレバーも効くのは壁がそこにある時だけ（Lv10 の再演）。
- 次軸（別プラン）: **monolith ↔ modular monolith ↔ microservices の分解**。現状 app は単一 items ドメインで
  薄いので、items+orders+users の多ドメイン app を先に作ってから分解 A/B を測る。
