#!/usr/bin/env bash
# Lv20 setup: k3d クラスタ作成 → イメージビルド&import → manifest apply → rollout 待機
# 冪等: クラスタが既に存在する場合はスキップする
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"

# --- 1. k3d クラスタ（agents 2 で 2 ノード構成。preferred anti-affinity を活かす）---
if k3d cluster get pal >/dev/null 2>&1; then
  echo "[skip] k3d cluster 'pal' already exists"
else
  echo "[create] k3d cluster pal --agents 2 --port 8081:80@loadbalancer"
  k3d cluster create pal --agents 2 --port "8081:80@loadbalancer"
fi

# k3d cluster create は context を自動切替するが、skip 分岐だと切替らない。
# kubeconfig が別クラスタ(EKS 等)を指したまま kubectl apply すると他クラスタへ流し込む事故になるため、
# 毎回 k3d-pal に明示的に切り替える。
echo "[context] use-context k3d-pal"
kubectl config use-context k3d-pal

# --- 2. Docker イメージビルド & k3d へ import ---
echo "[build] pal-api:lv20"
docker build -t pal-api:lv20 "$REPO/app"

echo "[import] pal-api:lv20 -> k3d cluster pal"
k3d image import pal-api:lv20 -c pal

# --- 3. manifest apply ---
echo "[apply] $SCRIPT_DIR/k8s/"
kubectl apply -f "$SCRIPT_DIR/k8s/"

# --- 4. rollout 待機 ---
echo "[wait] namespace pal-lv20 pods to be scheduled..."
kubectl -n pal-lv20 wait --for=condition=Available deployment/db       --timeout=120s
kubectl -n pal-lv20 wait --for=condition=Available deployment/items-db --timeout=120s

echo "[wait] items-service rollout..."
kubectl -n pal-lv20 rollout status deploy/items-service --timeout=180s

echo "[wait] api (coordinator) rollout..."
kubectl -n pal-lv20 rollout status deploy/api --timeout=180s

echo ""
echo "=== Lv20 ready ==="
echo ""

# --- 確認コマンド（コメント参照）---
# Pod 配置確認（api と items-service が別 NODE か）:
#   kubectl -n pal-lv20 get pods -o wide
#
# ヘルスチェック:
#   curl http://localhost:8081/health
#
# mode 切替 (none|2pc|saga):
#   kubectl -n pal-lv20 set env deploy/api ORDER_TX_MODE=2pc
#   kubectl -n pal-lv20 rollout restart deploy/api
#   kubectl -n pal-lv20 rollout status deploy/api
#
# 障害注入:
#   kubectl -n pal-lv20 set env deploy/api FAULT_POINT=after-prepare-all
#   # 戻す: kubectl -n pal-lv20 set env deploy/api FAULT_POINT-
#
# in-doubt 確認:
#   kubectl -n pal-lv20 exec deploy/items-db -- psql -U pal -d pal -c "SELECT gid,prepared FROM pg_prepared_xacts;"
#
# k6 負荷:
#   docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 -e SCENARIO=steady -e RATE=10 -e DURATION=15s grafana/k6 run - < "$REPO/load/order-tx.js"
#
# 撤収:
#   k3d cluster delete pal
