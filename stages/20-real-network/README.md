# Lv20 — 分散トランザクション × 実クラスタ測定台

## Thesis

Lv19 では Docker Compose の bridge ネットワーク上で `none / 2PC / saga` の3方式を比較した。
ここでは同じアプリコードを **k3d 2-agent クラスタ** に載せ替え、k8s の dataplane を経由させることで **クラスタ通信コスト**（CNI オーバーヘッド・kube-proxy ルーティング）込みの数字を取る。

**正直な注記**: 測定環境は同一 Mac 上の k3d なので、物理レイヤーは loopback に過ぎない。
計測できるのは「クラスタ dataplane 税」であって「WAN 税」ではない。
2PC / saga の遅さは HTTP 往復本数と DB の PREPARE/COMMIT サイクル数で効く。
WAN のような高レイテンシ環境ではその差が数十〜数百倍に拡大する。

podAntiAffinity (preferred) を coordinator に付け、`api` と `items-service` を別 node に配置することで
pod 間トラフィックがノード境界を越える構成にしている。

---

## 実測サマリ（この box・1VU 逐次 N=200・host load 11〜22 と荒い）

| 指標 | none | saga | 2PC |
|---|---|---|---|
| compose median (Lv19 参考) | 3.42ms | 5.04ms | 5.46ms |
| **k3d median（静穏）** | ~18-24ms | ~27ms | ~27ms |
| k3d テール p95（load 競合時） | ~90ms | ~90ms | **1.38s (max 3.73s)** |
| in-doubt（fault 注入） | リーク(ロック無) | 補償で回復(ロック無) | **prepared 宙吊り・行ロック** |

- **dataplane floor（~5x）は全モード一律**。往復本数差は median の中では縮む（2PC/none 比 compose 1.6x → k3d **1.16x**）。
- **2PC の税は median でなくテール/競合耐性に出る**（prepared txn を余分な cross-node 往復越しに保持 → load 下で p95 爆発）。
- in-doubt は基盤を k3d に替えても Lv19 と同一再現。→ Lv21 で自動回復させる。
- 詳細な考察は [`docs/learning-log/20-lv20.md`](../../docs/learning-log/20-lv20.md) / 教訓39。

---

## 起動

```bash
bash stages/20-real-network/setup.sh
```

初回は k3d クラスタ作成 + Docker ビルド + import が走る（3〜5分程度）。
2回目以降はクラスタ存在チェックでスキップされる（冪等）。

---

## mode 切替

```bash
# none → 2pc
kubectl -n pal-lv20 set env deploy/api ORDER_TX_MODE=2pc
kubectl -n pal-lv20 rollout restart deploy/api
kubectl -n pal-lv20 rollout status deploy/api

# 2pc → saga
kubectl -n pal-lv20 set env deploy/api ORDER_TX_MODE=saga
kubectl -n pal-lv20 rollout restart deploy/api
kubectl -n pal-lv20 rollout status deploy/api

# saga → none（リセット）
kubectl -n pal-lv20 set env deploy/api ORDER_TX_MODE=none
kubectl -n pal-lv20 rollout restart deploy/api
kubectl -n pal-lv20 rollout status deploy/api
```

---

## 障害注入

```bash
# after-prepare-all: 両 DB を PREPARE した後にクラッシュ → in-doubt 状態を作る（2PC 用）
kubectl -n pal-lv20 set env deploy/api FAULT_POINT=after-prepare-all

# after-first-write: 最初の DB 書き込み後にクラッシュ → saga 補償の確認
kubectl -n pal-lv20 set env deploy/api FAULT_POINT=after-first-write

# 注入を解除する（キー末尾の `-` で env var を削除）
kubectl -n pal-lv20 set env deploy/api FAULT_POINT-
```

---

## 配置確認

```bash
kubectl -n pal-lv20 get pods -o wide
```

`api` と `items-service` が異なる `NODE` 列に表示されていれば、pod 間トラフィックがノード境界を越えている。
（preferred anti-affinity のため保証ではない。agent 数 < replica 数の場合は同居する場合がある）

---

## in-doubt トランザクション確認

2PC で障害注入後、items-db に準備済みトランザクションが残っているか確認する：

```bash
kubectl -n pal-lv20 exec deploy/items-db -- \
  psql -U pal -d pal -c "SELECT gid, prepared FROM pg_prepared_xacts;"
```

### prepared の掃除

```bash
# まず一覧を取得し ROLLBACK 文を生成して実行
kubectl -n pal-lv20 exec deploy/items-db -- \
  psql -U pal -d pal -c \
  "SELECT 'ROLLBACK PREPARED ' || quote_literal(gid) || ';' FROM pg_prepared_xacts;" \
  -t | kubectl -n pal-lv20 exec -i deploy/items-db -- psql -U pal -d pal
```

または手動で `ROLLBACK PREPARED '<gid>';` を流す。

---

## k6 負荷テスト

```bash
# steady: 10 rps × 15 秒
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8081 \
  -e SCENARIO=steady \
  -e RATE=10 \
  -e DURATION=15s \
  grafana/k6 run - < load/order-tx.js

# faults: N=60 リクエストで障害注入シナリオ
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8081 \
  -e SCENARIO=faults \
  -e N=60 \
  grafana/k6 run - < load/order-tx.js
```

---

## stretch（任意・実装はしない）

`tc netem` で実遅延を注入すると、2PC / saga の往復コスト差を劇的に可視化できる。

方法は2つある：

**A. initContainer で自動注入**（NET_ADMIN capability が必要）

```yaml
initContainers:
  - name: netem
    image: alpine
    securityContext:
      capabilities:
        add: ["NET_ADMIN"]
    command: ["sh", "-c", "tc qdisc add dev eth0 root netem delay 5ms"]
```

**B. 手動注入**（実験中に一時的に）

```bash
kubectl -n pal-lv20 exec -it deploy/api -- \
  sh -c "tc qdisc add dev eth0 root netem delay 5ms"
```

k3d ノードの KVM/veth を直接操作する方法もある（`docker exec k3d-pal-agent-0`）。
5ms × 往復 = 2PC は 4〜6 RTT → 20〜30ms のオーバーヘッド vs none の 0ms、という差が出る。

---

## 撤収

```bash
k3d cluster delete pal
```

クラスタごと消えるので PVC / emptyDir の掃除は不要。
