# experiment: Go vs Node — CPU 律速はどこまで捌けるか

Lv0 で見た「Node 単一プロセスが CPU 律速で頭打ち」を、同一計算の Go サーバーと比較する番外編。
**結果と考察は [`docs/learning-log/note-go-vs-node.md`](../../docs/learning-log/note-go-vs-node.md)** に記録。

## 構成
- `main.go` … Node の `app/src/server.ts` の `/work` と**同一計算**（`cpu*1e6` 回の sqrt 加算）を行う最小 HTTP サーバー
- net/http は各リクエストを goroutine で処理 → `GOMAXPROCS` 個の OS スレッドに分散（CPU 処理が並列化）

## 再現手順
```bash
# 1. Go サーバーをビルド
docker build -t pal-go-work experiments/go-vs-node

# 2. 全コア(8)で起動して負荷
docker run -d --name pal-go -p 3001:3001 -e GOMAXPROCS=8 pal-go-work
docker run --rm -i -e BASE_URL=http://host.docker.internal:3001 -e CPU=10 grafana/k6 run - < load/cpu.js

# 3. 1コアに絞って再計測（並列度の効果を切り分け）
docker rm -f pal-go
docker run -d --name pal-go -p 3001:3001 -e GOMAXPROCS=1 pal-go-work
docker run --rm -i -e BASE_URL=http://host.docker.internal:3001 -e CPU=10 grafana/k6 run - < load/cpu.js

# 4. Node 側（Lv0）と比較
docker compose -f stages/00-local-compose/docker-compose.yml up -d
docker run --rm -i -e BASE_URL=http://host.docker.internal:3000 -e CPU=10 grafana/k6 run - < load/cpu.js

# 後片付け
docker rm -f pal-go
docker compose -f stages/00-local-compose/docker-compose.yml down
```

## 要点（詳細は学習ログへ）
- 1 コア同士: **Node の方が約1.6倍速い**（V8 JIT が tight loop に強い）
- Go の勝因は言語速度ではなく **8コア並列**（Go@1→Go@8 で約5倍）
- Node の対処は同じく **水平スケール（レプリカ増）** → ラボ本線 Lv1→Lv3
