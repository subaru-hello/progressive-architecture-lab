# 番外編 — Go だったら CPU 律速を捌けたのか？（Node vs Go 実測）

記録日: 2026-07-06 / マシン: 8 コア（物理8・論理8）/ Docker CPU=8

## 動機
Lv0 の体感実験で、1 コンテナの Node が CPU 律速（`/work?cpu=10`）で ~100 req/s・p95 373ms に頭打ちした。
「Node は JS が単一スレッドだから」と説明したが、**Go なら捌けたのか？** を同条件で実測して確かめる。

## 実験計画（言語速度と並列度を切り分ける）
Node 版 `/work` と**完全に同じ計算**（`cpu*1e6` 回の `sqrt` 加算）の Go サーバーを用意し、同じ Docker・同じ k6 で比較：
- **Node（実質 1 JS スレッド）** vs **Go `GOMAXPROCS=1`（1コア）** → 純粋な言語の単発速度
- **Go `GOMAXPROCS=1`** vs **Go `GOMAXPROCS=8`** → 並列化の効果
- 負荷は全ケース `load/cpu.js`（20 VUs × 20s, `/work?cpu=10`）

## コマンドと出力
```console
# Go サーバーをビルド
$ docker build -t pal-go-work experiments/go-vs-node

# 単発レイテンシ（cpu=10）
$ for i in 1 2 3; do curl -s -o /dev/null -w '%{time_total}s\n' 'localhost:3001/work?cpu=10'; done
0.064653s   # cold
0.037207s
0.024526s   # warm ~24ms（Node は ~22ms）

# Go GOMAXPROCS=8
$ docker run -d --name pal-go -p 3001:3001 -e GOMAXPROCS=8 pal-go-work
$ docker run --rm -i -e BASE_URL=http://host.docker.internal:3001 -e CPU=10 grafana/k6 run - < load/cpu.js
    http_req_duration..: avg=65.09ms  p(95)=106.4ms  max=279.94ms
    http_reqs..........: 6107   303.650743/s

# Go GOMAXPROCS=1
$ docker run -d --name pal-go -p 3001:3001 -e GOMAXPROCS=1 pal-go-work
$ docker run --rm -i -e BASE_URL=http://host.docker.internal:3001 -e CPU=10 grafana/k6 run - < load/cpu.js
    http_req_duration..: avg=327.45ms p(95)=492.07ms max=702.49ms
    http_reqs..........: 1224   60.504451/s

# Node（1 JS スレッド）を同条件で再計測
$ docker compose -f stages/00-local-compose/docker-compose.yml up -d
$ docker run --rm -i -e BASE_URL=http://host.docker.internal:3000 -e CPU=10 grafana/k6 run - < load/cpu.js
    http_req_duration..: avg=201.1ms  p(95)=372.49ms max=588.35ms
    http_reqs..........: 1990   98.440049/s
```

## 結果

| ケース | スループット | p95 | 単発 |
|---|---:|---:|---:|
| **Node**（1 JS スレッド） | 98 req/s | 372ms | ~22ms |
| **Go `GOMAXPROCS=1`** | 60 req/s | 492ms | ~24ms |
| **Go `GOMAXPROCS=8`** | **303 req/s** | **106ms** | ~24ms |

- Go@8 / Go@1 = **約5.0倍**（8コアだが完全な8倍でないのは、同じ8コア上で k6 自身も CPU を食う＋ネットワーク越しの固定オーバーヘッドのため）
- Go@8 / Node = **約3.1倍**（スループット）、p95 は **約3.5倍改善**
- Node / Go@1 = **約1.6倍**（＝**1コアでは Node の方が速かった**）

## 気づき（ここが本題）
1. **「Go だから速い」は今回のマイクロベンチでは否。** 1コア同士では V8 の JIT が tight な数値ループで強く、**Node が Go@1 より 1.6 倍速かった**。単発レイテンシもほぼ同じ（22ms vs 24ms）。
2. **効いたのは並列度だけ。** Go@8 の勝ちは 100% 「8コアを1プロセスで使えた」から。Go@1→Go@8 で 5 倍伸びたのがその証拠。
3. **つまり Node の頭打ちは「言語が遅い」ではなく「1プロセス＝CPUは1コア」。** 直し方は 2 つ：
   - 言語を変える（Go/Rust など、1プロセスで多コア）← 今回の Go@8
   - **プロセスを増やす（水平スケール）** ← Node 8 レプリカ ≈ 8 コア。**これがこのラボの本線（Lv1→Lv3）**
4. **注意: これは純 CPU のマイクロベンチ。** 実際の API は I/O 待ち（`ms=`）や DB クエリが主で、そこは Node の非同期モデルで問題なく、**先に Postgres の接続数/スループットが壁**になる。言語差はほとんど出ない。

## 結論
> Go なら「その CPU テストは」約3倍捌けた。ただし速さの源は言語ではなく**多コア並列**。
> Node は同じことを**レプリカを横に並べて**達成できる。→ ラボの Node/TS 選択は妥当のまま、次段（水平スケール）の動機がむしろ数値で裏付けられた。

## 再現方法
`experiments/go-vs-node/README.md` 参照。
