# 学習ログ

各ステージで実際に叩いたコマンド（標準入力）と出力、そこから得た気づきを時系列で残す。
後日ブログ化する際の一次資料。フォーマットは以下：

- **やったこと** … その段で何を構築したか
- **コマンドと出力** … コピペした実際の入出力（省略せず、要点は残す）
- **気づき / ハマり** … 観察した挙動・つまずき・数値
- **次へ進む理由** … この段の限界と、次段でどう解決するか

## ⭐ [LEARNINGS.md — 学びの蓄積（横断まとめ）](LEARNINGS.md)
各段の詳細から転用できる教訓だけを 1 本に蓄積。**段を進めるたびに追記する。**

## 目次（段ごとの詳細ログ）
- [Lv0 — docker-compose](00-lv0.md)
- [番外編 — Go vs Node（CPU 律速はどこまで捌けるか）](note-go-vs-node.md)
- [Lv1 — compose + proxy（複数レプリカ + LB）](01-lv1.md)
- [Lv2 — k3s 単一ノード（自己修復・probe 事故と対処）](02-lv2.md)
- [Lv3 — k8s 複数ノード + HPA（自動スケール・エラー0%）](03-lv3.md)
- [Lv4 — 単一DBの壁：コネクション枯渇 → PgBouncer](04-lv4.md)
- [Lv5 — リードレプリカ + read/write 分離：壁は消えず「移動」する](05-lv5.md)
- [Lv6 — Redis キャッシュ：read 負荷は「移す」でなく「消す」](06-lv6.md)
- [Lv7 — worker_threads：CPU 律速の「回避」でなく「根治」（probe 雪崩を単一 replica で根治）](07-lv7.md)
- [Lv8 — write-behind 非同期キュー：バッファであって乗数ではない（write の壁は消えない）](08-lv8.md)
- [Lv9 — INSERT バッチ化：律速は行数でなく commit 回数（consumer 1 のまま drain 6.4×）](09-lv9.md)
- [Lv10 — synchronous_commit=off：commit の cost を削る（回数削減には敵わない・durability を払う）](10-lv10.md)
- [Lv11 — 並列 committer + group commit：durability を保ったまま fsync を束ねる（実装のみ・A/B 実測は延期）](11-lv11.md)
- [Lv12 — scale-to-zero ↔ always-on：idle コストを cold-start latency に変換する](12-lv12.md)
- [Lv13 — monolith（多ドメイン app のベースライン）：cross-context を直 SQL JOIN で in-process 解決](13-lv13.md)
- [Lv14 — modular monolith：env 1 個差で schema 結合を runtime 無料で切る（latency は不変）](14-lv14.md)
- [Lv15 — microservices：分解の対価を回収（network 税は小・本体は idle 3.3×/原子性喪失/blast-radius 隔離）](15-lv15.md)
- [Lv16 — 泥団子と「動かせなさ」：引っ越しやすさ＝結合度（naive 移行は 100% ダウン・跨ぎ FK が抽出を拒否）](16-lv16.md)
- [Lv17 — 解きほぐし（mud → hexagonal）：結合を seam 1 本に落とす（層化は runtime 無料・境界＝事前の切断線）](17-lv17.md)
- [Lv18 — ダウンタイムゼロ抽出（strangler-fig）：hex を live で移す（規律=0% / naive=66.7% / 泥団子=100% ダウン）](18-lv18.md)
- [Lv19 — 分散トランザクション（none/2PC/saga）：タダの分散tx は無い（2PC=原子性↔ブロッキング / saga=可用性↔中間状態）](19-lv19.md)
- [Lv20 — 実ネットワークの税（k3d 化）：往復差は「平均」でなく「裾」に出る（dataplane floor は全モード一律・2PC はテールで壊れる）](20-lv20.md)
- [Lv21 — 2PC 自動 crash-recovery：in-doubt は「決定を永続化」して「起動時に再解決」すれば消える（commit point + リゾルバ + presumed-abort）](21-lv21.md)
