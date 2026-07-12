# 学びの蓄積（横断まとめ）

各段の詳細ログ（`NN-*.md`）から、**転用できる教訓だけ**を抜き出して蓄積するファイル。
段を進めるたびにここへ追記する。各項目に出所の段を `[Lv?]` で付す。

最終更新: 2026-07-11（Lv10 — synchronous_commit=off まで）

---

## 1. スケールの本質は「1プロセス = 1コア」 `[Lv0][Lv1]`
CPU 律速では Node は単一スレッドで頭打ち（Lv0: 98 req/s, p95 372ms）。
**水平スケール = 言語を変えずに多コアを使う手段**。レプリカ×3 で 276 req/s（≒3倍）と素直に伸びた。

## 2. 言語の速さ ≠ スループット `[番外編 Go vs Node]`
| | 1コア | 多コア |
|---|---:|---:|
| Node | **98 req/s**（V8 JIT が強い） | ×3レプリカ = 276 |
| Go | 60 req/s（1コアでは Node より遅い） | 1プロセス@8core = 303 |

Go の勝因は言語ではなく**並列度だけ**。1コア勝負ではむしろ Node が 1.6 倍速い。
「Node が遅い」のではなく「1プロセスが 1 コアなだけ」で、Node はレプリカで同じ多コアを取り返せる。

## 3. ロードバランサは「増やす」の前提条件 `[Lv0→Lv1]`
Lv0 で `--scale api=3` → **`port is already allocated`**。
1 ポートで受けて背後へ振り分ける proxy が無いとレプリカを増やせない。
全レスポンスに `instance` を入れておくと、**4/4/4 の分散が目視**できて理解が早い。

## 4. 「レプリカは多いほど良い」ではない（オーバーサブスクリプション） `[Lv1]`
8 コアに対しレプリカ×6 → スループットは 390 まで伸びるが **p95 悪化・max 1.54s**。
コア数を超えて積むとテールが荒れる。台数はコア数を意識して決める。

## 5. ⭐ probe は諸刃の剣：Node×k8s の飽和事故 `[Lv2]`
CPU 飽和で **イベントループ停止 → `/health`・`/ready` が応答不能 → k8s が健全 Pod を
Service から外す＆再起動 → 502 連鎖（98.8% エラー）**。
- Lv1(Caddy) は probe が無いので詰まっても捌けた。**k8s は能動的に Pod を抜くぶん飽和時に雪崩れる**。
- 対処: `startupProbe` 分離 ＋ `timeout`/`failureThreshold` 寛容化。
- ただし**単一スレッド飽和そのものは治らない**（真の解 = worker_threads でループを空ける / 飽和前に増設 = HPA）。

## 6. ⭐ 計測リテラシー：throughput 単体は嘘をつく `[Lv2]`
Lv2 で最初「538 req/s！」→ 実は **98.8% が 502 の高速返し**。median 1.4ms が異常のサインだった。
**必ず `http_req_failed` / エラー率 / `checks_succeeded` を見る**。数字は疑ってかかる。

## 7. 結論は「負荷の種類」で反転する `[Lv2]`
CPU 律速では事故る一方、**現実的な I/O 律速（ms=20）では Lv2 も 0% エラー・p95 37ms で快調**。
実 API は I/O 主体なので Node で十分、**先に Postgres が壁**になる。
CPU 飽和は人工的シナリオだが、対処は知っておくべき。

## 8. k8s の核心価値は「宣言的 desired state」 `[Lv2]`
Pod を消しても「3 レプリカ」を自動で維持（8 秒で再作成）。
Lv1 の命令的なレプリカ管理との決定的な差。

## 9. ⭐ HPA は「飽和前に増設」で probe 事故を回避できる `[Lv3]`
同種の CPU 負荷で **Lv2(固定3Pod)=98.8%エラー → Lv3(HPA 2→10)=0%エラー**。
負荷開始 ~45 秒で自動スケール。教訓5「真の解＝飽和する前に容量を足す」を実証。
上げは即・下げは安定化（60s）でフラッピングを防ぐ。

## 10. HPA の分母は `requests.cpu`、metrics-server が要る `[Lv3]`
利用率 = 使用量 / `requests.cpu`。だから requests を適切に置く必要がある。
`limits.cpu` は付けない（教訓5の CFS スロットリング回避）。k3s は metrics-server 同梱。

## 11. Pod は増やせても、コア／ノードには上限がある `[Lv3]`
maxReplicas 到達後も利用率 166% のまま = 真の限界は**ノード数（実コア）**。
次は Cluster Autoscaler でノード自体を増やす（自宅なら物理サーバー追加）。
※ k3d の複数ノードは 1 台上の同じコアを共有＝トポロジ学習用で実コアは増えない点に注意。

## 12. 単一 DB は水平スケールの外に残る `[Lv3]`
api は 10 Pod に増えても Postgres は 1。アプリのスケールと DB のスケールは別問題。
次の壁 = リードレプリカ / コネクションプーラ(PgBouncer) / 分散 DB。

## 13. ⭐ アプリを増やすと単一 DB は「接続数」で先に枯れる `[Lv4]`
api 4 レプリカ × `PG_POOL_MAX=15` = 最大 60 本の接続要求 vs DB `max_connections=20`。
負荷をかけると `FATAL: sorry, too many clients already` が 3751 件、エラー率 7.61%。
**アプリの水平スケールは、そのまま単一 DB への接続数の掛け算になる。**

## 14. ⭐ コネクションプーラは client 接続と server 接続を分離する `[Lv4]`
PgBouncer(`pool_mode=transaction`) を挟むと、60 本の *client 接続* を受けつつ
DB への *server 接続* は `default_pool_size=15` に集約 → 15 ≤ 20 で枯渇しない。
**同一負荷・同じ DB 設定のまま 7.61% → 0.00% エラー。** transaction モードは
トランザクション単位で server 接続を貸し借りするので短い query 多数の Web API に最適
（session モードは掴みっぱなしで効果薄。ただし transaction はセッション状態を跨げない制約あり）。

## 15. ⭐ スケールの壁は throughput とは限らない `[Lv4]`
Lv4 の壁ではスループットは直結もプーラもほぼ同じ（1227 vs 1232 req/s）。
DB にはまだ余裕があり、壊れていたのは**可用性（接続枯渇の 5xx）**だった。
プーラが消したのは速度でなく**エラー**。Lv0-1 の「レプリカで throughput が伸びる」壁とは別種。
＝ 計測はエラー率まで見る(教訓6)に加え、「何が壁か（速度 or 可用性 or 接続）」を毎回見極める。

## 16. ⭐ read/write 分離は primary を確実にオフロードする（が、それだけ） `[Lv5]`
readPool→replica / writePool→primary に割ると、read 95% 負荷で **primary の CPU が 130%→22%、
接続が 67→28** に激減。オフロード自体は本物で、write の余力・可用性は確実に上がる。
壁が「消えた」ように見えるのは primary 単体を見た時だけ。

## 17. ⭐⭐ 単一ホストでは壁は「消えず移動する」 `[Lv5]`
分離しても **write p95 は 145→147ms でほぼ不変、read と throughput はむしろ悪化**（158 vs 137ms / 2794 vs 3121 req/s）。
primary から剥がした read の CPU 負荷が、**同じコアを共有する replica に移って 120% で飽和**しただけ。
箱全体は依然 CPU 律速で、レプリケーション overhead のぶん僅かに損。
＝**分離という「パターン」は正しいが、「効果」はトポロジ（replica が別ハードにあるか）で決まる。**
Lv3 の「k3d 複数ノードは同じコアを共有＝実コアは増えない」（教訓11）が DB 層で再来。
read の水平スケールを実測したいなら replica を**別マシン**に置くしかない。

## 18. レプリケーションラグは「負荷中に」測らないと嘘になる `[Lv5]`
`now() - pg_last_xact_replay_timestamp()` は**負荷中** ~20–70ms（read-after-write 不整合の源）だが、
**アイドル時は青天井に増える**（新規トランザクションが無く replay 時刻が古びるだけ＝遅延でなくアーティファクト）。
また replica は cold start 時、primary の `CREATE TABLE` が届く前に read が来ると `relation does not exist` で 500 しうる
→ 起動時に readPool 側でテーブル出現を待ってから listen する（教訓6「計測・観測は素直な条件で」の運用版）。

## 19. ⭐⭐ 「負荷を移す」施策と「負荷を消す」施策は単一ホストで正反対に効く `[Lv5→Lv6]`
同じ read-heavy 負荷・同じ単一ホストで:
- **Lv5 read replica（移す）**: read を replica に移すだけ。@400VU で read p95 改善せず（147ms）、
  同じコアを共有するので箱全体は CPU 律速のまま＝壁は「移動」しただけ。
- **Lv6 Redis cache（消す）**: ヒット時に DB クエリ自体を消す（in-memory GET ≪ Postgres sort/scan）。
  @400VU で **read p95 636→99ms（−84%）、throughput 1207→3499（+190%）**。DB が飽和しなくなる。

＝ **単一ホストでスケールさせたいなら「負荷をどこかに移す」より「負荷を消す（キャッシュ・計算削減）」が効く。**
Lv3/5 のコア共有の罠は、仕事の総量が減らない限り付いて回る。

## 20. ⭐ キャッシュの効果は「read 比率」でなく「無効化の頻度」で決まる `[Lv6]`
read95:write5 でも、write ごとに単一キーを `DEL` で全消しするので、高負荷では ~7ms に 1 回
キャッシュが飛ぶ。実測 hit 率は 0.95 でなく **~76%**。DEL 直後は全レプリカが同時 miss して
DB に殺到する（**キャッシュスタンピード**）。write が稀なデータほどキャッシュは効く。

## 21. ⭐ cache-aside の `DEL` は read-after-write を保証しない `[Lv6]`
並行 GET が INSERT 前の DB を読み、その古いスナップショットを DEL の**後**に SET すると、
TTL 切れまで古い値がキャッシュに残る。逐次操作なら反映されるが並行下では保証なし。
「DEL すれば即整合」は嘘 ＝ キャッシュ整合の一番の難所。TTL は最終収束の安全網に過ぎない。
Redis はベストエフォート層に留め（`/ready` に含めない・障害時は DB フォールバック）、
可用性の依存先にしない。

## 22. ⭐⭐ probe 雪崩の根本原因は「コア不足」でなく「イベントループの占有」`[Lv2→Lv7]`
Lv2 で発見した probe 雪崩（CPU バーン→ /health・/ready がタイムアウト→ k8s が健全な Pod を Evict→連鎖）を、
Lv7 で **replicas: 1・単一プロセスのまま** worker_threads offload だけで根治した。実測:
同期バーン中の `/health` 失敗率 **46.38% → 0%**、p95 **1s（timeout 上限）→ 7.71ms**。
- **Lv3 の HPA（Pod 増）は回避策** — コアを足して CPU 律速を先送りするが、1 プロセスが焼ければ
  そのプロセスの probe はやはり飢える（雪崩の火種は残る）。
- **Lv7 の worker offload は根治** — CPU をイベントループから引き剥がすので、飽和中も probe が即応。
＝ **水平スケール（Pod 増）とスレッド並列（worker 増）は直交する手段。** 前者は容量を足し、後者は
イベントループを守る。雪崩の本当の原因はコア不足でなくループ占有なので、後者でしか根治しない。
（副産物: 4 スレッドが 4 コアを並列使用して throughput も約 4.3 倍。）

## 23. ⭐ 「イベントループを空ける」だけでは半分。上限キュー＋503 の背圧まで込みで解 `[Lv7]`
CPU を worker に逃がせばループは空くが、**無限キュー**にすると `/work` レイテンシが青天井に伸び、
滞留 job でメモリも膨らむ＝**壁が別の場所（レイテンシ・メモリ）に移動しただけ**（Lv5「負荷を移す」の再演）。
上限キュー（`WORKER_QUEUE_MAX`）を超えたら即 **503** を返し「捌けない負荷は落とす＝背圧」にするのが、
Lv2/3 の probe 雪崩（過負荷で全部を道連れ）への正しいアンチテーゼ。503 を返せば上位 LB/クライアントが
リトライを制御でき、システム全体が崩れない。過負荷時は「落とす」のが正しい振る舞い。

## 24. ⭐ 自作 worker プールは「worker の死」を一級市民として扱う `[Lv7]`
CPU を焼く実験は worker をクラッシュさせうる。素朴な実装だと (a) 死んだ worker にキュー先頭を
割り当て続ける、(b) `exit`（非 error 終了）で in-flight job が宙吊りになる——どちらも該当 `/work` が
**永久ハング**する（Fastify がレスポンスを返さない。qa レビューで検出）。`error`/`exit` 両方で
in-flight を reject し、worker を作り直して自己回復させるのが必須。ライブラリ（piscina）はこれを
隠すが、ラボでは自作して「プール飽和→背圧→worker 死→回復」の意思決定を露出させる方が学べる。

## 25. ⭐⭐ write-behind キューは「バッファ」であって「乗数」ではない `[Lv8]`
`POST` を 202 即返し（XADD）にすると client の体感 latency は DB commit から切り離せる。だが
**持続的な耐久 throughput は consumer の drain レート（DB commit 律速）のまま**で、enqueue の速さとは無関係。
sustained な enqueue レートが drain を超えると depth は際限なく伸び（＝ commit lag という負債）、
上限を付ければ 503 背圧に変わる。実測: 単一 consumer @400VU で depth が上限 100k に張り付き **write の 51% が 503**。
キューは write の壁を消さず、バーストを吸収して先送りするだけ。202≠永続化・at-least-once の重複・lag 分の
read-after-write ずれ、というコストも払う。

## 26. ⭐ 最適化の効果は「ボトルネックが本当にそこにあるか」で正負が反転する `[Lv5→Lv6→Lv8]`
Lv8 の write-behind は **DB が飽和して初めて勝つ**。低負荷（100VU、4 api が同期 INSERT を 40 コネクションで
並列に捌け DB が詰まらない）では、queue は XADD の 1 ホップぶん **同期 INSERT に負けた**（p95 20.7ms vs 13.7ms）うえ、
depth 0→41,562・commit lag ~19s の負債だけ残す。DB が飽和する高負荷（400VU）で初めて 202 即返し（72ms）が
同期 wall（120ms）に勝つ。Lv5「replica は単一ホストで効かない」/ Lv6「cache は hit 率次第」と同じ——
**ボトルネックの所在を実測で確かめずに施策を足すと、overhead を足すだけになりうる。**

## 27. ⭐ enqueue は drain より桁違いに軽いので producer は固定 consumer を必ず抜く `[Lv8]`
XADD（Redis への追記）は INSERT（DB commit/fsync）より桁違いに軽い。だから write-behind の耐久 throughput を
上げる本当のレバーは **「キュー追加」でなく「consumer の並列化/バッチ化」**。consumer を 1→3 に増やすと
背圧が 152k→81k（失敗 51%→35%）に減り drain はほぼ線形にスケールしたが、depth の立ち上がりは止まらなかった
（producer が抜く）。drain 容量を sustained write レートに合わせる（consumer 増設 or multi-row INSERT/COPY）のが本質。
「キューを挟めば write がスケールする」は幻想——キューは latency を切り離す道具で、throughput を増やす道具ではない。

## 28. ⭐ DB write の律速は「INSERT の行数」でなく「commit の回数」 `[Lv8→Lv9]`
Lv8 の consumer は N 件まとめて読んでも 1件ずつ INSERT+commit していた。これを **1本の multi-row INSERT
（`VALUES ($1),($2),...`）= 1 commit** に束ねると、consumer 数を 1 に固定したまま drain throughput が **6.4×**
（45,153→288,969 行）、平均 batch 32.9 行/commit、commit lag ~55s→~20ms。飽和して write の 54.84% を 503 で
弾いていたキュー（depth 100k 張り付き）が、depth ほぼ 0（ピーク 283）・**503 ゼロ**で流れるキューに変わった。
効くのは **commit の固定費（fsync/WAL flush/トランザクション）が行数でなく回数に比例する**から——33 行分の固定費を 1 回に畳む。
Lv8 教訓27「enqueue は INSERT より桁違いに軽い」と同じ構図が INSERT の中にもあった。**律速の単位を疑え（行 vs commit）。**
ただし代償: poison batch も shutdown 取りこぼしも **増幅係数 = batch size**（1 失敗が最大 batch size 件の巻き添え）＝
耐障害性の粒度を粗くする。また write p95 は per-row 67.7ms < batched 75.5ms に見えるが、per-row の値は受理された 45%
のみ（残り 55% は 503 即弾き）——**エラー率を見ないと latency に騙される**（Lv3 の再演）。

## 29. pipeline の「成功」を戻り値まで見ないと不変量が静かに壊れる `[Lv9]`
ioredis の `pipeline.exec()` は**個別コマンドのエラーでは reject せず** `[err, result]` の配列で resolve する
（reject は接続断など transport レベルのみ）。`await pipe.exec()` を try/catch するだけでは一部の XDEL/XACK 失敗を
取り逃し、「XACK 済みだが XDEL 未」= XLEN 恒久リークや重複再 INSERT が **warn すら出ず不可視**で進む。
逐次 await（per-row の `ackAndDelete`）なら各コマンドのエラーが自然に catch されるが、まとめて速くした瞬間に
このエラー可視性が抜ける。**バッチ化・pipeline 化は throughput と引き換えにエラーの粒度と可視性を失いやすい**
——戻り値配列を必ず走査する。

## 30. ⭐ 同じ fsync 壁でも「回数を削る」が「コストを削る」を圧倒する `[Lv9→Lv10]`
commit の fsync 固定費を攻める手は2つ。**バッチ化**＝fsync の「回数」を N→1 に削る（Lv9）。
**synchronous_commit=off**＝fsync の「cost」を削る＝commit が flush を待たず返る（Lv10）。実測すると:
per-row（fsync 未 amortize）で sync-off は drain +52%、batched（既に 59:1 amortize 済み）では +17% と、
**バッチ化の後では sync-off の伸びしろは小さい**。バッチ化単独は 400VU で drain 6.4×——回数削減が cost 削減を桁で上回る。
決定的なのは **sync-off 単独では regime を変えられない**点: per-row を sync-off にしても depth は上限 100k に張り付き
38% がなお 503（fsync 以外の per-row 固定費＝parse/plan・トランザクション・往復が残り、単一 consumer は producer を抜けない）。
飽和から救い出したのはバッチ化であって sync-off ではない。**二つは同じボトルネックへの代替レバーで、どちらが効くかは
「ボトルネックがまだそこに残っているか」で決まる**（教訓26 の再演）。なお単一 committer では commit_delay（group commit）は
効かない——fsync をまとめるには並列 committer が要る。代償は durability: クラッシュで直近数百 ms 分の commit 済みが消えうる（Lv8 の 202≠永続化の系譜）。

## 31. ⏳ 未実測 — group commit は「durability を保ったまま fsync を束ねる」手だが、並列 committer が要る `[Lv11]`
Lv10 で「単一 committer では commit_delay（group commit）が効かない／sync-off は durability と引き換え」と分かった。
Lv11 はその宿題: **synchronous_commit=on のまま**、consumer 内に N 本の committer coroutine を並行させ（各々専用の
read コネクション）、複数 commit を WAL flush 点に同時到達させて **1 fsync に束ねる**（`commit_delay`/`commit_siblings`）。
狙いは「durability を落とさず fsync を amortize」。**実測は未実施**（記録日はマシンが Spotlight 再インデックス＋
GUI 負荷で load 30–48・SIP で Spotlight 停止も不可 → 高 ambient load 下では単一 consumer の commit 挙動が CPU 争奪に
埋もれ group commit の効果が測れないため延期）。検証すべき仮説: ①並列 committer で commit_delay が発火するか、
②「committer 並列のみ(commit_delay=0)」との差分で group commit の**純効果**を分離（差が無ければ Lv8 の scale-out の
焼き直し）、③単一ホスト・800VU の CPU 飽和下では fsync 節約分が CPU 争奪に食われて効果が出ない可能性（Lv3/Lv5 の罠）。
→ 詳細と実測手順は `docs/learning-log/11-lv11.md`。**load<5 の窓が取れ次第 A/B を回して数値を確定する。**

## 32. ⭐ scale-to-zero は「idle コストを cold-start latency に変換する取引」— throughput は増えない `[Lv12]`
serverless の肝＝scale-to-zero を KEDA http-add-on で k3d に再現（EKS 不使用）。always-on(HPA min=2) ↔
scale-to-zero(min=0) を同一 image で背中合わせ実測: **cold-start 4,730ms vs 41ms（≈115×）**だが
**warm 定常は完全一致**（両アーム 82.6 req/s・p95 ~26ms・503 ゼロ）。**払うのは idle 後の初回一発だけで、
定常性能は一切犠牲にしない**。壁は消えず「idle 常駐コスト」→「初回 latency」へ移動する（Lv5「壁は移動する」/
Lv8「latency 切り離し≠throughput 増」の系譜）。2つの副次学び: ①**cold-start はエラーでなく latency に出る** —
interceptor が 0→1 の間リクエストをホールドするので burst でも 503 ゼロ（Lv2 の probe 事故＝502 高速返しの逆）。
②**scale-to-zero はタダではない** — app Pod=0 でも KEDA 制御面が ~160Mi 常駐し、1 workload なら always-on の
2 Pod(126Mi)より高い。idle 節約が黒字化するのは「制御面固定費 ÷ workload あたり節約」の損益分岐を超える
workload 数から（＝散発アクセスの休眠 workload が多数あるほど効く。常時トラフィックの単一 workload には
always-on が正解）。教訓6「数字は控除まで見ろ」の再演。※ Docker-Mac VM は cold-start が実機より膨れる（相対で読む）。

---

## メタな学び
- **同じアプリを全段で使い回し、基盤だけ変える**と、数字がフェアに比較でき「何が効いたか」を切り分けられる。
- **observability（`instance` 可視化・`/metrics`・k6）を最初の段から**入れると、後段の異常にすぐ気づける。

## 一言サマリ
> スケール = 多コアを使うこと。手段は言語かレプリカ。
> ただし計測はエラー率まで見ないと騙され、k8s では probe が飽和時に牙をむく。
> そして DB write は「行数」でなく「commit 回数」で詰まる——バッチ化は速さと引き換えに障害の粒度を粗くする。
> 同じ fsync 壁でも「回数を削る」が「コストを削る」を圧倒し、どのレバーも効くのは壁がまだそこにある時だけ。
