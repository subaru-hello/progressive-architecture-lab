# Lv17 — 解きほぐし（mud → hexagonal: DB/Domain/Usecase/Infra）

## 狙い
Lv16 の泥団子を **hexagonal(ports & adapters)** に組み替え、**結合度が seam 1 本に激減**することを実測する。
外部 HTTP・topology・latency は Lv13/mud と同じ——変わるのは内部構造だけ。usecase が RepoPort/ItemsPort
**interface** にしか依存しないので、次段 Lv18 で「アダプタ差し替え + DB 移動」だけで items をライブ抽出できる。
**hexagonal 境界＝事前に引いた移行の切断線**。

## 構成（層）
```
domains/<d>/  domain/(純 entity+不変条件・I/O ゼロ)  ports.ts(RepoPort interface)
              infra/pg-*-repo.ts(RepoPort を pg で実装)  usecase/(オーケストレーション)  routes.ts(HTTP アダプタ)
platform 相当の配線は server.ts(composition root) が担う。ARCH=hex(既定)。
```
- orders は domain/usecase/infra/ports/routes フル（オーケストレーションあり）。
- items/users は thin CRUD なので usecase 層を省く（routes→repo 直）＝**passthrough を作らない honest hexagonal**。

## 起動 / 停止
```bash
docker compose -f stages/17-hexagonal/docker-compose.yml up --build -d
docker compose -f stages/17-hexagonal/docker-compose.yml down -v
```

## 動作確認 & 結合度 after
```bash
# 外部契約は Lv13 と同一（stock 無しリスト・order 貫通・401/409/400）
curl -s -XPOST localhost:3000/orders -H 'x-auth-token: demo-token-1' -H 'content-type: application/json' -d '{"itemId":3,"qty":2}'
# 結合度 after（mud=JOIN1/FK2 → hex=0/0）
grep -rnE "JOIN (items|users)|REFERENCES (items|users)" app/src/domains
# 依存の向き（domain 層に I/O ゼロ）
grep -rnE "pg|fetch|fastify|redis|ioredis" app/src/domains/*/domain/*.ts   # 0 件
```

## 実測（結合度 before/after）
| | Lv16 mud | Lv17 hex |
|---|---|---|
| cross-domain 共有 tx / JOIN / 跨ぎ FK | 1 / 1 / 2 | **0 / 0 / 0** |
| cross-context の表現 | あちこちに絡む | **ItemsPort/UsersPort seam 1 本** |
| order_latency med | 5.85ms | **4.85ms（＝差なし・層化は runtime 無料）** |

## この段の限界（次に進む理由）
- 綺麗にしたが、まだ 1 プロセス・共有 1 DB。seam は引けたが「切って別 DB へ移す」のはこれから。
- → **Lv18** で hex seam から items をライブ抽出（strangler フル playbook）。
  **Lv16 で 100% ダウンした同じ引っ越しが、今度は k6 常時緑で通る**ことを示す。
