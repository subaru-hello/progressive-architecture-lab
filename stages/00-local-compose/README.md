# Lv0 — docker-compose（個人開発レベル）

## 狙い
アプリをコンテナ化し、Postgres と 1 対 1 で繋ぐ最小構成。DAU が小さいうちはこれで十分。
「1 プロセスで捌けるうちは一番安くて速い」を体感する段。

## 構成
```
[ localhost:3000 ] --> api (1 コンテナ) --> db (postgres:16)
```
- `api`: `app/` をビルドしたイメージ
- `db`: Postgres。`pgdata` ボリュームで永続化
- `depends_on: condition: service_healthy` で DB の起動完了を待ってから API 起動

## 起動 / 停止
```bash
# リポジトリルートで
docker compose -f stages/00-local-compose/docker-compose.yml up --build   # 起動（-d でバックグラウンド）
docker compose -f stages/00-local-compose/docker-compose.yml down          # 停止（-v でボリュームも削除）
```

## 動作確認
```bash
curl localhost:3000/ready
curl -X POST localhost:3000/items -H 'content-type: application/json' -d '{"name":"hello"}'
curl localhost:3000/items
curl 'localhost:3000/work?ms=50'      # 50ms 待機
curl 'localhost:3000/work?cpu=30'     # CPU バーン
```

## この段の限界（次に進む理由）
- `api` は 1 コンテナ。CPU を使う処理が増えると 1 プロセスで頭打ち。
- `docker compose up --scale api=3` で複数化しても、**手前に振り分ける LB がない**ので
  ポートを直に複数公開できず実運用にならない。
- → **Lv1** でリバースプロキシ（Caddy/Traefik）を前段に置き、複数レプリカへ負荷分散する。
