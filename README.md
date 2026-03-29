# Catalog Explorer
## 事前準備
- SQLウェアハウスの作成と権限の付与
  - アプリの「許可」タブの「アプリの認証」で、アプリで使用されているサービスプリンシパルのIDをコピー
  - 作成したSQLウェアハウスの「権限」から、サービスプリンシパルに対して「使用可能」権限を付与
- 確認したいデータに対する権限付与
  - 確認したいデータのカタログ、スキーマ、テーブルに対して「使用可能」権限を付与

## 権限モデルについて：BROWSE 権限とデータアクセスの違い

このアプリでは、カタログやスキーマの一覧が権限を、明示的に設定していなくても表示される場合があります。
これは Unity Catalog の **BROWSE 権限** によるものです。

### BROWSE 権限とは

- Catalog Explorer（UI）で作成されたカタログには、デフォルトで **All account users** グループに `BROWSE` 権限が付与されます。アプリのサービスプリンシパルもこのグループに含まれます。
- `BROWSE` 権限があれば、`USE CATALOG` や `USE SCHEMA` が無くても、カタログ・スキーマ・テーブルの **名前やメタデータ（説明、タグ等）を閲覧** できます。
- 対象範囲：REST API、information_schema、リネージグラフ、検索結果など。

### BROWSE だけではデータは読めない

`BROWSE` はメタデータの閲覧のみを許可します。テーブルのデータ（SELECT）にアクセスするには、以下の権限がすべて必要です：

| 操作 | 必要な権限 |
| --- | --- |
| カタログ・スキーマ・テーブル一覧の表示 | `BROWSE`（デフォルトで付与される場合あり） |
| テーブルのデータ取得（SELECT） | `USE CATALOG` + `USE SCHEMA` + `SELECT` |
| テーブルへのデータ書き込み | `USE CATALOG` + `USE SCHEMA` + `MODIFY` |

### 注意事項

- **SQL 文（`CREATE CATALOG`）や REST API、Databricks CLI で作成されたカタログ** には、`BROWSE` がデフォルトで付与されません。必要に応じて以下を実行してください：
  ```sql
  GRANT BROWSE ON CATALOG <catalog_name> TO `account users`;
  ```

- カタログ一覧は見えるがサンプルデータが取得できない場合は、上記の `USE CATALOG` / `USE SCHEMA` / `SELECT` 権限が不足している可能性があります。
