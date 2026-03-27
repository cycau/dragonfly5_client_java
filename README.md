# dragonfly5_client_java

Dragonfly5 サーバーに HTTP 経由で接続し、RDB 操作（Query / Execute / Transaction）を行う Java クライアントライブラリ。


## 要件

- Java 21 以上
- Maven 3.8 以上

## ライブラリとして取り込む

ソース: [github.com/cycau/dragonfly5_client_java](https://github.com/cycau/dragonfly5_client_java)  
配布: [JitPack](https://jitpack.io/)（タグ **`v1.0.0`**）。ビルド状況は [JitPack の当該ページ](https://jitpack.io/#cycau/dragonfly5_client_java/v1.0.0) で確認できる。

### Maven（`pom.xml`）

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>

<dependency>
  <groupId>com.github.cycau</groupId>
  <artifactId>dragonfly5_client_java</artifactId>
  <version>v1.0.0</version>
</dependency>
```

### Gradle（Kotlin DSL）

```kotlin
repositories {
    maven("https://jitpack.io")
}
dependencies {
    implementation("com.github.cycau:dragonfly5_client_java:v1.0.0")
}
```

### Gradle（Groovy）

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}
dependencies {
    implementation 'com.github.cycau:dragonfly5_client_java:v1.0.0'
}
```

API の使い方はこの README の「初期化」以降を参照。

## 初期化

### YAML ファイルから初期化

```java
import com.cycau.dragonfly5.rdbclient.RdbClient;

RdbClient.initWithYamlFile("config.yaml");
```

**config.yaml の例:**

```yaml
maxConcurrency: 100
defaultSecretKey: "secret-key"
defaultDatabase: "crm-system"

clusterNodes:
  - baseUrl: "http://localhost:5678"
    secretKey: "secret-key"
  - baseUrl: "http://localhost:5679"
    # secretKey 省略時は defaultSecretKey が適用される
```

| フィールド | 説明 |
|---|---|
| `maxConcurrency` | HTTP 同時リクエスト上限（最小 10） |
| `defaultSecretKey` | ノード個別の secretKey 未指定時に適用される共通キー |
| `defaultDatabase` | `getDefault()` 時に使用されるデフォルト DB 名 |
| `clusterNodes` | 接続先ノードの一覧 |

### コードから直接初期化

```java
import com.cycau.dragonfly5.rdbclient.RdbClient;
import com.cycau.dragonfly5.rdbclient.NodeEntry;

List<NodeEntry> nodes = List.of(
    new NodeEntry("http://localhost:5678", "secret-key"),
    new NodeEntry("http://localhost:5679", "secret-key")
);
RdbClient.init(nodes, "defaultDatabase", 100);
```

## 基本的な使い方

### Query（SELECT）

```java
import com.cycau.dragonfly5.rdbclient.*;

RdbClient client = RdbClient.getDefault();
// または: RdbClient client = RdbClient.get("crm-system", "");

Params params = new Params()
    .add("user-001", ValueType.STRING);

Records records = client.query(
    "SELECT * FROM users WHERE id = $1",
    params,
    null  // QueryOptions (省略可)
);

for (int i = 0; i < records.size(); i++) {
    Record row = records.get(i);
    System.out.println(row.get("id") + " " + row.get("email"));
}
```

### Query オプション（ページネーション・タイムアウト）

```java
Records records = client.query(
    "SELECT * FROM users WHERE status = $1",
    params,
    new QueryOptions(0, 100, 30)  // offset, limit, timeoutSec
);
```

### Execute（INSERT / UPDATE / DELETE）

```java
RdbClient client = RdbClient.getDefault();

Params params = new Params()
    .add("user-001", ValueType.STRING)
    .add("new@example.com", ValueType.STRING);

ExecuteResult result = client.execute(
    "UPDATE users SET email = $2 WHERE id = $1",
    params
);
System.out.printf("affected rows: %d%n", result.getEffectedRows());
```

## トランザクション

### 基本的な使い方（newTxDefault）

最もシンプルな方法。デフォルト DB・デフォルト分離レベル・自動 traceId で開始する。
`TxClient` は `AutoCloseable` を実装しているため、`try-with-resources` で自動的に Close される。

```java
try (TxClient tx = TxClient.newTxDefault()) {
    // トランザクション内で Execute
    tx.execute(
        "UPDATE users SET email = $2 WHERE id = $1",
        new Params()
            .add("user-001", ValueType.STRING)
            .add("tx@example.com", ValueType.STRING)
    );

    // トランザクション内で Query
    Records records = tx.query(
        "SELECT * FROM users WHERE id = $1",
        new Params().add("user-001", ValueType.STRING),
        null
    );

    // Commit
    tx.commit();
}  // 例外時は自動で rollback (Release)
```

### オプション指定（newTx）

DB 名・分離レベル・タイムアウトを明示的に指定する場合は `newTx` を使う。

```java
try (TxClient tx = TxClient.newTx(
    "crm-system",                    // DB 名（null/空文字でデフォルト）
    IsolationLevel.READ_COMMITTED,   // 分離レベル（null でサーバーデフォルト）
    60,                              // 最大タイムアウト秒（0 でサーバーデフォルト）
    ""                               // traceId（空文字で自動生成）
)) {
    // ...
    tx.commit();
}
```

| 引数 | 型 | 説明 |
|---|---|---|
| `databaseName` | `String` | 対象 DB 名。null/空文字で `defaultDatabase` を使用 |
| `isolationLevel` | `IsolationLevel` | トランザクション分離レベル。null でサーバーデフォルト |
| `maxTxTimeoutSec` | `int` | トランザクションの最大有効秒数。0 でサーバーデフォルト |
| `traceId` | `String` | トレース ID。空文字で `"d5" + nanoId(10)` を自動生成 |

### TxClient のメソッド一覧

| メソッド | 説明 |
|---|---|
| `query(sql, params, opts)` | トランザクション内で SELECT を実行 |
| `execute(sql, params)` | トランザクション内で INSERT/UPDATE/DELETE を実行 |
| `commit()` | コミット & クローズ |
| `rollback()` | ロールバック & クローズ |
| `close()` | ロールバック & クローズ（未コミット時のみ） |
| `getTxId()` | シリアライズ可能なトランザクション ID を返す |

### トランザクション ID の復元（restoreTx）

別プロセス・別リクエストでトランザクションを引き継ぐ場合:

```java
// トランザクション ID を取得して保存
String txId = tx.getTxId();

// 別の場所で復元
try (TxClient restoredTx = TxClient.restoreTx(txId)) {
    // 復元した TxClient で操作を続行
    restoredTx.commit();
}
```

## パラメータの型 (ValueType)

| ValueType | Java の型 | 説明 |
|---|---|---|
| `NULL` | `null` | NULL |
| `BOOL` | `boolean` | 真偽値 |
| `INT` | `short` / `int` / `long` | 整数 |
| `LONG` | `long` | 長整数 |
| `FLOAT` | `float` | 単精度浮動小数 |
| `DOUBLE` | `double` | 倍精度浮動小数 |
| `DECIMAL` | `BigDecimal` | 任意精度小数（内部で文字列として送信） |
| `DATE` | `String` (ISO-8601) | 日付 |
| `DATETIME` | `String` (ISO-8601) | 日時 |
| `STRING` | `String` | 文字列 |
| `BYTES` | `byte[]` | バイナリ（Base64 エンコードで送信） |
| `AS_IS` | `Object` | そのまま JSON シリアライズ |

## クエリ結果の型

レスポンスは JSON またはバイナリプロトコル (v4) で返却され、DB 型に応じて Java の型に自動変換される。

| DB 型 | Java の型 |
|---|---|
| `BOOL`, `BOOLEAN` | `Boolean` |
| `INT`, `INT2`, `INT4`, `SMALLINT`, `TINYINT`, `MEDIUMINT` | `Integer` |
| `BIGINT`, `INT8` | `Long` |
| `FLOAT`, `FLOAT4` | `Float` |
| `DOUBLE`, `FLOAT8` | `Double` |
| `NUMERIC`, `DECIMAL` | `BigDecimal` |
| `VARCHAR`, `CHAR`, `TEXT` 等 | `String` |
| `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, `DATETIME` | `Instant` |
| `BYTEA` | `byte[]` |
| その他 (`UUID`, `INTERVAL`, `BIT` 等) | `String`（そのまま） |

型安全なアクセスには `Record.getAs()` を使用:

```java
String name = record.getAs("name", String.class);
Integer age = record.getAs("age", Integer.class);
Instant createdAt = record.getAs("created_at", Instant.class);
```

## トランザクション分離レベル

| 定数 | 値 |
|---|---|
| `IsolationLevel.READ_UNCOMMITTED` | `READ_UNCOMMITTED` |
| `IsolationLevel.READ_COMMITTED` | `READ_COMMITTED` |
| `IsolationLevel.REPEATABLE_READ` | `REPEATABLE_READ` |
| `IsolationLevel.SERIALIZABLE` | `SERIALIZABLE` |

## クラスタ・ノード管理

- 初期化時に `/healz` で全ノードのヘルスチェックを実行し、データソース情報を取得する
- リクエスト時は、DB 名とエンドポイント種別に基づいて重み付きランダムでノードを選択する
- ノード障害時は自動リトライ（最大 2 回）し、別ノードへフォールオーバーする
- サーバーからの `307 Temporary Redirect` に追従し、指定ノードへリクエストを転送する
- トランザクション操作は BEGIN 時に決定されたノードに固定される
- 同時実行数はセマフォで制御され、通常リクエスト 80% / トランザクション 20% で配分される

## エラーハンドリング

すべての操作は `HaveError`（checked exception）をスローする。

```java
try {
    Records records = client.query("SELECT ...", params, null);
} catch (HaveError e) {
    System.err.println(e.getErrCode() + ": " + e.getMessage());
}
```

主なエラーコード:

| ErrCode | 説明 |
|---|---|
| `NETWORK_ERROR` | ネットワーク障害 |
| `NodeSelectionError` | 利用可能なノードなし |
| `RedirectCountExceeded` | リダイレクト回数超過 |
| `TxClosedError` | クローズ済みトランザクションへの操作 |



## ライセンス

Apache License 2.0
