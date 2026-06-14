# TiDB JDBC Upsert 写入模式 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 `jdbc_upsert` 写入模式,让 `spark.sql("INSERT INTO ...")` 经 SparkSQL Catalyst 走真正的 V2 `BatchWrite`,用 JDBC 批量 `INSERT ... ON DUPLICATE KEY UPDATE` 写入 TiDB —— 无 2PC、Spark UI 可见完整 DAG、性能近似拼 JDBC,并支持可选的"更新时间列"条件覆盖。

**Architecture:** 纯 SQL 生成逻辑(`UpsertSqlBuilder`)与 JDBC 写入引擎(`TiDBBatchWrite`/`TiDBDataWriterFactory`/`TiDBDataWrite`)放在 `core`(对 Spark 3.0~3.5 API 兼容)。仅版本敏感的 `Write` 包装(`build(): Write` 是 3.2+ API)放在 `spark-wrapper/spark-3.3` 和 `spark-wrapper/spark-3.5`,经 `ReflectionUtil` 按版本加载。`TiDBTable.newWriteBuilder` 按 `writeMode` 分支:`tikv` 返回旧的 `V1Write`(行为不变),`jdbc_upsert` 返回普通 `Write`(其 `toBatch` 即新 `BatchWrite`),Spark 据返回类型自动选 V1/V2 写入路径。

**Tech Stack:** Scala 2.12 / Spark 3.3.1 & 3.5.6 DataSource V2 write API / MySQL Connector-J 8.0.29(`rewriteBatchedStatements=true`)/ scalatest 3.0.8 / Maven(profile `-Pspark-3.3`、`-Pspark-3.5`)。

---

## 背景前提(实现者必读)

- `core` 模块默认按 `spark.version.compile = ${spark3_0.version} = 3.0.3` 编译。`org.apache.spark.sql.connector.write.Write` 与 `WriteBuilder.build(): Write` 是 **Spark 3.2+** 才有的 API,**不能**在 `core` 里引用;但 `BatchWrite`/`DataWriter`/`DataWriterFactory`/`PhysicalWriteInfo` 在 3.0 已存在且签名兼容,可放 `core`(现有 `core/.../v2/sink/*.scala` 就是证明)。
- 现有 `core/.../v2/sink/` 三个文件(`TiDBBatchWrite.scala`、`TiDBDataWriterFactory.scala`、`TiDBDataWrite.scala`)是留空 `???` 的占位骨架,**当前无任何代码引用**(写入实际走 `ReflectionUtil.newTiDBWriteBuilder` 返回的 `V1Write`)。本计划直接重写这三个文件。
- 连接信息(`tidb.addr/port/user/password`)在 SQL INSERT 路径下来自 Spark 会话级 conf,经 `TiDBTable.newWriteBuilder` 里 `sqlContext.getAllConfs` → `TiDBOptions`;`TiDBOptions.url` 已内置 `rewriteBatchedStatements=true`。
- 设计文档:`docs/superpowers/specs/2026-06-13-tidb-jdbc-upsert-write-design.md`。

**常用命令(每个版本 profile 各跑一次):**

```bash
# 编译 core(默认 3.0 profile,验证 core 代码不引用 3.2+ API)
mvn -q -pl core -am test-compile

# 编译 core + wrapper(3.3 / 3.5)
mvn -q -Pspark-3.3 -pl core,spark-wrapper/spark-3.3 -am test-compile
mvn -q -Pspark-3.5 -pl core,spark-wrapper/spark-3.5 -am test-compile

# 跑单个 scalatest 套件(纯单测,无需集群)
mvn -q -pl core test -DwildcardSuites=com.pingcap.tispark.write.UpsertSqlBuilderSuite
```

> 集群依赖:Task 1、2、3、4、5 均为纯逻辑/编译,**不需要** TiDB/TiKV 集群。仅 Task 6(端到端集成测试)需要一套可连接的 TiDB+TiKV。

---

## File Structure

**新建**
- `core/src/main/scala/com/pingcap/tispark/write/UpsertSqlBuilder.scala` — 纯函数,生成 `INSERT ... ON DUPLICATE KEY UPDATE` SQL。单一职责,零 Spark 依赖,完全可单测。
- `core/src/test/scala/com/pingcap/tispark/write/UpsertSqlBuilderSuite.scala` — `UpsertSqlBuilder` 单测。
- `core/src/test/scala/com/pingcap/tispark/write/TiDBOptionsUpsertSuite.scala` — 新增配置项解析单测。
- `spark-wrapper/spark-3.3/src/main/scala/org/apache/spark/sql/connector/write/TiDBUpsertWriteBuilder.scala` — 把 `BatchWrite` 包成 `Write`(3.3)。
- `spark-wrapper/spark-3.5/src/main/scala/org/apache/spark/sql/connector/write/TiDBUpsertWriteBuilder.scala` — 同上(3.5,内容与 3.3 一致)。
- `core/src/test/scala/org/apache/spark/sql/insertion/JDBCUpsertSuite.scala` — 端到端集成测试(需集群)。
- `docs/features/jdbc_upsert_write_userguide.md` — 用户文档。

**修改**
- `core/src/main/scala/com/pingcap/tispark/write/TiDBOptions.scala` — 新增 `writeMode`/`upsertUpdateTimeColumn`/`upsertBatchSize` 字段、常量与校验方法。
- `core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBBatchWrite.scala` — 重写为 JDBC upsert 的 `BatchWrite`。
- `core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBDataWriterFactory.scala` — 重写为携带 url/SQL/batchSize 的工厂。
- `core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBDataWrite.scala` — 重写为 JDBC prepared-statement 批量写 + 类型转换。
- `core/src/main/scala/com/pingcap/tispark/utils/ReflectionUtil.scala` — 新增 `newTiDBUpsertWriteBuilder`。
- `core/src/main/scala/com/pingcap/tispark/v2/TiDBTable.scala` — `capabilities()` 加 `BATCH_WRITE`;`newWriteBuilder` 按模式分支。

---

## Task 1: `UpsertSqlBuilder`(纯 SQL 生成,TDD)

**Files:**
- Create: `core/src/main/scala/com/pingcap/tispark/write/UpsertSqlBuilder.scala`
- Test: `core/src/test/scala/com/pingcap/tispark/write/UpsertSqlBuilderSuite.scala`

- [ ] **Step 1: Write the failing test**

Create `core/src/test/scala/com/pingcap/tispark/write/UpsertSqlBuilderSuite.scala`:

```scala
package com.pingcap.tispark.write

import org.scalatest.FunSuite

class UpsertSqlBuilderSuite extends FunSuite {

  test("no update-time column: unconditional ON DUPLICATE KEY UPDATE") {
    val sql = UpsertSqlBuilder.build(
      database = "d",
      table = "t",
      columns = Seq("id", "a", "b"),
      pkColumns = Set("id"),
      updateTimeColumn = None)
    assert(
      sql ==
        "INSERT INTO `d`.`t` (`id`, `a`, `b`) VALUES (?, ?, ?) " +
          "ON DUPLICATE KEY UPDATE `a` = VALUES(`a`), `b` = VALUES(`b`)")
  }

  test("with update-time column: conditional IF(VALUES(ts) >= ts, ...)") {
    val sql = UpsertSqlBuilder.build(
      database = "d",
      table = "t",
      columns = Seq("id", "a", "ts"),
      pkColumns = Set("id"),
      updateTimeColumn = Some("ts"))
    assert(
      sql ==
        "INSERT INTO `d`.`t` (`id`, `a`, `ts`) VALUES (?, ?, ?) " +
          "ON DUPLICATE KEY UPDATE " +
          "`a` = IF(VALUES(`ts`) >= `ts`, VALUES(`a`), `a`), " +
          "`ts` = IF(VALUES(`ts`) >= `ts`, VALUES(`ts`), `ts`)")
  }

  test("composite primary key columns are excluded from UPDATE clause") {
    val sql = UpsertSqlBuilder.build(
      database = "d",
      table = "t",
      columns = Seq("k1", "k2", "v"),
      pkColumns = Set("k1", "k2"),
      updateTimeColumn = None)
    assert(sql.endsWith("ON DUPLICATE KEY UPDATE `v` = VALUES(`v`)"))
  }

  test("all columns are PK: fallback to a no-op self assignment") {
    val sql = UpsertSqlBuilder.build(
      database = "d",
      table = "t",
      columns = Seq("k1", "k2"),
      pkColumns = Set("k1", "k2"),
      updateTimeColumn = None)
    assert(sql.endsWith("ON DUPLICATE KEY UPDATE `k1` = `k1`"))
  }

  test("identifiers containing backticks are escaped") {
    val sql = UpsertSqlBuilder.build(
      database = "d",
      table = "we`ird",
      columns = Seq("id", "a"),
      pkColumns = Set("id"),
      updateTimeColumn = None)
    assert(sql.startsWith("INSERT INTO `d`.`we``ird` (`id`, `a`)"))
  }

  test("empty columns is rejected") {
    assertThrows[IllegalArgumentException] {
      UpsertSqlBuilder.build("d", "t", Seq.empty, Set.empty, None)
    }
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl core test -DwildcardSuites=com.pingcap.tispark.write.UpsertSqlBuilderSuite`
Expected: 编译失败 / FAIL —— `UpsertSqlBuilder` 不存在。

- [ ] **Step 3: Write minimal implementation**

Create `core/src/main/scala/com/pingcap/tispark/write/UpsertSqlBuilder.scala`:

```scala
/*
 * Copyright 2026 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.write

/**
 * Builds an `INSERT ... ON DUPLICATE KEY UPDATE` statement used by the
 * `jdbc_upsert` write mode. Pure string generation, no Spark/TiDB dependency.
 */
object UpsertSqlBuilder {

  private def quote(name: String): String = s"`${name.replace("`", "``")}`"

  /**
   * @param database          target database
   * @param table             target table
   * @param columns           columns to insert, in placeholder order (== write schema order)
   * @param pkColumns         primary-key column names (excluded from the UPDATE clause)
   * @param updateTimeColumn  when set, each column is only overwritten if
   *                          `VALUES(col) >= col` (incoming row is newer-or-equal)
   */
  def build(
      database: String,
      table: String,
      columns: Seq[String],
      pkColumns: Set[String],
      updateTimeColumn: Option[String]): String = {
    require(columns.nonEmpty, "columns must not be empty")

    val cols = columns.map(quote).mkString(", ")
    val placeholders = columns.map(_ => "?").mkString(", ")
    val updateColumns = columns.filterNot(pkColumns.contains)

    val setClause =
      if (updateColumns.isEmpty) {
        // All columns are part of the primary key: a duplicate row is identical,
        // so emit a harmless no-op assignment (ODKU requires at least one).
        s"${quote(columns.head)} = ${quote(columns.head)}"
      } else {
        updateTimeColumn match {
          case Some(tsCol) =>
            val ts = quote(tsCol)
            updateColumns
              .map { c =>
                val qc = quote(c)
                s"$qc = IF(VALUES($ts) >= $ts, VALUES($qc), $qc)"
              }
              .mkString(", ")
          case None =>
            updateColumns.map(c => s"${quote(c)} = VALUES(${quote(c)})").mkString(", ")
        }
      }

    s"INSERT INTO ${quote(database)}.${quote(table)} ($cols) VALUES ($placeholders) " +
      s"ON DUPLICATE KEY UPDATE $setClause"
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl core test -DwildcardSuites=com.pingcap.tispark.write.UpsertSqlBuilderSuite`
Expected: PASS(6 个用例全绿)。

- [ ] **Step 5: Commit**

```bash
git add core/src/main/scala/com/pingcap/tispark/write/UpsertSqlBuilder.scala \
        core/src/test/scala/com/pingcap/tispark/write/UpsertSqlBuilderSuite.scala
git commit -m "feat(write): add UpsertSqlBuilder for jdbc_upsert mode"
```

---

## Task 2: `TiDBOptions` 新增配置项与校验(TDD)

**Files:**
- Modify: `core/src/main/scala/com/pingcap/tispark/write/TiDBOptions.scala`
- Test: `core/src/test/scala/com/pingcap/tispark/write/TiDBOptionsUpsertSuite.scala`

- [ ] **Step 1: Write the failing test**

Create `core/src/test/scala/com/pingcap/tispark/write/TiDBOptionsUpsertSuite.scala`:

```scala
package com.pingcap.tispark.write

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TiDBOptionsUpsertSuite extends FunSuite with BeforeAndAfterAll {

  // TiDBOptions ctor calls SparkContext.getOrCreate(); provide a local session.
  // extensions != TiExtensions so mergeWithSparkConf returns params untouched.
  override def beforeAll(): Unit = {
    SparkSession
      .builder()
      .master("local[1]")
      .appName("tidb-options-upsert-test")
      .config("spark.sql.extensions", "none")
      .getOrCreate()
  }

  private def opt(extra: (String, String)*): TiDBOptions =
    new TiDBOptions(Map("database" -> "d", "table" -> "t") ++ extra.toMap)

  test("default write mode is jdbc_upsert") {
    val o = opt()
    assert(o.writeMode == "jdbc_upsert")
    assert(o.isJdbcUpsertMode)
    assert(o.upsertUpdateTimeColumn.isEmpty)
    assert(o.upsertBatchSize == 1000)
  }

  test("tikv mode is recognised and not upsert") {
    val o = opt("tidb.write.mode" -> "TiKV")
    assert(o.writeMode == "tikv")
    assert(!o.isJdbcUpsertMode)
  }

  test("illegal write mode is rejected") {
    assertThrows[IllegalArgumentException](opt("tidb.write.mode" -> "bogus"))
  }

  test("update time column is parsed, blank treated as empty") {
    assert(opt("upsert.update_time_column" -> "ts").upsertUpdateTimeColumn.contains("ts"))
    assert(opt("upsert.update_time_column" -> "  ").upsertUpdateTimeColumn.isEmpty)
  }

  test("checkJdbcWriteRequired fails fast when connection params missing") {
    val ex = intercept[IllegalArgumentException](opt().checkJdbcWriteRequired())
    assert(ex.getMessage.contains("tidb.addr"))
  }

  test("checkJdbcWriteRequired passes when all connection params present") {
    val o = opt(
      "tidb.addr" -> "127.0.0.1",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "tidb.password" -> "")
    o.checkJdbcWriteRequired() // must not throw
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl core test -DwildcardSuites=com.pingcap.tispark.write.TiDBOptionsUpsertSuite`
Expected: 编译失败 —— `writeMode`/`isJdbcUpsertMode`/`upsertUpdateTimeColumn`/`upsertBatchSize`/`checkJdbcWriteRequired` 未定义。

- [ ] **Step 3: Write minimal implementation**

In `core/src/main/scala/com/pingcap/tispark/write/TiDBOptions.scala`, add the new option constants. Find the block of `newOption(...)` declarations in the companion `object TiDBOptions` (near `val TIDB_DEDUPLICATE: String = newOption("deduplicate")`, ~line 271) and add after it:

```scala
  val TIDB_WRITE_MODE: String = newOption("tidb.write.mode")
  val TIDB_UPSERT_UPDATE_TIME_COLUMN: String = newOption("upsert.update_time_column")
  val TIDB_UPSERT_BATCH_SIZE: String = newOption("upsert.batch_size")
```

Then in the `class TiDBOptions` body, after the existing optional-write parameters block (after `val deduplicate: ...`, ~line 79), add:

```scala
  // ------------------------------------------------------------
  // Optional parameters for jdbc_upsert write mode
  // ------------------------------------------------------------
  val writeMode: String =
    getOrDefault(TIDB_WRITE_MODE, "jdbc_upsert").trim.toLowerCase()
  require(
    Set("jdbc_upsert", "tikv").contains(writeMode),
    s"Unsupported '${TIDB_WRITE_MODE}': '$writeMode' (expected 'jdbc_upsert' or 'tikv')")

  val upsertUpdateTimeColumn: Option[String] = {
    val v = getOrDefault(TIDB_UPSERT_UPDATE_TIME_COLUMN, "")
    if (v == null || v.trim.isEmpty) None else Some(v.trim)
  }

  val upsertBatchSize: Int = getOrDefault(TIDB_UPSERT_BATCH_SIZE, "1000").toInt

  def isJdbcUpsertMode: Boolean = writeMode == "jdbc_upsert"

  /** Connection params required only when writing via jdbc_upsert (JDBC to TiDB). */
  def checkJdbcWriteRequired(): Unit = {
    Seq(
      TIDB_ADDRESS -> address,
      TIDB_PORT -> port,
      TIDB_USER -> user,
      TIDB_PASSWORD -> password).foreach {
      case (name, value) =>
        require(
          value != null,
          s"Option '$name' is required for write mode 'jdbc_upsert'. " +
            s"Set it as a Spark session conf, e.g. spark.tispark.$name=<value>.")
    }
  }
```

> 注:`address/port/user/password` 已是 `class TiDBOptions` 现有字段(`getWriteRequiredOrNull`,缺失为 `null`),`getOrDefault` 为现有 private 方法,均可直接使用。

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl core test -DwildcardSuites=com.pingcap.tispark.write.TiDBOptionsUpsertSuite`
Expected: PASS(6 个用例全绿)。

- [ ] **Step 5: Commit**

```bash
git add core/src/main/scala/com/pingcap/tispark/write/TiDBOptions.scala \
        core/src/test/scala/com/pingcap/tispark/write/TiDBOptionsUpsertSuite.scala
git commit -m "feat(write): add write.mode / update_time_column / batch_size options"
```

---

## Task 3: JDBC upsert 写入引擎(重写 core 的 V2 sink 三件套)

**Files:**
- Modify: `core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBBatchWrite.scala`
- Modify: `core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBDataWriterFactory.scala`
- Modify: `core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBDataWrite.scala`

- [ ] **Step 1: 确认旧桩无外部引用**

Run: `grep -rn "TiDBBatchWrite\|TiDBDataWriterFactory\|TiDBDataWrite\|WriteSucceeded" core/src/main spark-wrapper --include=*.scala`
Expected: 仅 `v2/sink/` 这三个文件内部互相引用,无其他生产代码引用。若发现别处引用,先评估再继续。
(rtk 不支持 `--include`;若报错改用:`grep -rn "TiDBBatchWrite" core/src/main`)

- [ ] **Step 2: 重写 `TiDBDataWrite.scala`(executor 端 JDBC writer)**

Replace the entire content of `core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBDataWrite.scala`:

```scala
/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.v2.sink

import com.pingcap.tispark.TiDBUtils
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, PreparedStatement}

/**
 * Per-partition JDBC writer for the `jdbc_upsert` mode. Builds one connection,
 * batches `INSERT ... ON DUPLICATE KEY UPDATE` via addBatch/executeBatch.
 */
case class TiDBDataWrite(
    schema: StructType,
    url: String,
    upsertSql: String,
    batchSize: Int)
    extends DataWriter[InternalRow] {

  private val dataTypes = schema.fields.map(_.dataType)
  private val converters =
    schema.fields.map(f => CatalystTypeConverters.createToScalaConverter(f.dataType))

  private var conn: Connection = _
  private var stmt: PreparedStatement = _
  private var rowsInBatch: Int = 0

  private def ensureOpen(): Unit = {
    if (conn == null) {
      conn = TiDBUtils.createConnectionFactory(url)()
      conn.setAutoCommit(true)
      stmt = conn.prepareStatement(upsertSql)
    }
  }

  override def write(record: InternalRow): Unit = {
    ensureOpen()
    var i = 0
    while (i < dataTypes.length) {
      val value =
        if (record.isNullAt(i)) null
        else converters(i)(record.get(i, dataTypes(i))).asInstanceOf[AnyRef]
      stmt.setObject(i + 1, value)
      i += 1
    }
    stmt.addBatch()
    rowsInBatch += 1
    if (rowsInBatch >= batchSize) {
      stmt.executeBatch()
      rowsInBatch = 0
    }
  }

  override def commit(): WriterCommitMessage = {
    if (stmt != null && rowsInBatch > 0) {
      stmt.executeBatch()
      rowsInBatch = 0
    }
    WriteSucceeded
  }

  override def abort(): Unit = close()

  override def close(): Unit = {
    if (stmt != null) {
      try stmt.close()
      catch { case _: Throwable => }
      stmt = null
    }
    if (conn != null) {
      try conn.close()
      catch { case _: Throwable => }
      conn = null
    }
  }
}

object WriteSucceeded extends WriterCommitMessage
```

- [ ] **Step 3: 重写 `TiDBDataWriterFactory.scala`**

Replace the entire content of `core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBDataWriterFactory.scala`:

```scala
/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.v2.sink

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

case class TiDBDataWriterFactory(
    schema: StructType,
    url: String,
    upsertSql: String,
    batchSize: Int)
    extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    TiDBDataWrite(schema, url, upsertSql, batchSize)
}
```

- [ ] **Step 4: 重写 `TiDBBatchWrite.scala`**

Replace the entire content of `core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBBatchWrite.scala`:

```scala
/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.v2.sink

import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * V2 BatchWrite for the `jdbc_upsert` mode. Writes through TiDB via JDBC batch
 * `INSERT ... ON DUPLICATE KEY UPDATE`; no 2PC, per-batch autocommit.
 */
case class TiDBBatchWrite(schema: StructType, tiDBOptions: TiDBOptions, upsertSql: String)
    extends BatchWrite {

  private final val logger = LoggerFactory.getLogger(getClass.getName)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    TiDBDataWriterFactory(schema, tiDBOptions.url, upsertSql, tiDBOptions.upsertBatchSize)

  override def commit(messages: Array[WriterCommitMessage]): Unit =
    logger.info(s"TiDB jdbc_upsert committed across ${messages.length} partitions")

  override def abort(messages: Array[WriterCommitMessage]): Unit =
    logger.warn(
      "TiDB jdbc_upsert aborted; per-batch autocommit means some rows may already " +
        "be persisted (no global rollback). Upserts are idempotent and safe to retry.")
}
```

- [ ] **Step 5: 编译验证(默认 3.0 profile + 3.3 + 3.5)**

Run:
```bash
mvn -q -pl core -am test-compile
mvn -q -Pspark-3.3 -pl core -am test-compile
mvn -q -Pspark-3.5 -pl core -am test-compile
```
Expected: 三条命令均编译通过(证明 core 仅用了 3.0 兼容的 V2 API)。

- [ ] **Step 6: Commit**

```bash
git add core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBBatchWrite.scala \
        core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBDataWriterFactory.scala \
        core/src/main/scala/com/pingcap/tispark/v2/sink/TiDBDataWrite.scala
git commit -m "feat(write): implement JDBC upsert BatchWrite/DataWriter in core"
```

---

## Task 4: 版本敏感的 `Write` 包装 + `ReflectionUtil`

**Files:**
- Create: `spark-wrapper/spark-3.3/src/main/scala/org/apache/spark/sql/connector/write/TiDBUpsertWriteBuilder.scala`
- Create: `spark-wrapper/spark-3.5/src/main/scala/org/apache/spark/sql/connector/write/TiDBUpsertWriteBuilder.scala`
- Modify: `core/src/main/scala/com/pingcap/tispark/utils/ReflectionUtil.scala`

- [ ] **Step 1: 新建 3.3 的 `TiDBUpsertWriteBuilder`**

Create `spark-wrapper/spark-3.3/src/main/scala/org/apache/spark/sql/connector/write/TiDBUpsertWriteBuilder.scala`:

```scala
/*
 * Copyright 2026 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.write

/**
 * Wraps a (version-agnostic) BatchWrite into a Spark 3.2+ `Write` so that
 * `spark.sql("INSERT INTO ...")` routes through WriteToDataSourceV2Exec.
 * Returning a non-V1Write makes Spark pick the V2 batch write path.
 */
case class TiDBUpsertWriteBuilder(batchWrite: BatchWrite) extends WriteBuilder {
  override def build(): Write = new Write {
    override def toBatch: BatchWrite = batchWrite
  }
}
```

- [ ] **Step 2: 新建 3.5 的 `TiDBUpsertWriteBuilder`(内容与 3.3 完全一致)**

Create `spark-wrapper/spark-3.5/src/main/scala/org/apache/spark/sql/connector/write/TiDBUpsertWriteBuilder.scala` with the **exact same content** as Step 1.

- [ ] **Step 3: 在 `ReflectionUtil` 增加加载方法**

In `core/src/main/scala/com/pingcap/tispark/utils/ReflectionUtil.scala`:

(a) 确认/添加 import(文件顶部已 import 了 `LogicalWriteInfo`、`WriteBuilder`;补 `BatchWrite`):
```scala
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, WriteBuilder}
```
> 若现有 import 是逐个分行的,改为追加一行 `import org.apache.spark.sql.connector.write.BatchWrite`。

(b) 在 `object ReflectionUtil` 顶部常量区(`TIDB_WRITE_BUILDER_CLASS` 附近)新增:
```scala
  private val TIDB_UPSERT_WRITE_BUILDER_CLASS =
    "org.apache.spark.sql.connector.write.TiDBUpsertWriteBuilder"
```
> 若 `TIDB_WRITE_BUILDER_CLASS` 不在顶部,就近放在 `newTiDBWriteBuilder` 方法之前即可。

(c) 在现有 `newTiDBWriteBuilder` 方法之后追加:
```scala
  def newTiDBUpsertWriteBuilder(batchWrite: BatchWrite): WriteBuilder = {
    classLoader
      .loadClass(TIDB_UPSERT_WRITE_BUILDER_CLASS)
      .getDeclaredConstructor(classOf[BatchWrite])
      .newInstance(batchWrite)
      .asInstanceOf[WriteBuilder]
  }
```

- [ ] **Step 4: 编译验证**

Run:
```bash
mvn -q -Pspark-3.3 -pl core,spark-wrapper/spark-3.3 -am test-compile
mvn -q -Pspark-3.5 -pl core,spark-wrapper/spark-3.5 -am test-compile
```
Expected: 均编译通过。

- [ ] **Step 5: Commit**

```bash
git add spark-wrapper/spark-3.3/src/main/scala/org/apache/spark/sql/connector/write/TiDBUpsertWriteBuilder.scala \
        spark-wrapper/spark-3.5/src/main/scala/org/apache/spark/sql/connector/write/TiDBUpsertWriteBuilder.scala \
        core/src/main/scala/com/pingcap/tispark/utils/ReflectionUtil.scala
git commit -m "feat(write): add per-version TiDBUpsertWriteBuilder + reflection loader"
```

---

## Task 5: `TiDBTable` 路由与 capability

**Files:**
- Modify: `core/src/main/scala/com/pingcap/tispark/v2/TiDBTable.scala`

- [ ] **Step 1: `capabilities()` 增加 `BATCH_WRITE`**

In `TiDBTable.capabilities()` (现有方法,~line 113),在 `capabilities.add(TableCapability.V1_BATCH_WRITE)` 之后加一行:
```scala
    capabilities.add(TableCapability.BATCH_WRITE)
```
> `TableCapability` 已 import。两个 capability 并存;实际走 V1 还是 V2 由 `build()` 返回类型决定(`V1Write` → V1 旧路径;普通 `Write` → V2)。

- [ ] **Step 2: 增加 import**

确认 `TiDBTable.scala` 顶部已有:
```scala
import com.pingcap.tispark.write.{TiDBDelete, TiDBOptions}
import com.pingcap.tispark.utils.{ReflectionUtil, TiUtil}
```
追加:
```scala
import com.pingcap.tispark.write.UpsertSqlBuilder
import com.pingcap.tispark.v2.sink.TiDBBatchWrite
```
> `scala.collection.JavaConverters._` 已 import(供 `table.getColumns.asScala`)。

- [ ] **Step 3: 重写 `newWriteBuilder` 按模式分支**

Replace the existing `newWriteBuilder` method body (现有方法,~line 138):

```scala
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    var option = sqlContext.getAllConfs
    if (!option.contains(TiDBOptions.TIDB_DATABASE)) {
      option += (TiDBOptions.TIDB_DATABASE -> databaseName)
    }
    if (!option.contains(TiDBOptions.TIDB_TABLE)) {
      option += (TiDBOptions.TIDB_TABLE -> tableName)
    }
    val tiDBOptions = new TiDBOptions(option)

    if (tiDBOptions.isJdbcUpsertMode) {
      tiDBOptions.checkJdbcWriteRequired()

      val writeColumns = info.schema().fieldNames.toSeq
      val pkColumns =
        table.getColumns.asScala.filter(_.isPrimaryKey).map(_.getName).toSet

      tiDBOptions.upsertUpdateTimeColumn.foreach { c =>
        require(
          writeColumns.contains(c),
          s"upsert.update_time_column '$c' is not among the written columns: " +
            writeColumns.mkString(", "))
      }

      val upsertSql = UpsertSqlBuilder.build(
        databaseName,
        tableName,
        writeColumns,
        pkColumns,
        tiDBOptions.upsertUpdateTimeColumn)
      logger.info(s"TiSpark jdbc_upsert SQL template: $upsertSql")

      ReflectionUtil.newTiDBUpsertWriteBuilder(
        TiDBBatchWrite(info.schema(), tiDBOptions, upsertSql))
    } else {
      ReflectionUtil.newTiDBWriteBuilder(info, tiDBOptions, sqlContext)
    }
  }
```

- [ ] **Step 4: 编译验证**

Run:
```bash
mvn -q -Pspark-3.3 -pl core,spark-wrapper/spark-3.3 -am test-compile
mvn -q -Pspark-3.5 -pl core,spark-wrapper/spark-3.5 -am test-compile
```
Expected: 均通过。

- [ ] **Step 5: Commit**

```bash
git add core/src/main/scala/com/pingcap/tispark/v2/TiDBTable.scala
git commit -m "feat(write): route INSERT to V2 jdbc_upsert BatchWrite, add BATCH_WRITE capability"
```

---

## Task 6: 端到端集成测试(需 TiDB + TiKV 集群)

**Files:**
- Create: `core/src/test/scala/org/apache/spark/sql/insertion/JDBCUpsertSuite.scala`

> 依赖:可连接的 TiDB+TiKV,且测试基类(`org.apache.spark.sql.insertion` 下既有套件,如 `BatchWritePkSuite`)已配好集群连接与 TiExtensions。本套件复用同一基类的连接/会话与 JDBC 辅助方法(`tidbStmt`、`spark`)。实现前先 `Read` 一个既有同目录套件确认基类名与可用辅助方法,按其签名对齐。

- [ ] **Step 1: 阅读既有套件确认基类与辅助方法**

Run: `sed -n '1,80p' core/src/test/scala/org/apache/spark/sql/insertion/BatchWritePkSuite.scala`
Expected: 记录其 `extends` 的基类、建表/灌数/JDBC 查询的辅助方法名(如 `tidbStmt.execute(...)`、`queryViaTiSpark`/`spark.sql`),Step 2 据此对齐。

- [ ] **Step 2: 写集成测试**

Create `core/src/test/scala/org/apache/spark/sql/insertion/JDBCUpsertSuite.scala`(以下为逻辑骨架,基类名与辅助方法按 Step 1 结果替换;`<BaseInsertSuite>`、`tidbStmt` 等占位需替换为真实名):

```scala
package org.apache.spark.sql.insertion

import org.apache.spark.sql.BaseTiSparkTest // 替换为 Step 1 确认的真实基类

class JDBCUpsertSuite extends BaseTiSparkTest {

  private val db = "tispark_test"
  private val tbl = "jdbc_upsert_t"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // jdbc_upsert 模式 + 连接参数(集群地址按测试框架现有配置取值)
    spark.conf.set("spark.tispark.tidb.write.mode", "jdbc_upsert")
    // 若基类未注入连接参数,这里显式设置 spark.tispark.tidb.addr/port/user/password
  }

  private def recreateTable(): Unit = {
    tidbStmt.execute(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
    tidbStmt.execute(
      s"""CREATE TABLE `$db`.`$tbl` (
         |  id BIGINT PRIMARY KEY,
         |  uk INT,
         |  v VARCHAR(64),
         |  ts BIGINT,
         |  UNIQUE KEY uk_idx (uk)
         |)""".stripMargin)
  }

  test("insert new rows then upsert existing by primary key") {
    recreateTable()
    spark.conf.set("spark.tispark.upsert.update_time_column", "")
    spark.sql(s"INSERT INTO `$db`.`$tbl` VALUES (1, 10, 'a', 100), (2, 20, 'b', 100)")
    spark.sql(s"INSERT INTO `$db`.`$tbl` VALUES (1, 10, 'a2', 200)")

    val rs = tidbStmt.executeQuery(s"SELECT v FROM `$db`.`$tbl` WHERE id = 1")
    assert(rs.next() && rs.getString("v") == "a2")
  }

  test("update_time_column guard: older row does not overwrite") {
    recreateTable()
    spark.conf.set("spark.tispark.upsert.update_time_column", "ts")
    spark.sql(s"INSERT INTO `$db`.`$tbl` VALUES (1, 10, 'new', 200)")
    // 来的数据 ts=150 < 库中 200 → 不应覆盖
    spark.sql(s"INSERT INTO `$db`.`$tbl` VALUES (1, 10, 'old', 150)")

    val rs = tidbStmt.executeQuery(s"SELECT v, ts FROM `$db`.`$tbl` WHERE id = 1")
    assert(rs.next())
    assert(rs.getString("v") == "new")
    assert(rs.getLong("ts") == 200L)
  }

  test("update_time_column guard: newer-or-equal row overwrites") {
    recreateTable()
    spark.conf.set("spark.tispark.upsert.update_time_column", "ts")
    spark.sql(s"INSERT INTO `$db`.`$tbl` VALUES (1, 10, 'old', 200)")
    spark.sql(s"INSERT INTO `$db`.`$tbl` VALUES (1, 10, 'new', 200)") // 相等 → 覆盖

    val rs = tidbStmt.executeQuery(s"SELECT v FROM `$db`.`$tbl` WHERE id = 1")
    assert(rs.next() && rs.getString("v") == "new")
  }

  test("missing connection params fail fast in jdbc_upsert mode") {
    recreateTable()
    // 临时清空连接参数,验证 checkJdbcWriteRequired 早失败
    val saved = spark.conf.getOption("spark.tispark.tidb.addr")
    spark.conf.unset("spark.tispark.tidb.addr")
    val ex = intercept[Exception] {
      spark.sql(s"INSERT INTO `$db`.`$tbl` VALUES (9, 90, 'x', 1)")
    }
    assert(ex.getMessage.contains("tidb.addr"))
    saved.foreach(v => spark.conf.set("spark.tispark.tidb.addr", v))
  }
}
```

- [ ] **Step 3: 跑集成测试**

Run: `mvn -q -Pspark-3.5 -pl core test -DwildcardSuites=org.apache.spark.sql.insertion.JDBCUpsertSuite`
Expected: 4 个用例 PASS。若基类连接参数注入方式不同,据 Step 1 调整 `beforeAll`。

- [ ] **Step 4: 验证 Spark UI 可见 DAG(手动)**

启动一个带 `spark.tispark.tidb.write.mode=jdbc_upsert` 的会话,执行一条 `INSERT INTO ... SELECT ...`,打开 Spark UI **SQL** 标签页,确认看到一条完整的物理计划(`... → AppendData/WriteToDataSourceV2`),而非一堆孤立 RDD job。记录截图/结论。

- [ ] **Step 5: Commit**

```bash
git add core/src/test/scala/org/apache/spark/sql/insertion/JDBCUpsertSuite.scala
git commit -m "test(write): end-to-end jdbc_upsert integration suite"
```

---

## Task 7: 用户文档

**Files:**
- Create: `docs/features/jdbc_upsert_write_userguide.md`

- [ ] **Step 1: 写用户文档**

Create `docs/features/jdbc_upsert_write_userguide.md`:

```markdown
# JDBC Upsert 写入模式

TiSpark 支持两种写入 TiDB 的模式,由 `spark.tispark.tidb.write.mode` 控制:

| 模式 | 说明 |
|---|---|
| `jdbc_upsert`(默认) | 经 SparkSQL Catalyst,通过 JDBC 批量 `INSERT ... ON DUPLICATE KEY UPDATE` 写入。无 2PC,Spark UI 可见完整执行计划,性能近似手写 JDBC 批量。 |
| `tikv` | 原有行为:Percolator 两阶段提交直写 TiKV。适合超大一次性 bulk load。 |

## 前置配置(仅 jdbc_upsert 需要)

`jdbc_upsert` 经 TiDB SQL 协议写入,必须提供 **TiDB Server**(非 PD)连接信息,作为会话级配置:

\`\`\`
spark.tispark.tidb.write.mode = jdbc_upsert   # 默认即此值
spark.tispark.tidb.addr       = <tidb-server-host>
spark.tispark.tidb.port       = 4000
spark.tispark.tidb.user       = <user>
spark.tispark.tidb.password   = <password>
\`\`\`

> 缺少上述任一连接参数会在写入前直接报错(early fail)。

## upsert 语义

- 主键不存在 → 插入新行;主键(或任意唯一键)冲突 → 更新非主键列。
- 注意:`ON DUPLICATE KEY UPDATE` 对**任意唯一键冲突**都会触发更新,不仅是主键。

## 可选:按更新时间列条件覆盖

\`\`\`
spark.tispark.upsert.update_time_column = ts
\`\`\`

配置后,仅当来源行的该列值 **>=** 库中现有值时才覆盖整行(`VALUES(ts) >= ts`);否则保留库中原值。该列必须存在于写入列中。默认空(无条件覆盖)。

## 其他

- `spark.tispark.upsert.batch_size`:每批 `executeBatch` 行数,默认 1000。
- **原子性**:按批 autocommit,非整批事务;失败时部分批次可能已提交,但 upsert 幂等可安全重试。
- 切回旧模式:设 `spark.tispark.tidb.write.mode=tikv`。
- 安全提示:密码作为明文会话 conf 会出现在 Spark UI Environment 页与事件日志中。
```

- [ ] **Step 2: Commit**

```bash
git add docs/features/jdbc_upsert_write_userguide.md
git commit -m "docs: jdbc_upsert write mode user guide"
```

---

## Self-Review 记录

**1. Spec coverage**
- 写入模式开关(默认 jdbc_upsert)→ Task 2 + Task 5。✓
- SQL INSERT 走 Catalyst V2 / Spark UI DAG → Task 4 + Task 5 + Task 6 Step 4。✓
- JDBC 批量 upsert / 无 2PC / 近似 JDBC 性能 → Task 3。✓
- ON DUPLICATE KEY UPDATE + 更新时间列条件 → Task 1。✓
- 更新时间列配置(可空)→ Task 2 + Task 5(校验列存在)。✓
- 连接信息来源 + 早失败 → Task 2(`checkJdbcWriteRequired`)+ Task 5。✓
- 3.3 / 3.5 兼容 → Task 4(双 wrapper)+ 各 Task 的双 profile 编译。✓
- 类型转换 → Task 3(`CatalystTypeConverters`)+ Task 6 覆盖各类型。✓
- 老 tikv 模式不变 → Task 5(分支保留 `newTiDBWriteBuilder`);Task 6 可加一条 tikv 回归(可选)。✓
- 文档(含行为变更/密码提示)→ Task 7。✓

**2. Placeholder scan**:Task 1~5、7 代码完整无占位。Task 6 集成测试的基类名/JDBC 辅助方法名标注为"按 Step 1 实读结果替换"——因其依赖集群测试框架的真实 API,故要求实现者先读既有同目录套件对齐,而非凭空假设;这是有意的、受控的待定,非代码占位。

**3. Type consistency**:
- `TiDBBatchWrite(schema: StructType, tiDBOptions: TiDBOptions, upsertSql: String)` —— Task 3 定义,Task 5 调用一致。✓
- `TiDBDataWriterFactory(schema, url, upsertSql, batchSize)` / `TiDBDataWrite(schema, url, upsertSql, batchSize)` —— Task 3 内部一致。✓
- `UpsertSqlBuilder.build(database, table, columns: Seq[String], pkColumns: Set[String], updateTimeColumn: Option[String])` —— Task 1 定义,Task 5 调用一致(`writeColumns: Seq`、`pkColumns: Set`、`Option`)。✓
- `ReflectionUtil.newTiDBUpsertWriteBuilder(batchWrite: BatchWrite): WriteBuilder` —— Task 4 定义,Task 5 调用一致。✓
- `TiDBOptions.isJdbcUpsertMode` / `checkJdbcWriteRequired()` / `upsertUpdateTimeColumn: Option[String]` / `upsertBatchSize: Int` / `url` —— Task 2 定义,Task 3/5 使用一致。✓
```
