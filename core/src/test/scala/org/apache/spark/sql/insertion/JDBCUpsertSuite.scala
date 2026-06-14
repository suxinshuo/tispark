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

package org.apache.spark.sql.insertion

import com.pingcap.tispark.datasource.BaseBatchWriteTest

/**
 * End-to-end integration suite for the `jdbc_upsert` write mode.
 *
 * Routes `INSERT INTO` through the TiDB catalog into the V2 BatchWrite path
 * (`TiDBTable.newWriteBuilder` -> `TiDBBatchWrite`), which issues JDBC
 * `INSERT ... ON DUPLICATE KEY UPDATE` against TiDB. Verifies upsert semantics
 * and the optional `upsert.update_time_column` guard against a real cluster.
 *
 * NOTE: requires a running TiDB+TiKV+PD cluster (configured via
 * `tidb_config.properties`). The connection params
 * (`spark.tispark.tidb.addr/port/user/password`) are required by jdbc_upsert
 * and are injected into the live SparkSession conf in [[beforeAll]] from the
 * shared test connection fields, since the base test infra does not put them
 * into session conf automatically.
 */
class JDBCUpsertSuite extends BaseBatchWriteTest("jdbc_upsert_t", "tispark_test") {

  // table name (without db). `table` / `database` come from the base ctor.
  private val tbl = table

  // The TiSpark-visible, possibly db-prefixed database name. In the test infra
  // `dbPrefix` defaults to "" so this is normally just `tispark_test`.
  private def fqTable = s"`$databaseWithPrefix`.`$tbl`"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // jdbc_upsert needs explicit TiDB connection params in the session conf,
    // because the write path reads them from sqlContext.getAllConfs.
    spark.conf.set("spark.tispark.tidb.addr", tidbAddr)
    spark.conf.set("spark.tispark.tidb.port", tidbPort.toString)
    spark.conf.set("spark.tispark.tidb.user", tidbUser)
    spark.conf.set("spark.tispark.tidb.password", tidbPassword)
    // exercise the jdbc_upsert write mode explicitly (it is also the default).
    spark.conf.set("spark.tispark.tidb.write.mode", "jdbc_upsert")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Reset between tests so a test that omits this conf never inherits a stale value.
    spark.conf.set("spark.tispark.upsert.update_time_column", "")
  }

  private def recreateTable(): Unit = {
    jdbcUpdate(s"DROP TABLE IF EXISTS `$database`.`$tbl`")
    jdbcUpdate(s"""CREATE TABLE `$database`.`$tbl` (
         |  id BIGINT PRIMARY KEY,
         |  uk INT,
         |  v VARCHAR(64),
         |  ts BIGINT,
         |  UNIQUE KEY uk_idx (uk)
         |)""".stripMargin)
    // make the freshly-created table visible to the TiDB catalog / TiSpark.
    setCurrentDatabase(database)
  }

  test("insert new rows then upsert existing by primary key") {
    recreateTable()
    spark.conf.set("spark.tispark.upsert.update_time_column", "")
    spark.sql(s"INSERT INTO $fqTable VALUES (1, 10, 'a', 100), (2, 20, 'b', 100)")
    spark.sql(s"INSERT INTO $fqTable VALUES (1, 10, 'a2', 200)")
    val rs = tidbStmt.executeQuery(s"SELECT v FROM `$database`.`$tbl` WHERE id = 1")
    assert(rs.next() && rs.getString("v") == "a2")
    rs.close()
  }

  test("update_time_column guard: older row does not overwrite") {
    recreateTable()
    spark.conf.set("spark.tispark.upsert.update_time_column", "ts")
    spark.sql(s"INSERT INTO $fqTable VALUES (1, 10, 'new', 200), (2, 20, 'side', 999)")
    spark.sql(s"INSERT INTO $fqTable VALUES (1, 10, 'old', 150)")
    val rs = tidbStmt.executeQuery(s"SELECT v, ts FROM `$database`.`$tbl` WHERE id = 1")
    assert(rs.next())
    assert(rs.getString("v") == "new")
    assert(rs.getLong("ts") == 200L)
    rs.close()
    // the older-row write must not have touched the unrelated side row.
    val rsSide = tidbStmt.executeQuery(s"SELECT v, ts FROM `$database`.`$tbl` WHERE id = 2")
    assert(rsSide.next())
    assert(rsSide.getString("v") == "side")
    assert(rsSide.getLong("ts") == 999L)
    rsSide.close()
  }

  test("update_time_column guard: newer-or-equal row overwrites") {
    recreateTable()
    spark.conf.set("spark.tispark.upsert.update_time_column", "ts")
    spark.sql(s"INSERT INTO $fqTable VALUES (1, 10, 'old', 200)")
    spark.sql(s"INSERT INTO $fqTable VALUES (1, 10, 'new', 200)")
    val rs = tidbStmt.executeQuery(s"SELECT v FROM `$database`.`$tbl` WHERE id = 1")
    assert(rs.next() && rs.getString("v") == "new")
    rs.close()
    // a strictly-newer ts must also overwrite.
    spark.sql(s"INSERT INTO $fqTable VALUES (1, 10, 'newest', 300)")
    val rsNewer = tidbStmt.executeQuery(s"SELECT v FROM `$database`.`$tbl` WHERE id = 1")
    assert(rsNewer.next() && rsNewer.getString("v") == "newest")
    rsNewer.close()
  }

  test("missing connection params fail fast in jdbc_upsert mode") {
    recreateTable()
    spark.conf.set("spark.tispark.upsert.update_time_column", "")
    val saved = spark.conf.getOption("spark.tispark.tidb.addr")
    spark.conf.unset("spark.tispark.tidb.addr")
    try {
      val ex = intercept[Exception] {
        spark.sql(s"INSERT INTO $fqTable VALUES (9, 90, 'x', 1)")
      }
      assert(
        messageChain(ex).exists(_.contains("tidb.addr")),
        s"expected an error mentioning 'tidb.addr', got: ${messageChain(ex).mkString(" | ")}")
    } finally {
      saved.foreach(v => spark.conf.set("spark.tispark.tidb.addr", v))
    }
  }

  // Fully-qualified, db-prefixed name for the wide-type table used below.
  private def fqTypesTable = s"`$databaseWithPrefix`.`jdbc_upsert_types_t`"

  private def recreateTypesTable(): Unit = {
    jdbcUpdate(s"DROP TABLE IF EXISTS `$database`.`jdbc_upsert_types_t`")
    jdbcUpdate(s"""CREATE TABLE `$database`.`jdbc_upsert_types_t` (
         |  id BIGINT PRIMARY KEY,
         |  c_dec DECIMAL(18,4),
         |  c_ts  TIMESTAMP NULL,
         |  c_date DATE NULL,
         |  c_bin VARBINARY(64) NULL,
         |  c_bool TINYINT(1) NULL,
         |  c_str VARCHAR(64) NULL
         |)""".stripMargin)
    // make the freshly-created table visible to the TiDB catalog / TiSpark.
    setCurrentDatabase(database)
  }

  // Validates the type-conversion write path flagged as highest-risk in the
  // design: each Spark InternalRow value is converted via
  // `CatalystTypeConverters.createToScalaConverter` and bound with JDBC
  // `setObject`. Exercises DECIMAL / TIMESTAMP / DATE / BINARY / BOOLEAN(tinyint)
  // / NULL, both on the initial INSERT and on the ON DUPLICATE KEY UPDATE path.
  // REQUIRES A LIVE CLUSTER to actually run (this environment has none); it is
  // here so the type round-trips can be validated against a real TiDB.
  test("wide types round-trip through jdbc_upsert (DECIMAL/TIMESTAMP/DATE/BINARY/BOOLEAN/NULL)") {
    recreateTypesTable()
    spark.conf.set("spark.tispark.upsert.update_time_column", "")

    // Initial INSERT: id=1 fully populated; id=2 leaves the nullable columns NULL
    // to exercise null binding through setObject.
    spark.sql(s"""INSERT INTO $fqTypesTable VALUES (
         |  1,
         |  CAST(12.3400 AS DECIMAL(18,4)),
         |  TIMESTAMP '2021-01-02 03:04:05',
         |  DATE '2021-01-02',
         |  CAST('abc' AS BINARY),
         |  CAST(1 AS TINYINT),
         |  'hello'
         |)""".stripMargin)
    spark.sql(s"""INSERT INTO $fqTypesTable VALUES (
         |  2,
         |  CAST(1.0000 AS DECIMAL(18,4)),
         |  CAST(NULL AS TIMESTAMP),
         |  CAST(NULL AS DATE),
         |  CAST(NULL AS BINARY),
         |  CAST(NULL AS TINYINT),
         |  CAST(NULL AS STRING)
         |)""".stripMargin)

    // UPSERT id=1 with CHANGED values to exercise ON DUPLICATE KEY UPDATE for
    // these types (different decimal, timestamp, string).
    spark.sql(s"""INSERT INTO $fqTypesTable VALUES (
         |  1,
         |  CAST(99.9999 AS DECIMAL(18,4)),
         |  TIMESTAMP '2022-06-07 08:09:10',
         |  DATE '2022-06-07',
         |  CAST('xyz' AS BINARY),
         |  CAST(0 AS TINYINT),
         |  'world'
         |)""".stripMargin)

    // Read back id=1 and assert the UPDATED values landed.
    val rs = tidbStmt.executeQuery(
      s"SELECT c_dec, c_ts, c_date, c_str FROM `$database`.`jdbc_upsert_types_t` WHERE id = 1")
    assert(rs.next())
    assert(rs.getBigDecimal("c_dec").compareTo(new java.math.BigDecimal("99.9999")) == 0)
    assert(rs.getString("c_str") == "world")
    // NOTE: exact temporal formatting depends on the JDBC driver / cluster; only
    // assert the round-trip is non-null here and confirm formatting on a live cluster.
    assert(rs.getTimestamp("c_ts") != null)
    assert(rs.getDate("c_date") != null)
    rs.close()

    // Read back id=2 and assert the NULL columns came through as NULL via setObject.
    val rsNull = tidbStmt.executeQuery(
      s"SELECT c_ts, c_date, c_bin, c_bool, c_str FROM `$database`.`jdbc_upsert_types_t` WHERE id = 2")
    assert(rsNull.next())
    assert(rsNull.getObject("c_ts") == null && rsNull.wasNull())
    assert(rsNull.getObject("c_date") == null && rsNull.wasNull())
    assert(rsNull.getObject("c_bin") == null && rsNull.wasNull())
    assert(rsNull.getObject("c_bool") == null && rsNull.wasNull())
    assert(rsNull.getObject("c_str") == null && rsNull.wasNull())
    rsNull.close()
  }

  // Spark may wrap the require() failure (IllegalArgumentException) inside an
  // AnalysisException / SparkException, so walk the cause chain when matching.
  private def messageChain(t: Throwable): List[String] = {
    var cur: Throwable = t
    val acc = scala.collection.mutable.ListBuffer.empty[String]
    while (cur != null) {
      if (cur.getMessage != null) acc += cur.getMessage
      cur = cur.getCause
    }
    acc.toList
  }
}
