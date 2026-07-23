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

import com.pingcap.tispark.v2.sink.{TiDBDataWrite, TiDBDataWriterFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TiDBOptionsUpsertSuite extends FunSuite with BeforeAndAfterAll {

  // TiDBOptions ctor calls SparkContext.getOrCreate(); provide a local session.
  // extensions != TiExtensions so mergeWithSparkConf returns params untouched.
  override def beforeAll(): Unit = {
    super.beforeAll()
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

  test("non-positive batch size is rejected") {
    assertThrows[IllegalArgumentException](opt("upsert.batch_size" -> "0"))
    assertThrows[IllegalArgumentException](opt("upsert.batch_size" -> "-5"))
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

  test("sink case-class toString does not leak the JDBC password") {
    val secret = "sup3rSecretPw"
    val o = opt(
      "tidb.addr" -> "127.0.0.1",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "tidb.password" -> secret)
    // sanity: the password really is embedded in the JDBC URL we are guarding.
    assert(o.url.contains(secret))

    val schema = StructType(Seq(StructField("id", IntegerType)))
    val sql = "INSERT INTO `d`.`t` (`id`) VALUES (?) ON DUPLICATE KEY UPDATE `id` = `id`"

    val factory = TiDBDataWriterFactory(schema, o, sql)
    assert(!factory.toString.contains(secret))

    // Constructing the writer does not open a connection (ensureOpen is lazy).
    val writer = TiDBDataWrite(schema, o, sql)
    assert(!writer.toString.contains(secret))
  }
}
