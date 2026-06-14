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
    assert(
      sql ==
        "INSERT INTO `d`.`t` (`k1`, `k2`) VALUES (?, ?) " +
          "ON DUPLICATE KEY UPDATE `k1` = `k1`")
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
