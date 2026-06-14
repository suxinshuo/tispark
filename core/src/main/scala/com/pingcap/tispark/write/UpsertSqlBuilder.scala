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
   *                          `VALUES(col) >= col` (incoming row is newer-or-equal).
   *                          The caller must ensure this name appears in `columns`;
   *                          this builder does not validate it (see TiDBTable.newWriteBuilder).
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
