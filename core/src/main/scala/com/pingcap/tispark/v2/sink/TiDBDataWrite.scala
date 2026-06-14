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
import org.slf4j.LoggerFactory

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

  private final val logger = LoggerFactory.getLogger(getClass.getName)

  private val dataTypes = schema.fields.map(_.dataType)
  private val converters =
    schema.fields.map(f => CatalystTypeConverters.createToScalaConverter(f.dataType))

  private var conn: Connection = _
  private var stmt: PreparedStatement = _
  private var rowsInBatch: Int = 0

  private def ensureOpen(): Unit = {
    if (stmt == null) {
      val c = TiDBUtils.createConnectionFactory(url)()
      c.setAutoCommit(true)
      stmt = c.prepareStatement(upsertSql)
      conn = c
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
      catch { case t: Throwable => logger.warn("Failed to close PreparedStatement", t) }
      stmt = null
    }
    if (conn != null) {
      try conn.close()
      catch { case t: Throwable => logger.warn("Failed to close JDBC connection", t) }
      conn = null
    }
  }
}

object WriteSucceeded extends WriterCommitMessage
