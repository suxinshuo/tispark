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
    TiDBDataWriterFactory(schema, tiDBOptions, upsertSql)

  override def commit(messages: Array[WriterCommitMessage]): Unit =
    logger.info(s"TiDB jdbc_upsert committed across ${messages.length} partitions")

  override def abort(messages: Array[WriterCommitMessage]): Unit =
    logger.warn(
      "TiDB jdbc_upsert aborted; per-batch autocommit means some rows may already " +
        "be persisted (no global rollback). Upserts are idempotent and safe to retry.")
}
