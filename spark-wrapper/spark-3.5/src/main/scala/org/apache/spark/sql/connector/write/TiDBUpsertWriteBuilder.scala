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
