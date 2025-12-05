/*
 * Copyright 2019 PingCAP, Inc.
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

import com.pingcap.tikv._
import com.pingcap.tikv.meta.TiTableInfo
import com.pingcap.tikv.partition.{PartitionedTable, TableCommon}
import com.pingcap.tikv.util.ConvertUpstreamUtils
import com.pingcap.tispark.TiDBUtils
import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.utils.{JdbcUtil, ResourceUtil, TiUtil, TwoPhaseCommitHepler, WriteUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.slf4j.LoggerFactory
import org.tikv.common.exception.TiBatchWriteException
import org.tikv.txn.TTLManager

import scala.collection.JavaConverters._

object TiBatchWrite {
  type SparkRow = org.apache.spark.sql.Row
  type TiRow = com.pingcap.tikv.row.Row
  type TiDataType = com.pingcap.tikv.types.DataType
  // Milliseconds
  private val MIN_DELAY_CLEAN_TABLE_LOCK = 60000
  private val DELAY_CLEAN_TABLE_LOCK_AND_COMMIT_BACKOFF_DELTA = 30000
  private val PRIMARY_KEY_COMMIT_BACKOFF =
    MIN_DELAY_CLEAN_TABLE_LOCK - DELAY_CLEAN_TABLE_LOCK_AND_COMMIT_BACKOFF_DELTA

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def write(df: DataFrame, tiContext: TiContext, options: TiDBOptions): Unit = {
    val dataToWrite = Map(DBTable(options.database, options.table) -> df)
    new TiBatchWrite(dataToWrite, tiContext, options).write()
  }

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def write(
      dataToWrite: Map[DBTable, DataFrame],
      sparkSession: SparkSession,
      parameters: Map[String, String]): Unit = {
    TiExtensions.getTiContext(sparkSession) match {
      case Some(tiContext) =>
        val tiDBOptions = new TiDBOptions(
          parameters ++ Map(TiDBOptions.TIDB_MULTI_TABLES -> "true"))
        tiDBOptions.checkWriteRequired()
        new TiBatchWrite(dataToWrite, tiContext, tiDBOptions).write()
      case None =>
        throw new TiBatchWriteException("TiExtensions is disable!")
    }
  }
}

class TiBatchWrite(
    @transient val dataToWrite: Map[DBTable, DataFrame],
    @transient val tiContext: TiContext,
    options: TiDBOptions)
    extends Serializable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  import com.pingcap.tispark.write.TiBatchWrite._

  private var tiConf: TiConfiguration = _
  @transient private var clientSession: ClientSession = _
  private var useTableLock: Boolean = _
  @transient private var ttlManager: TTLManager = _
  private var isTTLUpdate: Boolean = _
  private var lockTTLSeconds: Long = _
  @transient private var tiBatchWriteTables: List[TiBatchWriteTable] = _
  @transient private var startMS: Long = _
  private var startTs: Long = _
  private var twoPhaseCommitHepler: TwoPhaseCommitHepler = _
  private val tiAuthorization: Option[TiAuthorization] = tiContext.tiAuthorization

  private def write(): Unit = {
    if (tiContext.writeUpsertEnable) {
      logger.info("Using upsert write mode")
      logger.warn("Upsert write mode does not support transaction")
      upsertWrite()
    } else {
      logger.info("Using two phase commit write mode")
      try {
        doWrite()
      } finally {
        close()
      }
    }
  }

  /**
   * Direct upsert write, no transaction
   */
  private def upsertWrite(): Unit = {
    val jdbcUrl = s"jdbc:mysql://${options.address}:${options.port}?rewriteBatchedStatements=true"
    val jdbcUser = options.user
    val jdbcPassword = options.password

    dataToWrite.foreach(dtw => {
      val dbTable = s"${dtw._1.database}.${dtw._1.table}"
      val df = dtw._2

      // 从 sparkConf 获取 upsert 配置
      val upsertWritePartitionNum = tiContext.writeUpsertPartitionNum
      val upsertWriteBatchSize = tiContext.writeUpsertBatchSize

      val columns = df.columns.mkString(",")
      val updateColumns = df.columns.map(colName => s"$colName=values($colName)").mkString(",")
      val placeholder = (1 to df.columns.length).map(_ => "?").mkString(",")
      val upsertSql = s"INSERT INTO $dbTable ($columns) VALUES ($placeholder) ON DUPLICATE KEY UPDATE $updateColumns"
      logger.info(s"upsertSql: $upsertSql")

      // 如果当前 partition 数量 > upsertWritePartitionNum * 2, 则 coalescePartition, 否则 repartition
      val repartitionDf = df.rdd.partitions.length match {
        case x if x > upsertWritePartitionNum * 2 => df.coalesce(upsertWritePartitionNum)
        case _ => df.repartition(upsertWritePartitionNum)
       }
      repartitionDf.foreachPartition((rowIter: Iterator[Row]) => {
        if (rowIter.nonEmpty) {
          logger.info(s"开始处理分区数据 | JDBC URL: $jdbcUrl | 用户: $jdbcUser")
          ResourceUtil.using(JdbcUtil.getConn(jdbcUrl, jdbcUser, jdbcPassword)) { conn => {
            conn.setAutoCommit(false)
            ResourceUtil.using(conn.prepareStatement(upsertSql)) { pstmt => {
              var rowCount = 0
              while (rowIter.hasNext) {
                val row = rowIter.next()
                for (i <- 0 until row.length) {
                  val paramIndex = i + 1
                  val value = row.get(i)
                  Option(value) match {
                    case Some(v) =>
                      v match {
                        // 字符串类型
                        case s: String => pstmt.setString(paramIndex, s)

                        // 整数类型
                        case i: Integer => pstmt.setInt(paramIndex, i)
                        case l: java.lang.Long => pstmt.setLong(paramIndex, l)
                        case s: java.lang.Short => pstmt.setShort(paramIndex, s)
                        case b: java.lang.Byte => pstmt.setByte(paramIndex, b)

                        // 浮点数类型
                        case d: java.lang.Double => pstmt.setDouble(paramIndex, d)
                        case f: java.lang.Float => pstmt.setFloat(paramIndex, f)

                        // 布尔类型
                        case b: java.lang.Boolean => pstmt.setBoolean(paramIndex, b)

                        // 日期时间类型
                        case date: java.sql.Date => pstmt.setDate(paramIndex, date)
                        case time: java.sql.Time => pstmt.setTime(paramIndex, time)
                        case timestamp: java.sql.Timestamp => pstmt.setTimestamp(paramIndex, timestamp)

                        // Java 8 日期时间类型
                        case instant: java.time.Instant => pstmt.setTimestamp(paramIndex, java.sql.Timestamp.from(instant))
                        case localDate: java.time.LocalDate => pstmt.setDate(paramIndex, java.sql.Date.valueOf(localDate))
                        case localTime: java.time.LocalTime => pstmt.setTime(paramIndex, java.sql.Time.valueOf(localTime))
                        case localDateTime: java.time.LocalDateTime => pstmt.setTimestamp(paramIndex, java.sql.Timestamp.valueOf(localDateTime))

                        // 大数值类型
                        case bigDecimal: java.math.BigDecimal => pstmt.setBigDecimal(paramIndex, bigDecimal)
                        case bigInteger: java.math.BigInteger => pstmt.setBigDecimal(paramIndex, new java.math.BigDecimal(bigInteger))

                        // 字节数组
                        case bytes: Array[Byte] => pstmt.setBytes(paramIndex, bytes)

                        // 默认情况使用setObject
                        case _ => pstmt.setObject(paramIndex, v)
                      }
                    case None =>
                      pstmt.setObject(paramIndex, null)
                  }
                }
                pstmt.addBatch()
                rowCount += 1

                if (rowCount >= upsertWriteBatchSize) {
                  pstmt.executeBatch()
                  pstmt.clearBatch()
                  rowCount = 0
                }
              }
              if (rowCount > 0) {
                pstmt.executeBatch()
                pstmt.clearBatch()
                rowCount = 0
              }
              conn.commit()
            }
            }
          }
          }
        } else {
          logger.info("当前分区无数据, 跳过JDBC连接创建")
        }
      })
    })
  }

  private def close(): Unit = {
    try {
      if (tiBatchWriteTables != null) {
        tiBatchWriteTables.foreach(_.unpersistAll())
      }
    } catch {
      case _: Throwable =>
    }

    try {
      twoPhaseCommitHepler.close()
    } catch {
      case _: Throwable =>
    }
  }

  private def doWrite(): Unit = {
    startMS = System.currentTimeMillis()

    // check if write enable
    if (!tiContext.tiConf.isWriteEnable) {
      throw new TiBatchWriteException(
        "tispark batch write is disabled! set spark.tispark.write.enable to enable.")
    }

    // initialize
    tiConf = mergeSparkConfWithDataSourceConf(tiContext.conf, options)
    clientSession = tiContext.clientSession
    val tikvSupportUpdateTTL =
      ConvertUpstreamUtils.isTiKVVersionGreatEqualThanVersion(
        clientSession.getTiKVSession.getPDClient,
        "3.0.5")
    val isTiDBV4 = ConvertUpstreamUtils.isTiKVVersionGreatEqualThanVersion(
      clientSession.getTiKVSession.getPDClient,
      "4.0.0")
    isTTLUpdate = options.isTTLUpdate(tikvSupportUpdateTTL)
    lockTTLSeconds = options.getLockTTLSeconds(tikvSupportUpdateTTL)

    // init tiBatchWriteTables
    tiBatchWriteTables = {
      dataToWrite.flatMap {
        case (dbTable, df) =>
          val tableOptions = options.setDBTable(dbTable)
          val tiTableRef = tableOptions.getTiTableRef(tiConf)
          val tiTableInfo = clientSession.getCatalog.getTable(
            tableOptions.getTiTableRef(tiConf).databaseName,
            tableOptions.getTiTableRef(tiConf).tableName)

          if (tiTableInfo == null) {
            throw new NoSuchTableException(tiTableRef.databaseName, tiTableRef.tableName)
          }

          val table = new TableCommon(tiTableInfo.getId, tiTableInfo.getId, tiTableInfo)

          /**
           * Since `TiBatchWriteTable` is associated with a physical table,
           * - if the table is partitioned, we need to transfer the logical table to the physical table
           * and then group the rows by physical table to generate TiBatchWriteTable.
           * - if the table is not partitioned, the logical table is the same as the physical table.
           */
          if (tiTableInfo.isPartitionEnabled) {
            transferToPhysicalTables(df, tiTableInfo, table, isTiDBV4, tableOptions)
          } else {
            List(new TiBatchWriteTable(df, tiContext, tableOptions, tiConf, isTiDBV4, table))
          }
      }.toList
    }

    // check unsupported
    tiBatchWriteTables.foreach(_.checkUnsupported())

    // check authorization
    if (TiAuthorization.enableAuth) {
      tiBatchWriteTables.foreach(_.checkAuthorization(tiAuthorization, options))
    }

    // cache data
    tiBatchWriteTables.foreach(_.persist())

    // check empty
    tiBatchWriteTables = tiBatchWriteTables.filter { table =>
      !table.isDFEmpty
    }
    if (tiBatchWriteTables.isEmpty) {
      logger.warn("data is empty!")
      return
    }

    // check schema
    tiBatchWriteTables.foreach(_.checkColumnNumbers())

    // get timestamp as start_ts
    val startTimeStamp = clientSession.getTiKVSession.getTimestamp
    startTs = startTimeStamp.getVersion
    logger.info(s"startTS: $startTs")
    tiContext.serviceSafePoint.updateStartTs(startTimeStamp)

    // pre calculate
    val shuffledRDD: RDD[(SerializableKey, Array[Byte])] = {
      val rddList = tiBatchWriteTables.map(_.preCalculate(startTimeStamp))
      if (rddList.lengthCompare(1) == 0) {
        rddList.head
      } else {
        tiContext.sparkSession.sparkContext.union(rddList)
      }
    }

    // take one row as primary key
    val (primaryKey: SerializableKey, primaryRow: Array[Byte]) = {
      val takeOne = shuffledRDD.take(1)
      if (takeOne.length == 0) {
        logger.warn("there is no data in source rdd")
        return
      } else {
        takeOne(0)
      }
    }

    logger.info(s"primary key: $primaryKey")

    // split region
    val finalRDD = if (options.enableRegionSplit) {
      val insertRDD = shuffledRDD.filter(kv => kv._2.length > 0)
      val orderedSplitPoints = getRegionSplitPoints(insertRDD)

      try {
        clientSession.getTiKVSession.splitRegionAndScatter(
          orderedSplitPoints.map(_.bytes).asJava,
          options.splitRegionBackoffMS,
          options.scatterRegionBackoffMS,
          options.scatterWaitMS)
      } catch {
        case e: Throwable => logger.warn("split region and scatter error!", e)
      }

      // shuffle according to split points
      shuffledRDD.partitionBy(
        new TiReginSplitPartitioner(orderedSplitPoints, options.maxWriteTaskNumber))
    } else {
      shuffledRDD
    }

    // filter primary key
    val secondaryKeysRDD = finalRDD.filter { keyValue =>
      !keyValue._1.equals(primaryKey)
    }

    // for test
    if (options.sleepBeforePrewritePrimaryKey > 0) {
      logger.info(s"sleep ${options.sleepBeforePrewritePrimaryKey} ms for test")
      Thread.sleep(options.sleepBeforePrewritePrimaryKey)
    }

    twoPhaseCommitHepler = TwoPhaseCommitHepler(startTs, options)

    // driver primary pre-write
    twoPhaseCommitHepler.prewritePrimaryKeyByDriver(primaryKey, primaryRow)

    // for test
    if (options.sleepAfterPrewritePrimaryKey > 0) {
      logger.info(s"sleep ${options.sleepAfterPrewritePrimaryKey} ms for test")
      Thread.sleep(options.sleepAfterPrewritePrimaryKey)
    }

    // executors secondary pre-write
    twoPhaseCommitHepler.prewriteSecondaryKeyByExecutors(secondaryKeysRDD, primaryKey)

    // checkschema if not useTableLock
    val schemaUpdateTimes = if (useTableLock) {
      Nil
    } else {
      tiBatchWriteTables.map(_.buildSchemaUpdateTime())
    }
    // driver primary commit
    val commitTs =
      twoPhaseCommitHepler.commitPrimaryKeyWithRetryByDriver(primaryKey, schemaUpdateTimes)

    // stop ttl
    twoPhaseCommitHepler.stopPrimaryKeyTTLUpdate()

    // executors secondary commit
    twoPhaseCommitHepler.commitSecondaryKeyByExecutors(secondaryKeysRDD, commitTs)

    // update table statistics: modify_count & count
    if (options.enableUpdateTableStatistics) {
      val tiDBJDBCClient = new TiDBJDBCClient(TiDBUtils.createConnectionFactory(options.url)())
      tiBatchWriteTables.foreach(_.updateTableStatistics(startTs, tiDBJDBCClient))
      try {
        tiDBJDBCClient.close()
      } catch {
        case _: Throwable =>
      }
    }

    val endMS = System.currentTimeMillis()
    logger.info(s"batch write cost ${(endMS - startMS) / 1000} seconds")
  }

  private def transferToPhysicalTables(
      df: DataFrame,
      tiTableInfo: TiTableInfo,
      table: TableCommon,
      isTiDBV4: Boolean,
      options: TiDBOptions) = {
    val pTable = PartitionedTable.newPartitionTable(table, tiTableInfo)
    val colsInDf = df.columns.toList.map(_.toLowerCase())

    pTable.getPhysicalTables
      .map(table => {
        val dfPartitioned = df.filter(
          row =>
            table.equals(
              pTable.locatePartition(WriteUtil.sparkRow2TiKVRow(row, tiTableInfo, colsInDf))))
        new TiBatchWriteTable(dfPartitioned, tiContext, options, tiConf, isTiDBV4, table)
      })
      .toList
  }

  private def getRegionSplitPoints(
      rdd: RDD[(SerializableKey, Array[Byte])]): List[SerializableKey] = {
    val count = rdd.count()

    if (count < options.regionSplitThreshold) {
      return Nil
    }

    val regionSplitPointNum = if (options.regionSplitNum > 0) {
      options.regionSplitNum
    } else {
      Math.min(
        Math.max(
          options.minRegionSplitNum,
          Math.ceil(count.toDouble / options.regionSplitKeys).toInt),
        options.maxRegionSplitNum)
    }
    logger.info(s"regionSplitPointNum=$regionSplitPointNum")

    val sampleSize = (regionSplitPointNum + 1) * options.sampleSplitFrac
    logger.info(s"sampleSize=$sampleSize")

    val sampleData = rdd.sample(false, sampleSize.toDouble / count).collect()
    logger.info(s"sampleData size=${sampleData.length}")

    val splitPointNumUsingSize = if (options.regionSplitUsingSize) {
      val avgSize = getAverageSizeInBytes(sampleData)
      logger.info(s"avgSize=$avgSize Bytes")
      if (avgSize <= options.bytesPerRegion / options.regionSplitKeys) {
        regionSplitPointNum
      } else {
        Math.min(
          Math.floor((count.toDouble / options.bytesPerRegion) * avgSize).toInt,
          sampleData.length / 10)
      }
    } else {
      regionSplitPointNum
    }
    logger.info(s"splitPointNumUsingSize=$splitPointNumUsingSize")

    val finalRegionSplitPointNum = Math.min(
      Math.max(options.minRegionSplitNum, splitPointNumUsingSize),
      options.maxRegionSplitNum)
    logger.info(s"finalRegionSplitPointNum=$finalRegionSplitPointNum")

    val sortedSampleData = sampleData
      .map(_._1)
      .sorted(new Ordering[SerializableKey] {
        override def compare(x: SerializableKey, y: SerializableKey): Int = {
          x.compareTo(y)
        }
      })
    val orderedSplitPoints = new Array[SerializableKey](finalRegionSplitPointNum)
    val step = Math.floor(sortedSampleData.length.toDouble / (finalRegionSplitPointNum + 1)).toInt
    for (i <- 0 until finalRegionSplitPointNum) {
      orderedSplitPoints(i) = sortedSampleData((i + 1) * step)
    }

    logger.info(s"orderedSplitPoints size=${orderedSplitPoints.length}")
    orderedSplitPoints.toList
  }

  private def getAverageSizeInBytes(keyValues: Array[(SerializableKey, Array[Byte])]): Int = {
    var avg: Double = 0
    var t: Int = 1
    keyValues.foreach { keyValue =>
      val keySize: Double = keyValue._1.bytes.length + keyValue._2.length
      avg = avg + (keySize - avg) / t
      t = t + 1
    }
    Math.ceil(avg).toInt
  }

  private def mergeSparkConfWithDataSourceConf(
      conf: SparkConf,
      options: TiDBOptions): TiConfiguration = {
    val clonedConf = conf.clone()
    // priority: data source config > spark config
    clonedConf.setAll(options.parameters)
    TiUtil.sparkConfToTiConf(clonedConf, Option.empty)
  }
}
