// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.writer

import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.listener.DorisTransactionListener
import org.apache.doris.spark.load.{CachedDorisStreamLoadClient, DorisStreamLoad}
import org.apache.doris.spark.sql.Utils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.time.Duration
import java.util.Objects
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success}

class DorisWriter(settings: SparkSettings) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisWriter])

  val batchSize: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_SIZE,
    ConfigurationOptions.SINK_BATCH_SIZE_DEFAULT)
  private val maxRetryTimes: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_MAX_RETRIES,
    ConfigurationOptions.SINK_MAX_RETRIES_DEFAULT)
  private val sinkTaskPartitionSize: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TASK_PARTITION_SIZE)
  private val sinkTaskUseRepartition: Boolean = settings.getProperty(ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION,
    ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION_DEFAULT.toString).toBoolean
  private val batchInterValMs: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS,
    ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS_DEFAULT)

  private val enable2PC: Boolean = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_ENABLE_2PC,
    ConfigurationOptions.DORIS_SINK_ENABLE_2PC_DEFAULT);

  private val dorisStreamLoader: DorisStreamLoad = CachedDorisStreamLoadClient.getOrCreate(settings)

  def write(dataFrame: DataFrame): Unit = {

    val sc = dataFrame.sqlContext.sparkContext
    val preCommittedTxnAcc = sc.collectionAccumulator[Int]("preCommittedTxnAcc")
    if (enable2PC) {
      sc.addSparkListener(new DorisTransactionListener(preCommittedTxnAcc, dorisStreamLoader))
    }

    var resultRdd = dataFrame.rdd
    val dfColumns = dataFrame.columns
    if (Objects.nonNull(sinkTaskPartitionSize)) {
      resultRdd = if (sinkTaskUseRepartition) resultRdd.repartition(sinkTaskPartitionSize) else resultRdd.coalesce(sinkTaskPartitionSize)
    }
    resultRdd
      .foreachPartition(partition => {
        partition
          .grouped(batchSize)
          .foreach(batch => flush(batch, dfColumns))
      })

    /**
     * flush data to Doris and do retry when flush error
     *
     */
    def flush(batch: Seq[Row], dfColumns: Array[String]): Unit = {
      Utils.retry[Integer, Exception](maxRetryTimes, Duration.ofMillis(batchInterValMs.toLong), logger) {
        dorisStreamLoader.loadV2(batch.asJava, dfColumns, enable2PC)
      } match {
        case Success(txnIds) => if (enable2PC) handleLoadSuccess(txnIds.asScala, preCommittedTxnAcc)
        case Failure(e) =>
          if (enable2PC) handleLoadFailure(preCommittedTxnAcc)
          throw new IOException(
            s"Failed to load batch data on BE: ${dorisStreamLoader.getLoadUrlStr} node and exceeded the max ${maxRetryTimes} retry times.", e)
      }
    }

  }

  def writeStream(dataFrame: DataFrame): Unit = {

    val sc = dataFrame.sqlContext.sparkContext
    val preCommittedTxnAcc = sc.collectionAccumulator[Int]("preCommittedTxnAcc")
    if (enable2PC) {
      sc.addSparkListener(new DorisTransactionListener(preCommittedTxnAcc, dorisStreamLoader))
    }

    var resultRdd = dataFrame.queryExecution.toRdd
    val schema = dataFrame.schema
    val dfColumns = dataFrame.columns
    if (Objects.nonNull(sinkTaskPartitionSize)) {
      resultRdd = if (sinkTaskUseRepartition) resultRdd.repartition(sinkTaskPartitionSize) else resultRdd.coalesce(sinkTaskPartitionSize)
    }
    resultRdd
      .foreachPartition(partition => {
        partition
          .grouped(batchSize)
          .foreach(batch =>
            flush(batch, dfColumns))
      })

    /**
     * flush data to Doris and do retry when flush error
     *
     */
    def flush(batch: Seq[InternalRow], dfColumns: Array[String]): Unit = {
      Utils.retry[util.List[Integer], Exception](maxRetryTimes, Duration.ofMillis(batchInterValMs.toLong), logger) {
        dorisStreamLoader.loadStream(convertToObjectList(batch, schema), dfColumns, enable2PC)
      } match {
        case Success(txnIds) => if (enable2PC) handleLoadSuccess(txnIds.asScala, preCommittedTxnAcc)
        case Failure(e) =>
          if (enable2PC) handleLoadFailure(preCommittedTxnAcc)
          throw new IOException(
            s"Failed to load batch data on BE: ${dorisStreamLoader.getLoadUrlStr} node and exceeded the max ${maxRetryTimes} retry times.", e)
      }
    }

    def convertToObjectList(rows: Seq[InternalRow], schema: StructType): util.List[util.List[Object]] = {
      rows.map(row => {
        row.toSeq(schema).map(_.asInstanceOf[AnyRef]).toList.asJava
      }).asJava
    }

  }

  private def handleLoadSuccess(txnIds: mutable.Buffer[Integer], acc: CollectionAccumulator[Int]): Unit = {
    txnIds.foreach(txnId => acc.add(txnId))
  }

  def handleLoadFailure(acc: CollectionAccumulator[Int]): Unit = {
    // if task run failed, acc value will not be returned to driver,
    // should abort all pre committed transactions inside the task
    logger.info("load task failed, start aborting previously pre-committed transactions")
    if (acc.isZero) {
      logger.info("no pre-committed transactions, skip abort")
      return
    }
    val abortFailedTxnIds = mutable.Buffer[Int]()
    acc.value.asScala.foreach(txnId => {
      Utils.retry[Unit, Exception](3, Duration.ofSeconds(1), logger) {
        dorisStreamLoader.abort(txnId)
      } match {
        case Success(_) =>
        case Failure(_) => abortFailedTxnIds += txnId
      }
    })
    if (abortFailedTxnIds.nonEmpty) logger.warn("not aborted txn ids: {}", abortFailedTxnIds.mkString(","))
    acc.reset()
  }


}
