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
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.time.Duration
import java.util
import java.util.Objects
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class DorisWriter(settings: SparkSettings) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisWriter])

  private val sinkTaskPartitionSize: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TASK_PARTITION_SIZE)
  private val sinkTaskUseRepartition: Boolean = settings.getProperty(ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION,
    ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION_DEFAULT.toString).toBoolean
  private val batchInterValMs: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS,
    ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS_DEFAULT)

  private val enable2PC: Boolean = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_ENABLE_2PC,
    ConfigurationOptions.DORIS_SINK_ENABLE_2PC_DEFAULT)
  private val sinkTxnIntervalMs: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TXN_INTERVAL_MS,
    ConfigurationOptions.DORIS_SINK_TXN_INTERVAL_MS_DEFAULT)
  private val sinkTxnRetries: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TXN_RETRIES,
    ConfigurationOptions.DORIS_SINK_TXN_RETRIES_DEFAULT)

  private val dorisStreamLoader: DorisStreamLoad = CachedDorisStreamLoadClient.getOrCreate(settings)

  /**
   * write data in batch mode
   *
   * @param dataFrame source dataframe
   */
  def write(dataFrame: DataFrame): Unit = {
    doWrite(dataFrame, dorisStreamLoader.load)
  }

  /**
   * write data in stream mode
   *
   * @param dataFrame source dataframe
   */
  def writeStream(dataFrame: DataFrame): Unit = {
    doWrite(dataFrame, dorisStreamLoader.loadStream)
  }

  private def doWrite(dataFrame: DataFrame, loadFunc: (util.Iterator[InternalRow], StructType) => Long): Unit = {

    val sc = dataFrame.sqlContext.sparkContext
    logger.info(s"applicationAttemptId: ${sc.applicationAttemptId.getOrElse(-1)}")
    val preCommittedTxnAcc = sc.collectionAccumulator[Long]("preCommittedTxnAcc")
    if (enable2PC) {
      sc.addSparkListener(new DorisTransactionListener(preCommittedTxnAcc, dorisStreamLoader, sinkTxnIntervalMs, sinkTxnRetries))
    }

    var resultRdd = dataFrame.queryExecution.toRdd
    val schema = dataFrame.schema
    if (Objects.nonNull(sinkTaskPartitionSize)) {
      resultRdd = if (sinkTaskUseRepartition) resultRdd.repartition(sinkTaskPartitionSize) else resultRdd.coalesce(sinkTaskPartitionSize)
    }
    resultRdd.foreachPartition(iterator => {
      val intervalNanos = Duration.ofMillis(batchInterValMs.toLong).toNanos
      while (iterator.hasNext) {
        Try {
          loadFunc(iterator.asJava, schema)
        } match {
          case Success(txnId) => if (enable2PC) handleLoadSuccess(txnId, preCommittedTxnAcc)
          case Failure(e) =>
            if (enable2PC) handleLoadFailure(preCommittedTxnAcc)
            throw new IOException(
              s"Failed to load batch data on BE: ${dorisStreamLoader.getLoadUrlStr} node.", e)
        }
        LockSupport.parkNanos(intervalNanos)
      }
    })

  }

  private def handleLoadSuccess(txnId: Long, acc: CollectionAccumulator[Long]): Unit = {
    acc.add(txnId)
  }

  private def handleLoadFailure(acc: CollectionAccumulator[Long]): Unit = {
    // if task run failed, acc value will not be returned to driver,
    // should abort all pre committed transactions inside the task
    logger.info("load task failed, start aborting previously pre-committed transactions")
    if (acc.isZero) {
      logger.info("no pre-committed transactions, skip abort")
      return
    }
    val abortFailedTxnIds = mutable.Buffer[Long]()
    acc.value.asScala.foreach(txnId => {
      Utils.retry[Unit, Exception](sinkTxnRetries, Duration.ofMillis(sinkTxnIntervalMs), logger) {
        dorisStreamLoader.abortById(txnId)
      } match {
        case Success(_) =>
        case Failure(_) => abortFailedTxnIds += txnId
      }
    })
    if (abortFailedTxnIds.nonEmpty) logger.warn("not aborted txn ids: {}", abortFailedTxnIds.mkString(","))
    acc.reset()
  }


}
