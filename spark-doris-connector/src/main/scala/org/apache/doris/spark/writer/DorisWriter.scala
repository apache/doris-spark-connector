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
import org.apache.doris.spark.load.{CachedDorisStreamLoadClient, DorisStreamLoad}
import org.apache.doris.spark.sql.Utils
import org.apache.doris.spark.txn.TransactionHandler
import org.apache.doris.spark.txn.listener.DorisTransactionListener
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.time.Duration
import java.util
import java.util.Objects
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

class DorisWriter(settings: SparkSettings, preCommittedTxnAcc: CollectionAccumulator[Long]) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisWriter])

  private val sinkTaskPartitionSize: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TASK_PARTITION_SIZE)
  private val sinkTaskUseRepartition: Boolean = settings.getProperty(ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION,
    ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION_DEFAULT.toString).toBoolean

  private val maxRetryTimes: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_MAX_RETRIES,
    ConfigurationOptions.SINK_MAX_RETRIES_DEFAULT)
  private val batchSize: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_SIZE,
    ConfigurationOptions.SINK_BATCH_SIZE_DEFAULT)
  private val batchInterValMs: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS,
    ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS_DEFAULT)

  if (maxRetryTimes > 0) {
    logger.info(s"batch retry enabled, size is $batchSize, interval is $batchInterValMs")
  }

  private val enable2PC: Boolean = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_ENABLE_2PC,
    ConfigurationOptions.DORIS_SINK_ENABLE_2PC_DEFAULT)

  private val dorisStreamLoader: DorisStreamLoad = CachedDorisStreamLoadClient.getOrCreate(settings)

  private var isStreaming = false;

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
    isStreaming = true
    doWrite(dataFrame, dorisStreamLoader.loadStream)
  }

  private def doWrite(dataFrame: DataFrame, loadFunc: (util.Iterator[InternalRow], StructType) => Long): Unit = {
    // do not add spark listener when job is streaming mode
    if (enable2PC && !isStreaming) {
      dataFrame.sparkSession.sparkContext.addSparkListener(new DorisTransactionListener(preCommittedTxnAcc, settings))
    }

    var resultRdd = dataFrame.queryExecution.toRdd
    val schema = dataFrame.schema
    if (Objects.nonNull(sinkTaskPartitionSize)) {
      resultRdd = if (sinkTaskUseRepartition) resultRdd.repartition(sinkTaskPartitionSize) else resultRdd.coalesce(sinkTaskPartitionSize)
    }
    resultRdd.foreachPartition(iterator => {

      while (iterator.hasNext) {
        val batchIterator = new BatchIterator[InternalRow](iterator, batchSize, maxRetryTimes > 0)
        val retry = Utils.retry[Long, Exception](maxRetryTimes, Duration.ofMillis(batchInterValMs.toLong), logger) _
        retry(loadFunc(batchIterator.asJava, schema))(batchIterator.reset()) match {
          case Success(txnId) =>
            if (enable2PC) handleLoadSuccess(txnId, preCommittedTxnAcc)
            batchIterator.close()
          case Failure(e) =>
            if (enable2PC) handleLoadFailure(preCommittedTxnAcc)
            batchIterator.close()
            throw new IOException(
              s"Failed to load batch data on BE: ${dorisStreamLoader.getLoadUrlStr} node.", e)
        }
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

    try TransactionHandler(settings).abortTransactions(acc.value.asScala.toList)
    catch {
      case e: Exception => throw e
    }
    finally acc.reset()
  }

  /**
   * iterator for batch load
   * if retry time is greater than zero, enable batch retry and put batch data into buffer
   *
   * @param iterator         parent iterator
   * @param batchSize        batch size
   * @param batchRetryEnable whether enable batch retry
   * @tparam T data type
   */
  private class BatchIterator[T](iterator: Iterator[T], batchSize: Int, batchRetryEnable: Boolean) extends Iterator[T] {

    private val buffer: ArrayBuffer[T] = if (batchRetryEnable) new ArrayBuffer[T](batchSize) else ArrayBuffer.empty[T]

    private var recordCount = 0

    private var isReset = false

    override def hasNext: Boolean = recordCount < batchSize && iterator.hasNext

    override def next(): T = {
      recordCount += 1
      if (batchRetryEnable) {
        if (isReset && buffer.nonEmpty) {
          buffer(recordCount)
        } else {
          val elem = iterator.next
          buffer += elem
          elem
        }
      } else {
        iterator.next
      }
    }

    /**
     * reset record count for re-read
     */
    def reset(): Unit = {
      recordCount = 0
      isReset = true
      logger.info("batch iterator is reset")
    }

    /**
     * clear buffer when buffer is not empty
     */
    def close(): Unit = {
      if (buffer.nonEmpty) {
        buffer.clear()
        logger.info("buffer is cleared and batch iterator is closed")
      }
    }

  }


}
