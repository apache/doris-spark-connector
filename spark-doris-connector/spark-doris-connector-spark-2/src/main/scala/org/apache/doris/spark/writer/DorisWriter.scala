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

import org.apache.doris.spark.client.write.{CopyIntoProcessor, DorisCommitter, StreamLoadProcessor}
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.load.CommitMessage
import org.apache.doris.spark.sql.Utils
import org.apache.doris.spark.txn.TransactionHandler
import org.apache.doris.spark.txn.listener.DorisTransactionListener
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

class DorisWriter(config: DorisConfig,
                  txnAcc: CollectionAccumulator[CommitMessage],
                  isStreaming: Boolean) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisWriter])

  private val loadMode: String = config.getValue(DorisOptions.LOAD_MODE)
  private val sinkTaskUseRepartition: Boolean = config.getValue(DorisOptions.DORIS_SINK_TASK_USE_REPARTITION)

  private val maxRetryTimes: Int = config.getValue(DorisOptions.DORIS_SINK_MAX_RETRIES)
  private val batchSize: Int = config.getValue(DorisOptions.DORIS_SINK_BATCH_SIZE)
  private val batchInterValMs: Int = config.getValue(DorisOptions.DORIS_SINK_BATCH_INTERVAL_MS)

  if (maxRetryTimes > 0) {
    logger.info(s"batch retry enabled, size is $batchSize, interval is $batchInterValMs")
  }

  private val enable2PC: Boolean = config.getValue(DorisOptions.DORIS_SINK_ENABLE_2PC)

  private val writer: org.apache.doris.spark.client.write.DorisWriter[InternalRow] = generateLoader

  private val sinkTxnRetries = config.getValue(DorisOptions.DORIS_SINK_TXN_RETRIES)

  private val sinkTxnIntervalMs = config.getValue(DorisOptions.DORIS_SINK_TXN_INTERVAL_MS)

  private val txnHandler: TransactionHandler = TransactionHandler(generateCommitter, sinkTxnRetries, sinkTxnIntervalMs)

  /**
   * write data in batch mode
   *
   * @param dataFrame source dataframe
   */
  def write(dataFrame: DataFrame): Unit = {
    doWrite(dataFrame, batchWriteFunc)
  }

  private def doWrite(dataFrame: DataFrame, loadFunc: (Iterator[InternalRow], StructType) => Option[CommitMessage]): Unit = {
    // do not add spark listener when job is streaming mode
    if (enable2PC && !isStreaming) {
      dataFrame.sparkSession.sparkContext.addSparkListener(new DorisTransactionListener(txnAcc, txnHandler))
    }
    var resultDataFrame = dataFrame
    if (config.contains(DorisOptions.DORIS_SINK_TASK_PARTITION_SIZE)) {
      val sinkTaskPartitionSize = config.getValue(DorisOptions.DORIS_SINK_TASK_PARTITION_SIZE)
      resultDataFrame = if (sinkTaskUseRepartition) dataFrame.repartition(sinkTaskPartitionSize) else dataFrame.coalesce(sinkTaskPartitionSize)
    }

    val resultRdd = resultDataFrame.queryExecution.toRdd
    val schema = resultDataFrame.schema

    resultRdd.foreachPartition(iterator => {
      while (iterator.hasNext) {
        val batchIterator = new BatchIterator(iterator, batchSize, maxRetryTimes > 0)
        val retry = Utils.retry[Option[CommitMessage], Exception](maxRetryTimes, Duration.ofMillis(batchInterValMs.toLong), logger) _
        retry(loadFunc(batchIterator, schema))(batchIterator.reset()) match {
          case Success(msg) =>
            if (enable2PC) handleLoadSuccess(msg, txnAcc)
            batchIterator.close()
          case Failure(e) =>
            if (enable2PC) handleLoadFailure(txnAcc)
            batchIterator.close()
            throw e
        }
      }

    })

  }

  private def handleLoadSuccess(msg: Option[CommitMessage], acc: CollectionAccumulator[CommitMessage]): Unit = {
    acc.add(msg.get)
  }

  private def handleLoadFailure(acc: CollectionAccumulator[CommitMessage]): Unit = {
    // if task run failed, acc value will not be returned to driver,
    // should abort all pre committed transactions inside the task
    logger.info("load task failed, start aborting previously pre-committed transactions")
    if (acc.isZero) {
      logger.info("no pre-committed transactions, skip abort")
      return
    }

    try txnHandler.abortTransactions(acc.value.asScala.toList)
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
   */
  private class BatchIterator(iterator: Iterator[InternalRow], batchSize: Int, batchRetryEnable: Boolean) extends Iterator[InternalRow] {

    private val buffer: ArrayBuffer[InternalRow] =
      if (batchRetryEnable) new ArrayBuffer[InternalRow](batchSize) else ArrayBuffer.empty[InternalRow]

    private var recordCount = 0

    private var isReset = false

    override def hasNext: Boolean = {
      if (recordCount < batchSize) {
        if (isReset) {
          recordCount < buffer.size
        } else {
          iterator.hasNext
        }
      } else {
        false
      }
    }

    override def next(): InternalRow = {
      recordCount += 1
      if (batchRetryEnable) {
        if (isReset) {
          readBuffer()
        } else {
          writeBufferAndReturn()
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
      isReset = buffer.nonEmpty
      if (isReset) {
        logger.info("buffer is not empty and batch iterator is reset")
      }
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

    private def readBuffer(): InternalRow = {
      if (recordCount == buffer.size) {
        logger.debug("read buffer end, recordCount:{}, bufferSize: {}", recordCount, buffer.size)
        isReset = false
      }
      buffer(recordCount - 1)
    }

    private def writeBufferAndReturn(): InternalRow = {
      val elem = iterator.next.copy
      buffer += elem
      elem
    }

  }

  @throws[IllegalArgumentException]
  private def generateLoader: org.apache.doris.spark.client.write.DorisWriter[InternalRow] = {
    loadMode match {
      case "stream_load" => new StreamLoadProcessor(config)
      case "copy_into" => new CopyIntoProcessor(config)
      // case "stream_load" => new StreamLoader(settings, isStreaming)
      // case "copy_into" => new CopyIntoLoader(settings, isStreaming)
      case _ => throw new IllegalArgumentException(s"Unsupported load mode: $loadMode")
    }
  }

  @throws[IllegalArgumentException]
  private def generateCommitter: DorisCommitter = {
    loadMode match {
      case "stream_load" => new StreamLoadProcessor(config)
      case "copy_into" => new CopyIntoProcessor(config)
      case _ => throw new IllegalArgumentException(s"Unsupported load mode: $loadMode")
    }
  }

  private def batchWriteFunc(iter: Iterator[InternalRow], schema: StructType): Option[CommitMessage] = {
    writer match {
      case processor: StreamLoadProcessor => processor.setSchema(schema)
      case processor: CopyIntoProcessor => processor.setSchema(schema)
      case _ => // do nothing
    }
    while (iter.hasNext) {
      writer.load(iter.next())
    }
    val txnId = writer.stop()
    if (txnId == null) None else Some(CommitMessage(txnId.toLong))
  }

  def getTransactionHandler: TransactionHandler = txnHandler


}
