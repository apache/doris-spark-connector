package org.apache.doris.spark.write

import org.apache.commons.lang3.StringUtils
import org.apache.doris.spark.client.write.{DorisWriter, StreamLoadProcessor}
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.util.Retry
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.time.Duration
import scala.collection.mutable
import scala.util.{Failure, Success}

class DorisDataWriter(config: DorisConfig, schema: StructType, partitionId: Int, taskId: Long, epochId: Long = -1) extends DataWriter[InternalRow] with Logging {

  private val writer: DorisWriter[InternalRow] = config.getValue(DorisOptions.LOAD_MODE) match {
    case "stream_load" => new StreamLoadProcessor(config, schema)
    case _ => throw new IllegalArgumentException()
  }

  private val batchSize = config.getValue(DorisOptions.DORIS_SINK_BATCH_SIZE)

  private val batchIntervalMs = config.getValue(DorisOptions.DORIS_SINK_BATCH_INTERVAL_MS)

  private val retries = config.getValue(DorisOptions.DORIS_SINK_MAX_RETRIES)

  private val twoPhaseCommitEnabled = config.getValue(DorisOptions.DORIS_SINK_ENABLE_2PC)

  private var currentBatchCount = 0

  private val committedMessages = mutable.Buffer[String]()

  private lazy val recordBuffer = mutable.Buffer[InternalRow]()

  override def write(record: InternalRow): Unit = {
    if (currentBatchCount >= batchSize) {
      val txnId = Some(writer.stop())
      if (txnId.isDefined) {
        committedMessages += txnId.get
        currentBatchCount = 0
        if (retries != 0) {
          recordBuffer.clear()
        }
      } else {
        throw new Exception()
      }
    }
    loadWithRetries(record)
    currentBatchCount += 1
  }

  override def commit(): WriterCommitMessage = {
    val txnId = writer.stop()
    if (twoPhaseCommitEnabled) {
      if (StringUtils.isNotBlank(txnId)) {
        committedMessages += txnId
      } else {
        throw new Exception()
      }
    }
    DorisWriterCommitMessage(partitionId, taskId, epochId, committedMessages.toArray)
  }

  override def abort(): Unit = {
    close()
  }

  override def close(): Unit = {
    if (writer != null) {
      writer.close()
    }
  }

  @throws[Exception]
  private def loadWithRetries(record: InternalRow): Unit = {
    recordBuffer += record
    var isRetrying = false
    Retry.exec[Unit, Exception](retries, Duration.ofMillis(batchIntervalMs), log) {
      if (isRetrying) {
        currentBatchCount = 0
        do {
          writer.load(recordBuffer(currentBatchCount))
          currentBatchCount += 1
        } while (currentBatchCount < recordBuffer.size - 1)
      }
      writer.load(record)
    } {
      isRetrying = true
    } match {
      case Success(_) => // do nothing
      case Failure(exception) => throw new Exception(exception)
    }

  }

}

case class DorisWriterCommitMessage(partitionId: Int, taskId: Long, epochId: Long, commitMessages: Array[String]) extends WriterCommitMessage
