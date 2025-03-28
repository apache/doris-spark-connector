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

package org.apache.doris.spark.write

import org.apache.doris.spark.client.write.{CopyIntoProcessor, DorisCommitter, DorisWriter, StreamLoadProcessor}
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.util.Retry
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.time.Duration
import java.util.concurrent.locks.LockSupport
import scala.collection.mutable
import scala.util.{Failure, Random, Success}

class DorisDataWriter(config: DorisConfig, schema: StructType, partitionId: Int, taskId: Long, epochId: Long = -1) extends DataWriter[InternalRow] with Logging {

  private val batchSize = config.getValue(DorisOptions.DORIS_SINK_BATCH_SIZE)

  private val (writer: DorisWriter[InternalRow], committer: DorisCommitter) =
    config.getValue(DorisOptions.LOAD_MODE) match {
      case "stream_load" => (new StreamLoadProcessor(config, schema), new StreamLoadProcessor(config, schema))
      case "copy_into" => (new CopyIntoProcessor(config, schema), new CopyIntoProcessor(config, schema))
      case mode => throw new IllegalArgumentException("Unsupported load mode: " + mode)
    }

  private val batchIntervalMs = config.getValue(DorisOptions.DORIS_SINK_BATCH_INTERVAL_MS)

  private val retries = config.getValue(DorisOptions.DORIS_SINK_MAX_RETRIES)

  private val twoPhaseCommitEnabled = config.getValue(DorisOptions.DORIS_SINK_ENABLE_2PC)

  private val committedMessages = mutable.Buffer[String]()

  private lazy val recordBuffer = mutable.Buffer[InternalRow]()

  override def write(record: InternalRow): Unit = loadBatchWithRetries(record)

  override def commit(): WriterCommitMessage = {
    val txnId = Option(writer.stop())
    if (twoPhaseCommitEnabled) {
      if (txnId.isDefined) {
        committedMessages += txnId.get
      } else {
        log.warn("No txn {} to commit batch", txnId)
      }
    }
    DorisWriterCommitMessage(partitionId, taskId, epochId, committedMessages.toArray)
  }

  override def abort(): Unit = {
    if (committedMessages.nonEmpty) {
      committedMessages.foreach(msg => committer.abort(msg))
    }
    close()
  }

  override def close(): Unit = {
    if (writer != null) {
      writer.close()
    }
  }

  @throws[Exception]
  private def loadBatchWithRetries(record: InternalRow): Unit = {
    var isRetrying = false
    Retry.exec[Unit, Exception](retries, Duration.ofMillis(batchIntervalMs.toLong), log) {
      if (isRetrying) {
        // retrying, reload data from buffer
        do {
          val idx = writer.getBatchCount
          writer.load(recordBuffer(idx))
        } while (writer.getBatchCount < recordBuffer.size)
        isRetrying = false
      }
      if (writer.endOfBatch()) {
        // end of batch, stop batch write
        val txnId = Option(writer.stop())
        if (twoPhaseCommitEnabled) {
          if (txnId.isDefined) {
            committedMessages += txnId.get
          } else {
            throw new Exception("Failed to end batch write")
          }
        }
        // clear buffer if retry is enabled
        if (retries > 0) {
          recordBuffer.clear()
        }
        writer.resetBatchCount()
        LockSupport.parkNanos(Duration.ofMillis(batchIntervalMs.toLong).toNanos)
      }
      writer.load(record)
    } {
      // batch write failed, set retry flag and reset batch count
      isRetrying = true
      writer.resetBatchCount()
    } match {
      case Success(_) => if (retries > 0) recordBuffer += record
      case Failure(exception) => throw new Exception(exception)
    }
  }

}

case class DorisWriterCommitMessage(partitionId: Int, taskId: Long, epochId: Long, commitMessages: Array[String]) extends WriterCommitMessage
