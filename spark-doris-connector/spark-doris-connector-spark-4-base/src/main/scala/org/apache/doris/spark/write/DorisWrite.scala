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

import org.apache.doris.spark.client.write.{CopyIntoProcessor, DorisCommitter, StreamLoadProcessor}
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class DorisWrite(config: DorisConfig, schema: StructType) extends BatchWrite with StreamingWrite {

  private val LOG = LoggerFactory.getLogger(classOf[DorisWrite])

  private val committer: DorisCommitter = config.getValue(DorisOptions.LOAD_MODE) match {
    case "stream_load" => new StreamLoadProcessor(config, schema)
    case "copy_into" => new CopyIntoProcessor(config, schema)
    case _ => throw new IllegalArgumentException()
  }

  private var lastCommittedEpoch: Option[Long] = None

  private val committedEpochLock = new AnyRef

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
    new DorisDataWriterFactory(config, schema)
  }

  // for batch write
  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    if (writerCommitMessages != null && writerCommitMessages.nonEmpty) {
      writerCommitMessages.filter(_ != null)
        .foreach(_.asInstanceOf[DorisWriterCommitMessage].commitMessages.foreach(committer.commit))
    }
  }

  // for batch write
  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    LOG.info("writerCommitMessages size: " + writerCommitMessages.length)
    if (writerCommitMessages.exists(_ != null) && writerCommitMessages.nonEmpty) {
      writerCommitMessages.filter(_ != null)
        .foreach(_.asInstanceOf[DorisWriterCommitMessage].commitMessages.foreach(committer.abort))
    }
  }

  override def useCommitCoordinator(): Boolean = true

  override def createStreamingWriterFactory(physicalWriteInfo: PhysicalWriteInfo): StreamingDataWriterFactory = {
    new DorisDataWriterFactory(config, schema)
  }

  // for streaming write
  override def commit(epochId: Long, writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    committedEpochLock.synchronized {
      if (lastCommittedEpoch.isEmpty || epochId > lastCommittedEpoch.get && writerCommitMessages.exists(_ != null)) {
        writerCommitMessages.foreach(_.asInstanceOf[DorisWriterCommitMessage].commitMessages.foreach(committer.commit))
        lastCommittedEpoch = Some(epochId)
      }
    }
  }

  // for streaming write
  override def abort(epochId: Long, writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    committedEpochLock.synchronized {
      if ((lastCommittedEpoch.isEmpty || epochId > lastCommittedEpoch.get) && writerCommitMessages.exists(_ != null)) {
        writerCommitMessages.foreach(_.asInstanceOf[DorisWriterCommitMessage].commitMessages.foreach(committer.abort))
      }
    }
  }

}