package org.apache.doris.spark.write

import org.apache.doris.spark.client.{DorisCommitter, StreamLoadProcessor}
import org.apache.doris.spark.config.{DorisConfig, DorisConfigOptions}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class DorisWrite(config: DorisConfig, schema: StructType) extends BatchWrite with StreamingWrite {

  private val committer: DorisCommitter = config.getValue(DorisConfigOptions.LOAD_MODE) match {
    case "stream_load" => new StreamLoadProcessor(config, schema)
    case _ => throw new IllegalArgumentException()
  }

  private var lastCommittedEpoch: Option[Long] = None

  private val committedEpochLock = new AnyRef

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
    new DorisDataWriterFactory(config, schema)
  }

  // for batch write
  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    if (writerCommitMessages.exists(_ != null)) {
      writerCommitMessages.foreach(_.asInstanceOf[DorisWriterCommitMessage].commitMessages.foreach(committer.commit))
    }
  }

  // for batch write
  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    if (writerCommitMessages.exists(_ != null)) {
      writerCommitMessages.foreach(_.asInstanceOf[DorisWriterCommitMessage].commitMessages.foreach(committer.abort))
    }
  }

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