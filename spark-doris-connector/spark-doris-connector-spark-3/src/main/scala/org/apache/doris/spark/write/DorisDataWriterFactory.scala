package org.apache.doris.spark.write

import org.apache.doris.spark.config.DorisConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class DorisDataWriterFactory(config: DorisConfig, schema: StructType) extends DataWriterFactory with StreamingDataWriterFactory {

  // for batch write
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new DorisDataWriter(config, schema, partitionId, taskId)
  }

  // for streaming write
  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new DorisDataWriter(config, schema, partitionId, taskId, epochId)
  }
}
