package org.apache.doris.spark.read

import org.apache.doris.spark.config.DorisConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class DorisPartitionReaderFactory(schema: StructType, mode: ScanMode, config: DorisConfig) extends PartitionReaderFactory {

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    new DorisPartitionReader(inputPartition, schema, mode, config)
  }

}