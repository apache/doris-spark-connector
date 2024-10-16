package org.apache.doris.spark.write

import org.apache.doris.spark.config.DorisConfig
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.connector.write.{BatchWrite, WriteBuilder}
import org.apache.spark.sql.types.StructType

class DorisWriteBuilder(config: DorisConfig, schema: StructType) extends WriteBuilder {

  override def buildForBatch(): BatchWrite = {
    new DorisWrite(config, schema)
  }

  override def buildForStreaming(): StreamingWrite = {
    new DorisWrite(config, schema)
  }

}
