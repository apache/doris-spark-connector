package org.apache.doris.spark.client.read

import org.apache.doris.spark.client.DorisReaderPartition
import org.apache.doris.spark.config.DorisConfig

abstract class DorisReader(partition: DorisReaderPartition, config: DorisConfig) {

  protected var rowBatch: RowBatch = _

  def hasNext: Boolean

  def next(): AnyRef

  def close(): Unit

}
