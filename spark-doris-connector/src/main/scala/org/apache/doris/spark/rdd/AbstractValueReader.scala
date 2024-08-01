package org.apache.doris.spark.rdd

import org.apache.doris.spark.serialization.RowBatch

trait AbstractValueReader {

  protected var rowBatch: RowBatch = _

  def hasNext: Boolean

  /**
   * get next value.
   * @return next value
   */
  def next: AnyRef

  def close(): Unit

}
