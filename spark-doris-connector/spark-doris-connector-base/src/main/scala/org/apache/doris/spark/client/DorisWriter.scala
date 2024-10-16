package org.apache.doris.spark.client

import org.apache.spark.sql.catalyst.InternalRow

trait DorisWriter[R] {

  def load(row: R): Unit

  def stop(): String

  def close(): Unit

}
