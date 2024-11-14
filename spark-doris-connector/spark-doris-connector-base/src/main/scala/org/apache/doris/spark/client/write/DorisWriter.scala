package org.apache.doris.spark.client.write

trait DorisWriter[R] extends Serializable {

  def load(row: R): Unit

  def stop(): String

  def close(): Unit

}
