package org.apache.doris.spark.client

trait DorisCommitter {

  def commit(m: String): Unit

  def abort(m: String): Unit

}
