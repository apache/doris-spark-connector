package org.apache.doris.spark.client.write

trait DorisCommitter extends Serializable {

  def commit(m: String): Unit

  def abort(m: String): Unit

}
