package org.apache.doris.spark.load

import org.apache.doris.spark.cfg.SparkSettings
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Loader, interface class for write data to doris
 */
trait Loader extends Serializable {

  /**
   * execute load
   *
   * @param iterator row data iterator
   * @param schema row data schema
   * @return commit message
   */
  def load(iterator: Iterator[InternalRow], schema: StructType): Option[CommitMessage]

  /**
   * commit transaction
   *
   * @param msg commit message
   */
  def commit(msg: CommitMessage): Unit

  /**
   * abort transaction
   *
   * @param msg commit message
   */
  def abort(msg: CommitMessage): Unit

}

/**
 * Commit message class
 *
 * @param value message value
 */
case class CommitMessage(value: Any) extends Serializable