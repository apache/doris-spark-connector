package org.apache.doris.spark.sql

import org.apache.doris.spark.client.entity.DorisReaderPartition
import org.apache.doris.spark.client.read.DorisFlightSqlReader
import org.apache.doris.spark.config.DorisOptions
import org.apache.doris.spark.exception.ShouldNeverHappenException

import scala.collection.JavaConverters.asScalaBufferConverter

class DorisRowFlightSqlReader(partition: DorisReaderPartition) extends DorisFlightSqlReader(partition)  {

  private val rowOrder: Seq[String] = config.getValue(DorisOptions.DORIS_READ_FIELDS).split(",")

  override def next(): AnyRef = {
    if (!hasNext) {
      throw new ShouldNeverHappenException
    }
    val row: DorisRow = new DorisRow(rowOrder)
    rowBatch.next.asScala.zipWithIndex.foreach {
      case (s, index) if index < row.values.size => row.values.update(index, s)
      case _ => // nothing
    }
    row
  }
}
