package org.apache.doris.spark.sql

import org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_READ_FIELD
import org.apache.doris.spark.cfg.Settings
import org.apache.doris.spark.exception.ShouldNeverHappenException
import org.apache.doris.spark.rdd.ScalaADBCValueReader
import org.apache.doris.spark.rest.PartitionDefinition
import org.apache.doris.spark.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class ScalaDorisRowADBCValueReader(partition: PartitionDefinition, settings: Settings)
  extends ScalaADBCValueReader(partition, settings) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[ScalaDorisRowADBCValueReader].getName)

  val rowOrder: Seq[String] = settings.getProperty(DORIS_READ_FIELD).split(",")

  override def next: AnyRef = {
    if (!hasNext) {
      logger.error(SHOULD_NOT_HAPPEN_MESSAGE)
      throw new ShouldNeverHappenException
    }
    val row: ScalaDorisRow = new ScalaDorisRow(rowOrder)
    rowBatch.next.asScala.zipWithIndex.foreach{
      case (s, index) if index < row.values.size => row.values.update(index, s)
      case _ => // nothing
    }
    row
  }

}
