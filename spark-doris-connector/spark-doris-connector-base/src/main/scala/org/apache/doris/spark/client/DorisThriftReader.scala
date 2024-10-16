package org.apache.doris.spark.client

import org.apache.doris.spark.config.{DorisConfig, DorisConfigOptions}
import org.apache.doris.spark.util.SchemaConvertors

import scala.collection.JavaConverters.asScalaBufferConverter

class DorisThriftReader(partition: DorisReaderPartition, config: DorisConfig) extends AbstractThriftReader(partition, config) {

  private val readFields = config.getValue(DorisConfigOptions.DORIS_READ_FIELDS).split(",").map(_.replaceAll("`", ""))

  private val unsupportedCols = config.getValue(DorisConfigOptions.DORIS_UNSUPPORTED_COLUMNS).split(",")

  override protected val dorisSchema: DorisSchema = SchemaConvertors.convertToSchema(scanOpenResult.getSelectedColumns.asScala, readFields, unsupportedCols)

}
