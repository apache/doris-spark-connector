package org.apache.doris.spark.client.read

import org.apache.doris.spark.client.{DorisReaderPartition, DorisSchema}
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.util.SchemaConvertors

import scala.collection.JavaConverters.asScalaBufferConverter

class DorisThriftReader(partition: DorisReaderPartition, config: DorisConfig) extends AbstractThriftReader(partition, config) {

  private lazy val tableSchema: DorisSchema = {
    val tableIdentifier = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER)
    val arr = tableIdentifier.split("\\.")
    frontend.getTableSchema(arr(0), arr(1))
  }

  private lazy val readFields: Array[String] = {
    if (config.contains(DorisOptions.DORIS_READ_FIELDS)) {
      val dorisReadFields = config.getValue(DorisOptions.DORIS_READ_FIELDS)
      if (dorisReadFields == "*") {
        tableSchema.properties.map(_.name.replaceAll("`", "")).toArray
      } else dorisReadFields.split(",").map(_.replaceAll("`", ""))
    } else tableSchema.properties.map(_.name.replaceAll("`", "")).toArray
  }

  private lazy val unsupportedCols = tableSchema.properties
    .filter(field => field.`type`.toUpperCase == "BITMAP" || field.`type`.toUpperCase == "HLL").map(_.name)
    .toArray

  override protected val dorisSchema: DorisSchema = {
    SchemaConvertors.convertToSchema(scanOpenResult.getSelectedColumns.asScala, readFields, unsupportedCols)
  }

}
