package org.apache.doris.spark.catalog

import org.apache.doris.spark.config.DorisConfig
import org.apache.doris.spark.read.DorisScanBuilder
import org.apache.doris.spark.write.DorisWriteBuilder
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.types.StructType

class DorisTable(identifier: Identifier, config: DorisConfig, schema: Option[StructType])
  extends DorisTableBase(identifier, config, schema) {

  override def createScanBuilder(config: DorisConfig, schema: StructType): ScanBuilder = new DorisScanBuilder(config, schema)
  override protected def createWriteBuilder(config: DorisConfig, schema: StructType): WriteBuilder = new DorisWriteBuilder(config, schema)
}
