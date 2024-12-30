package org.apache.doris.spark.catalog

import org.apache.doris.spark.config.DorisConfig
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.types.StructType

class DorisTableCatalog extends DorisTableCatalogBase {
  override def dropNamespace(strings: Array[String]): Boolean = throw new UnsupportedOperationException()
  override def newTableInstance(identifier: Identifier, config: DorisConfig, schema: Option[StructType]): Table =
    new DorisTable(identifier, config, schema)
}
