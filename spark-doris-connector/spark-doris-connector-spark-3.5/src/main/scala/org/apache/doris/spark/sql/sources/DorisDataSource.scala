package org.apache.doris.spark.sql.sources

import org.apache.doris.spark.catalog.{DorisTable, DorisTableProviderBase}
import org.apache.doris.spark.config.DorisConfig
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.types.StructType

class DorisDataSource extends DorisTableProviderBase with DorisSourceRegisterTrait with Serializable {

  override def newTableInstance(identifier: Identifier, config: DorisConfig, schema: Option[StructType]): Table =
    new DorisTable(identifier, config, schema)

}
