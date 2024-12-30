package org.apache.doris.spark.catalog

import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

abstract class DorisTableProviderBase extends TableProvider {

  protected var t: Table = _

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (t == null) t = getTable(options)
    t.schema()
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    if (t != null) t
    else {
      val dorisConfig = DorisConfig.fromMap(properties, false)
      val tableIdentifier = dorisConfig.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER)
      val tableIdentifierArr = tableIdentifier.split("\\.")
      newTableInstance(Identifier.of(Array[String](tableIdentifierArr(0)), tableIdentifierArr(1)), dorisConfig, Some(schema))
    }
  }

  private def getTable(options: CaseInsensitiveStringMap): Table = {
    if (t != null) t
    else {
      val dorisConfig = DorisConfig.fromMap(options, false)
      val tableIdentifier = dorisConfig.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER)
      val tableIdentifierArr = tableIdentifier.split("\\.")
      newTableInstance(Identifier.of(Array[String](tableIdentifierArr(0)), tableIdentifierArr(1)), dorisConfig, None)
    }
  }

  def newTableInstance(identifier: Identifier, config: DorisConfig, schema: Option[StructType]): Table;

}
