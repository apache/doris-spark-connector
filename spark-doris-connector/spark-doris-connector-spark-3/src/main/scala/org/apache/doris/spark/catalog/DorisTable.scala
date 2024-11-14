package org.apache.doris.spark.catalog

import org.apache.doris.spark.client.{DorisFrontend, DorisSchema}
import org.apache.doris.spark.config.DorisConfig
import org.apache.doris.spark.read.DorisScanBuilder
import org.apache.doris.spark.util.SchemaConvertors
import org.apache.doris.spark.write.DorisWriteBuilder
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._
import scala.language.implicitConversions


class DorisTable(identifier: Identifier, config: DorisConfig, schema: Option[StructType]) extends Table with SupportsRead with SupportsWrite {

  private lazy val frontend:DorisFrontend = DorisFrontend(config)

  override def name(): String = identifier.toString

  override def schema(): StructType = schema.getOrElse({
    val dorisSchema = frontend.getTableSchema(identifier.namespace()(0), identifier.name())
    dorisSchema
  })

  override def capabilities(): util.Set[TableCapability] = {
    Set(BATCH_READ,
      BATCH_WRITE,
      STREAMING_WRITE).asJava
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    new DorisScanBuilder(config: DorisConfig, schema())
  }

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = {
    new DorisWriteBuilder(config, logicalWriteInfo.schema())
  }

  private implicit def dorisSchemaToStructType(dorisSchema: DorisSchema): StructType = {
    StructType(dorisSchema.properties.map(field => {
      StructField(field.name, SchemaConvertors.toCatalystType(field.`type`, field.precision, field.scale))
    }))
  }

}
