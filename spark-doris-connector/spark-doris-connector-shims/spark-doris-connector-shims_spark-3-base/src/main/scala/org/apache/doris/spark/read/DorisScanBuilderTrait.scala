package org.apache.doris.spark.read

import org.apache.doris.spark.config.DorisConfig
import org.apache.doris.spark.util.DorisDialects
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

protected[spark] abstract class DorisScanBuilderTrait(config: DorisConfig, schema: StructType) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var readSchema: StructType = schema

  private var pushDownPredicates: Array[Filter] = Array[Filter]()

  override def build(): Scan = new DorisScan(config, readSchema, pushDownPredicates)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, unsupported) = filters.partition(DorisDialects.compileFilter(_).isDefined)
    this.pushDownPredicates = pushed
    unsupported
  }

  override def pushedFilters(): Array[Filter] = pushDownPredicates

  override def pruneColumns(requiredSchema: StructType): Unit = {
    readSchema = StructType(requiredSchema.fields.filter(schema.contains(_)))
  }

}
