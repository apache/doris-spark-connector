package org.apache.doris.spark.client

import org.apache.commons.lang3.StringUtils
import org.apache.doris.spark.config.{DorisConfig, DorisConfigOptions}
import org.apache.doris.spark.util.DorisDialects
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable
import scala.util.control.Breaks

object ReaderPartitionGenerator {

  def generatePartitions(config: DorisConfig, schema: StructType = StructType(Array[StructField]()), filters: Array[Filter] = Array()): Array[DorisReaderPartition] = {
    DorisFrontend.initialize(config)
    val fullTableName = config.getValue(DorisConfigOptions.DORIS_TABLE_IDENTIFIER)
    val (db, table) = {
      val arr = fullTableName.split("\\.")
      (arr(0).replaceAll("`", ""), arr(1).replaceAll("`", ""))
    }
    val unsupportedCols: Array[String] = config.getValue(DorisConfigOptions.DORIS_UNSUPPORTED_COLUMNS).split(",").map(_.replaceAll("`", ""))
    val readCols = readColumns(db, table, schema, unsupportedCols)
    val predicates: String = if (filters.isEmpty) {
      if (config.contains(DorisConfigOptions.DORIS_FILTER_QUERY)) {
        config.getValue(DorisConfigOptions.DORIS_FILTER_QUERY)
      } else ""
    } else filters.map(DorisDialects.compileFilter).mkString(" AND ")
    doGenerate(db, table, readCols, predicates,config)
  }

  private def doGenerate(db: String, table: String, readColumns: String, predicates: String, config: DorisConfig): Array[DorisReaderPartition] = {
    val sql = s"SELECT $readColumns FROM `$db`.`$table`${if (StringUtils.isBlank(predicates)) "" else " WHERE " + predicates}"
    val queryPlan = DorisFrontend.getQueryPlan(db, table, sql)
    val beToTablets = mappingBeToTablets(queryPlan)
    val maxTabletSize = config.getValue(DorisConfigOptions.DORIS_TABLET_SIZE)
    generatePartitions(db, table, beToTablets, queryPlan.opaquedQueryPlan, maxTabletSize)
  }

  private def readColumns(db: String, table: String, schema: StructType, unsupportedCols: Array[String]): String = {
    (if (schema.isEmpty) DorisFrontend.getTableAllColumns(db, table) else schema.fields.map(_.name)).map {
      case f if unsupportedCols.contains(f) => s"'READ UNSUPPORTED' AS `$f`"
      case f => s"`$f`"
    }.mkString(",")
  }

  private def mappingBeToTablets(queryPlan: QueryPlan): Map[String, mutable.Buffer[Long]] = {
    val be2Tablets = new mutable.HashMap[String, mutable.Buffer[Long]]
    queryPlan.partitions.foreach {
      case (tabletId, tabletInfos) => {
        var targetBe: Option[String] = None
        var tabletCount = Integer.MAX_VALUE
        Breaks.breakable {
          tabletInfos.routings.foreach(backend => {
            if (!be2Tablets.contains(backend)) {
              be2Tablets += (backend -> mutable.ListBuffer[Long]())
              targetBe = Some(backend)
              Breaks.break()
            } else if (be2Tablets(backend).size < tabletCount) {
              targetBe = Some(backend)
              tabletCount = be2Tablets(backend).size
            }
          })
        }
        if (targetBe.isEmpty) {
          // TODO: throw exception
          throw new Exception()
        }
        be2Tablets(targetBe.get) += tabletId.toLong
      }
    }
    be2Tablets.toMap
  }

  private def generatePartitions(database: String, table: String, beToTablets: Map[String, mutable.Buffer[Long]],
                                 opaquedQueryPlan: String, maxTabletSize: Int): Array[DorisReaderPartition] = {
    beToTablets.flatMap {
      case (backend, tabletIds) =>
        val distinctTablets = tabletIds.distinct
        val partitions = new mutable.ListBuffer[DorisReaderPartition]
        var offset = 0
        while (offset < distinctTablets.size) {
          val tablets = distinctTablets.slice(offset, offset + maxTabletSize).toArray
          offset += maxTabletSize
          partitions += DorisReaderPartition(database, table, backend, tablets, opaquedQueryPlan)
        }
        partitions
    }.toArray
  }

}
