package org.apache.doris.spark.client.read

import org.apache.commons.lang3.StringUtils
import org.apache.doris.spark.client.{DorisFrontend, DorisReaderPartition, QueryPlan}
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.exception.DorisException
import org.apache.doris.spark.util.DorisDialects
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable
import scala.util.control.Breaks

object ReaderPartitionGenerator {

  /*
   * for spark 2
   */
  def generatePartitions(config: DorisConfig): Array[DorisReaderPartition] = {
    val readCols = (frontend: DorisFrontend, db: String, table: String) => {
      if (config.contains(DorisOptions.DORIS_READ_FIELDS) && config.getValue(DorisOptions.DORIS_READ_FIELDS) != "*") {
        config.getValue(DorisOptions.DORIS_READ_FIELDS).split(",").map(_.replaceAll("`", ""))
      } else {
        frontend.getTableAllColumns(db, table)
      }
    }
    doGenerate(config, readCols,
      if (config.contains(DorisOptions.DORIS_FILTER_QUERY)) config.getValue(DorisOptions.DORIS_FILTER_QUERY) else "")
  }

  /*
   * for spark 3
   */
  def generatePartitions(config: DorisConfig,
                         schema: StructType = StructType(Array[StructField]()),
                         filters: Array[Filter] = Array()): Array[DorisReaderPartition] = {
    val readCols = (frontend: DorisFrontend, db: String, table: String) => {
      if (schema.isEmpty) frontend.getTableAllColumns(db, table) else schema.fields.map(_.name)
    }
    doGenerate(config, readCols, if (filters.isEmpty) "" else filters.map(DorisDialects.compileFilter).mkString(" AND "))
  }

  private def doGenerate(config: DorisConfig, readCols: (DorisFrontend, String, String) => Array[String],
                         predicates: String): Array[DorisReaderPartition] = {
    val frontend = DorisFrontend(config)
    val fullTableName = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER)
    val (db, table) = {
      val arr = fullTableName.split("\\.")
      (arr(0).replaceAll("`", ""), arr(1).replaceAll("`", ""))
    }
    val unsupportedCols: Array[String] = config.getValue(DorisOptions.DORIS_UNSUPPORTED_COLUMNS).split(",").map(_.replaceAll("`", ""))
    val finalReadCols = readCols(frontend, db, table).map {
      case f if unsupportedCols.contains(f) => s"'READ UNSUPPORTED' AS `$f`"
      case f => s"`$f`"
    }.mkString(",")
    val sql = s"SELECT $finalReadCols FROM `$db`.`$table`${if (StringUtils.isBlank(predicates)) "" else " WHERE " + predicates}"
    val queryPlan = frontend.getQueryPlan(db, table, sql)
    val beToTablets = mappingBeToTablets(queryPlan)
    val maxTabletSize = config.getValue(DorisOptions.DORIS_TABLET_SIZE)
    distributeTabletsToPartitions(db, table, beToTablets, queryPlan.opaquedQueryPlan, maxTabletSize, config)
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
          throw new DorisException()
        }
        be2Tablets(targetBe.get) += tabletId.toLong
      }
    }
    be2Tablets.toMap
  }

  private def distributeTabletsToPartitions(database: String, table: String, beToTablets: Map[String, mutable.Buffer[Long]],
                                            opaquedQueryPlan: String, maxTabletSize: Int, config: DorisConfig): Array[DorisReaderPartition] = {
    beToTablets.flatMap {
      case (backend, tabletIds) =>
        val distinctTablets = tabletIds.distinct
        val partitions = new mutable.ListBuffer[DorisReaderPartition]
        var offset = 0
        while (offset < distinctTablets.size) {
          val tablets = distinctTablets.slice(offset, offset + maxTabletSize).toArray
          offset += maxTabletSize
          partitions += DorisReaderPartition(database, table, backend, tablets, opaquedQueryPlan, config)
        }
        partitions
    }.toArray
  }

}
