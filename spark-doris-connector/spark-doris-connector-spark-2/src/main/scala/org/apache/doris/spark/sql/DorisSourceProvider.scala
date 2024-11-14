// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.sql

import org.apache.doris.spark.client.DorisFrontend
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.load.CommitMessage
import org.apache.doris.spark.sql.DorisSourceProvider.SHORT_NAME
import org.apache.doris.spark.writer.DorisWriter
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

private[sql] class DorisSourceProvider extends DataSourceRegister
  with RelationProvider
  with CreatableRelationProvider
  with StreamSinkProvider
  with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisSourceProvider].getName)

  override def shortName(): String = SHORT_NAME

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new DorisRelation(sqlContext, parameters)
  }


  /**
   * df.save
   */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    val config = DorisConfig.fromSparkConf(sqlContext.sparkContext.getConf, parameters)

    mode match {
      case SaveMode.Overwrite =>
        truncateTable(config)
      case _: SaveMode => // do nothing
    }

    // accumulator for transaction handling
    val acc = sqlContext.sparkContext.collectionAccumulator[CommitMessage]("BatchTxnAcc")
    // init stream loader
    val writer = new DorisWriter(config, acc, false)
    writer.write(data)

    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException

      override def schema: StructType = unsupportedException

      override def needConversion: Boolean = unsupportedException

      override def sizeInBytes: Long = unsupportedException

      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException

      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from doris write operation is not usable.")
    }
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    new DorisStreamLoadSink(sqlContext, DorisConfig.fromMap(Utils.params(parameters, logger)))
  }

  private def truncateTable(config: DorisConfig): Unit = {
    val frontend = DorisFrontend(config)
    frontend.queryFrontends()(conn => {
      val query = s"TRUNCATE TABLE ${config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER)}"
      val stmt = conn.createStatement()
      stmt.execute(query)
      logger.info(s"truncate table ${config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER)} success")
    })
  }

}

object DorisSourceProvider {
  val SHORT_NAME: String = "doris"
}