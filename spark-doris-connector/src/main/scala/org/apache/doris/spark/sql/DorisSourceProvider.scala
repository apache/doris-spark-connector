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

import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.exception.IllegalArgumentException
import org.apache.doris.spark.sql.DorisSourceProvider.SHORT_NAME
import org.apache.doris.spark.writer.DorisWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.mapAsJavaMapConverter

private[sql] class DorisSourceProvider extends DataSourceRegister
  with RelationProvider
  with CreatableRelationProvider
  with StreamSinkProvider
  with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisSourceProvider].getName)

  override def shortName(): String = SHORT_NAME

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new DorisRelation(sqlContext, Utils.params(parameters, logger))
  }


  /**
   * df.save
   */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    val sparkSettings = new SparkSettings(sqlContext.sparkContext.getConf)
    sparkSettings.merge(Utils.params(parameters, logger).asJava)

    if (mode == SaveMode.Overwrite) {
      val tableIdentifier = sparkSettings.getProperty(ConfigurationOptions.DORIS_TABLE_IDENTIFIER)
      val identifier = tableIdentifier.split("\\.")
      if (identifier.length != 2) {
        logger.error(s"argument 'table.identifier' is illegal, value is '${tableIdentifier}'.")
        throw new IllegalArgumentException("table.identifier", tableIdentifier)
      }

      JdbcUtils.truncateAndCreateTableFromSchema(
        sparkSettings.getProperty(ConfigurationOptions.DORIS_JDBC_DRIVER),
        sparkSettings.getProperty(ConfigurationOptions.DORIS_JDBC_URL),
        sparkSettings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_USER),
        sparkSettings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD),
        identifier(0), identifier(1),
        data.schema
      )
    }

    // init stream loader
    val writer = new DorisWriter(sparkSettings)
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
    val sparkSettings = new SparkSettings(new SparkConf())
    sparkSettings.merge(Utils.params(parameters, logger).asJava)
    new DorisStreamLoadSink(sqlContext, sparkSettings)
  }
}

object DorisSourceProvider {
  val SHORT_NAME: String = "doris"
}