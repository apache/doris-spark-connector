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

import org.apache.doris.spark.DorisStreamLoad
import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.sql.DorisSourceProvider.SHORT_NAME
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.{Logger, LoggerFactory}
import java.io.IOException
import java.util
import org.apache.doris.spark.rest.RestService
import java.util.Objects
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.util.control.Breaks

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
    // init stream loader
    val dorisStreamLoader = new DorisStreamLoad(sparkSettings, data.columns)

    val maxRowCount = sparkSettings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_SIZE, ConfigurationOptions.SINK_BATCH_SIZE_DEFAULT)
    val maxRetryTimes = sparkSettings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_MAX_RETRIES, ConfigurationOptions.SINK_MAX_RETRIES_DEFAULT)
    val sinkTaskPartitionSize = sparkSettings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TASK_PARTITION_SIZE)
    val sinkTaskUseRepartition = sparkSettings.getProperty(ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION, ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION_DEFAULT.toString).toBoolean
    val batchInterValMs = sparkSettings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS, ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS_DEFAULT)

    logger.info(s"maxRowCount ${maxRowCount}")
    logger.info(s"maxRetryTimes ${maxRetryTimes}")
    logger.info(s"batchInterVarMs ${batchInterValMs}")

    var resultRdd = data.rdd
    if (Objects.nonNull(sinkTaskPartitionSize)) {
      resultRdd = if (sinkTaskUseRepartition) resultRdd.repartition(sinkTaskPartitionSize) else resultRdd.coalesce(sinkTaskPartitionSize)
    }

    resultRdd.foreachPartition(partition => {
      val rowsBuffer: util.List[util.List[Object]] = new util.ArrayList[util.List[Object]](maxRowCount)
      partition.foreach(row => {
        val line: util.List[Object] = new util.ArrayList[Object]()
        for (i <- 0 until row.size) {
          val field = row.get(i)
          line.add(field.asInstanceOf[AnyRef])
        }
        rowsBuffer.add(line)
        if (rowsBuffer.size > maxRowCount - 1 ) {
          flush
        }
      })
      // flush buffer
      if (!rowsBuffer.isEmpty) {
        flush
      }

      /**
       * flush data to Doris and do retry when flush error
       *
       */
      def flush = {
        val loop = new Breaks
        var err: Exception = null
        loop.breakable {

          for (i <- 1 to maxRetryTimes) {
            try {
              dorisStreamLoader.loadV2(rowsBuffer)
              rowsBuffer.clear()
              Thread.sleep(batchInterValMs.longValue())
              loop.break()
            }
            catch {
              case e: Exception =>
                try {
                  logger.debug("Failed to load data on BE: {} node ", dorisStreamLoader.getLoadUrlStr)
                  if (err == null) err = e
                  Thread.sleep(1000 * i)
                } catch {
                  case ex: InterruptedException =>
                    Thread.currentThread.interrupt()
                    throw new IOException("unable to flush; interrupted while doing another attempt", e)
                }
            }
          }

          if (!rowsBuffer.isEmpty) {
            throw new IOException(s"Failed to load ${maxRowCount} batch data on BE: ${dorisStreamLoader.getLoadUrlStr} node and exceeded the max ${maxRetryTimes} retry times.", err)
          }
        }

      }

    })
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