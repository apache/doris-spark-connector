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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.{CachedDorisStreamLoadClient, DorisStreamLoad}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}
import java.io.IOException
import org.apache.doris.spark.rest.RestService
import java.util.Objects
import scala.util.control.Breaks

private[sql] class DorisStreamLoadSink(sqlContext: SQLContext, settings: SparkSettings) extends Sink with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisStreamLoadSink].getName)
  @volatile private var latestBatchId = -1L
  val maxRowCount: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_SIZE, ConfigurationOptions.SINK_BATCH_SIZE_DEFAULT)
  val maxRetryTimes: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_MAX_RETRIES, ConfigurationOptions.SINK_MAX_RETRIES_DEFAULT)
  val sinkTaskPartitionSize = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TASK_PARTITION_SIZE)
  val sinkTaskUseRepartition = settings.getProperty(ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION, ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION_DEFAULT.toString).toBoolean
  val dorisStreamLoader: DorisStreamLoad = CachedDorisStreamLoadClient.getOrCreate(settings)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logger.info(s"Skipping already committed batch $batchId")
    } else {
      write(data.queryExecution)
      latestBatchId = batchId
    }
  }

  def write(queryExecution: QueryExecution): Unit = {
    val schema = queryExecution.analyzed.output
    var resultRdd = queryExecution.toRdd
    if (Objects.nonNull(sinkTaskPartitionSize)) {
      resultRdd = if (sinkTaskUseRepartition) resultRdd.repartition(sinkTaskPartitionSize) else resultRdd.coalesce(sinkTaskPartitionSize)
    }
    // write for each partition
    resultRdd.foreachPartition(iter => {
      val objectMapper = new ObjectMapper()
      val rowArray = objectMapper.createArrayNode()
      iter.foreach(row => {
        val rowNode = objectMapper.createObjectNode()
        for (i <- 0 until row.numFields) {
          val colName = schema(i).name
          val value = row.copy().getUTF8String(i)
          if (value == null) {
            rowNode.putNull(colName)
          } else {
            rowNode.put(colName, value.toString)
          }
        }
        rowArray.add(rowNode)
        if (rowArray.size > maxRowCount - 1) {
          flush
        }
      })
      // flush buffer
      if (!rowArray.isEmpty) {
        flush
      }

      /**
       * flush data to Doris and do retry when flush error
       *
       */
      def flush = {
        val loop = new Breaks
        loop.breakable {

          for (i <- 0 to maxRetryTimes) {
            try {
              dorisStreamLoader.load(rowArray.toString)
              rowArray.removeAll()
              loop.break()
            }
            catch {
              case e: Exception =>
                try {
                  logger.debug("Failed to load data on BE: {} node ", dorisStreamLoader.getLoadUrlStr)
                  //If the current BE node fails to execute Stream Load, randomly switch to other BE nodes and try again
                  dorisStreamLoader.setHostPort(RestService.randomBackendV2(settings, logger))
                  Thread.sleep(1000 * i)
                } catch {
                  case ex: InterruptedException =>
                    logger.warn("Data that failed to load : " + rowArray.toString)
                    Thread.currentThread.interrupt()
                    throw new IOException("unable to flush; interrupted while doing another attempt", e)
                }
            }
          }

          if (!rowArray.isEmpty) {
            logger.warn("Data that failed to load : " + rowArray.toString)
            throw new IOException(s"Failed to load data on BE: ${dorisStreamLoader.getLoadUrlStr} node and exceeded the max retry times.")
          }
        }
      }
    })
  }

  override def toString: String = "DorisStreamLoadSink"
}
