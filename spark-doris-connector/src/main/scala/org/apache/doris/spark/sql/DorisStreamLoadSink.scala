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
import org.apache.doris.spark.{CachedDorisStreamLoadClient, DorisStreamLoad}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.time.Duration
import java.util
import java.util.Objects
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

private[sql] class DorisStreamLoadSink(sqlContext: SQLContext, settings: SparkSettings) extends Sink with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisStreamLoadSink].getName)
  @volatile private var latestBatchId = -1L
  val batchSize: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_SIZE, ConfigurationOptions.SINK_BATCH_SIZE_DEFAULT)
  val maxRetryTimes: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_MAX_RETRIES, ConfigurationOptions.SINK_MAX_RETRIES_DEFAULT)
  val sinkTaskPartitionSize = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TASK_PARTITION_SIZE)
  val sinkTaskUseRepartition = settings.getProperty(ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION, ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION_DEFAULT.toString).toBoolean
  val batchInterValMs = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS, ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS_DEFAULT)

  val dorisStreamLoader: DorisStreamLoad = CachedDorisStreamLoadClient.getOrCreate(settings)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logger.info(s"Skipping already committed batch $batchId")
    } else {
      write(data.rdd)
      latestBatchId = batchId
    }
  }

  def write(rdd: RDD[Row]): Unit = {
    var resultRdd = rdd
    if (Objects.nonNull(sinkTaskPartitionSize)) {
      resultRdd = if (sinkTaskUseRepartition) resultRdd.repartition(sinkTaskPartitionSize) else resultRdd.coalesce(sinkTaskPartitionSize)
    }
    resultRdd
      .map(_.toSeq.map(_.asInstanceOf[AnyRef]).toList.asJava)
      .foreachPartition(partition => {
        partition
          .grouped(batchSize)
          .foreach(batch => flush(batch))
      })

    /**
     * flush data to Doris and do retry when flush error
     *
     */
    def flush(batch: Iterable[util.List[Object]]): Unit = {
      Utils.retry[Unit, Exception](maxRetryTimes, Duration.ofMillis(batchInterValMs.toLong), logger) {
        dorisStreamLoader.loadV2(batch.toList.asJava)
      } match {
        case Success(_) =>
        case Failure(e) =>
          throw new IOException(
            s"Failed to load batch data on BE: ${dorisStreamLoader.getLoadUrlStr} node and exceeded the max $maxRetryTimes retry times.", e)
      }
    }
  }

  override def toString: String = "DorisStreamLoadSink"
}
