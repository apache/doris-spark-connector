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

package org.apache.doris.spark.writer

import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.listener.DorisTransactionListener
import org.apache.doris.spark.load.{CachedDorisStreamLoadClient, DorisStreamLoad}
import org.apache.doris.spark.sql.Utils
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.time.Duration
import java.util
import java.util.Objects
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class DorisWriter(settings: SparkSettings) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisWriter])

  val batchSize: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_SIZE,
    ConfigurationOptions.SINK_BATCH_SIZE_DEFAULT)
  private val maxRetryTimes: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_MAX_RETRIES,
    ConfigurationOptions.SINK_MAX_RETRIES_DEFAULT)
  private val sinkTaskPartitionSize: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TASK_PARTITION_SIZE)
  private val sinkTaskUseRepartition: Boolean = settings.getProperty(ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION,
    ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION_DEFAULT.toString).toBoolean
  private val batchInterValMs: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS,
    ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS_DEFAULT)

  private val enable2PC: Boolean = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_ENABLE_2PC,
    ConfigurationOptions.DORIS_SINK_ENABLE_2PC_DEFAULT);

  private val dorisStreamLoader: DorisStreamLoad = CachedDorisStreamLoadClient.getOrCreate(settings)

  def write(dataFrame: DataFrame): Unit = {

    val sc = dataFrame.sqlContext.sparkContext
    val successTxnAcc = sc.collectionAccumulator[Int]("successTxnAcc")
    if (enable2PC) {
      sc.addSparkListener(new DorisTransactionListener(successTxnAcc, dorisStreamLoader))
    }

    var resultRdd = dataFrame.rdd
    val dfColumns = dataFrame.columns
    if (Objects.nonNull(sinkTaskPartitionSize)) {
      resultRdd = if (sinkTaskUseRepartition) resultRdd.repartition(sinkTaskPartitionSize) else resultRdd.coalesce(sinkTaskPartitionSize)
    }
    resultRdd
      .map(_.toSeq.map(_.asInstanceOf[AnyRef]).toList.asJava)
      .foreachPartition(partition => {
        partition
          .grouped(batchSize)
          .foreach(batch => flush(batch, dfColumns))
      })

    /**
     * flush data to Doris and do retry when flush error
     *
     */
    def flush(batch: Iterable[util.List[Object]], dfColumns: Array[String]): Unit = {
      Utils.retry[util.List[Integer], Exception](maxRetryTimes, Duration.ofMillis(batchInterValMs.toLong), logger) {
        dorisStreamLoader.loadV2(batch.toList.asJava, dfColumns, enable2PC)
      } match {
        case Success(txnIds) => if (enable2PC) txnIds.forEach(txnId => successTxnAcc.add(txnId))
        case Failure(e) =>
          throw new IOException(
            s"Failed to load batch data on BE: ${dorisStreamLoader.getLoadUrlStr} node and exceeded the max ${maxRetryTimes} retry times.", e)
      }
    }

  }


}
