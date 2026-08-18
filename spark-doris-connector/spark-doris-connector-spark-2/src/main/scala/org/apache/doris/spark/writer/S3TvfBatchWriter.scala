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

import org.apache.doris.spark.client.write.tvf.{
  S3ClientObjectStore,
  S3TvfColumnUtils,
  S3TvfCommitter,
  S3TvfWriter
}
import org.apache.doris.spark.config.{DorisConfig, DorisOptions, S3TvfOptions}
import org.apache.spark.sql.DataFrame

/** Spark 2 batch adapter for the shared S3 TVF writer and committer. */
class S3TvfBatchWriter(config: DorisConfig) extends Serializable {

  def write(dataFrame: DataFrame): Unit = {
    val resultDataFrame = repartition(dataFrame)
    val schema = resultDataFrame.schema
    val options = S3TvfOptions.fromConfig(config)
    val tableIdentifier = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER).split("\\.")
    val database = tableIdentifier(0).replaceAll("`", "").trim
    val table = tableIdentifier(1).replaceAll("`", "").trim
    val labelPrefix = config.getValue(DorisOptions.DORIS_SINK_LABEL_PREFIX).trim
    val columns = S3TvfColumnUtils.resolveColumns(config.getSinkProperties, schema)
    val batchSize = config.getValue(DorisOptions.DORIS_SINK_BATCH_SIZE)

    resultDataFrame.queryExecution.toRdd
      .mapPartitionsWithIndex { case (partitionId, records) =>
        val writer = new S3TvfWriter(
          options,
          database,
          table,
          labelPrefix,
          schema,
          columns,
          partitionId,
          batchSize,
          new S3ClientObjectStore(options))
        try {
          records.foreach(writer.write)
          val committable = writer.prepareCommit()
          if (!committable.isEmpty) {
            val committer = new S3TvfCommitter(config)
            try {
              committer.commit(committable)
            } finally {
              committer.close()
            }
          }
          Iterator.single(())
        } finally {
          writer.close()
        }
      }
      .count()
  }

  private def repartition(dataFrame: DataFrame): DataFrame = {
    if (!config.contains(DorisOptions.DORIS_SINK_TASK_PARTITION_SIZE)) {
      dataFrame
    } else {
      val partitionSize = config.getValue(DorisOptions.DORIS_SINK_TASK_PARTITION_SIZE)
      if (config.getValue(DorisOptions.DORIS_SINK_TASK_USE_REPARTITION)) {
        dataFrame.repartition(partitionSize)
      } else {
        dataFrame.coalesce(partitionSize)
      }
    }
  }
}
