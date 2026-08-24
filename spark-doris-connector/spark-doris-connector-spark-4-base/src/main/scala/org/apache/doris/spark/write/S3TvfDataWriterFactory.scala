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

package org.apache.doris.spark.write

import org.apache.doris.spark.client.write.tvf.S3TvfColumnUtils
import org.apache.doris.spark.config.{DorisConfig, DorisOptions, S3TvfOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

private[write] class S3TvfDataWriterFactory(
    config: DorisConfig,
    schema: StructType) extends DataWriterFactory {

  private val options = S3TvfOptions.fromConfig(config)
  private val tableIdentifier = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER).split("\\.")
  private val database = tableIdentifier(0).replaceAll("`", "").trim
  private val table = tableIdentifier(1).replaceAll("`", "").trim
  private val labelPrefix = config.getValue(DorisOptions.DORIS_SINK_LABEL_PREFIX).trim
  private val columns = S3TvfColumnUtils.resolveColumns(config.getSinkProperties, schema)
  private val batchSize = config.getValue(DorisOptions.DORIS_SINK_BATCH_SIZE)

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new S3TvfDataWriter(
      config,
      options,
      database,
      table,
      labelPrefix,
      schema,
      columns,
      partitionId,
      batchSize)
  }
}
