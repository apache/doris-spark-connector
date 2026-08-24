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

import org.apache.doris.spark.client.write.tvf.{S3ClientObjectStore, S3TvfCommitter, S3TvfWriter}
import org.apache.doris.spark.config.{DorisConfig, S3TvfOptions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

private[write] class S3TvfDataWriter(
    config: DorisConfig,
    options: S3TvfOptions,
    database: String,
    table: String,
    labelPrefix: String,
    schema: StructType,
    columns: java.util.List[String],
    partitionId: Int,
    batchSize: Int) extends DataWriter[InternalRow] {

  private val writer = new S3TvfWriter(
    options,
    database,
    table,
    labelPrefix,
    schema,
    columns,
    partitionId,
    batchSize,
    new S3ClientObjectStore(options))
  private var closed = false

  override def write(record: InternalRow): Unit = writer.write(record)

  override def commit(): WriterCommitMessage = {
    val committable = writer.prepareCommit()
    if (!committable.isEmpty) {
      val committer = new S3TvfCommitter(config)
      try {
        committer.commit(committable)
      } finally {
        committer.close()
      }
    }
    S3TvfWriterCommitMessage
  }

  override def abort(): Unit = close()

  override def close(): Unit = {
    if (!closed) {
      writer.close()
      closed = true
    }
  }
}

private[write] case object S3TvfWriterCommitMessage extends WriterCommitMessage
