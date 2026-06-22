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

import org.apache.doris.spark.client.DorisFrontendClient
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.types.StructType

class DorisWriteBuilder(config: DorisConfig, schema: StructType) extends WriteBuilder with SupportsTruncate {

  private var isTruncate = false

  override def buildForBatch(): BatchWrite = {
    if (isTruncate) {
      val client = new DorisFrontendClient(config)
      val tableDb = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER).split("\\.")
      client.truncateTable(tableDb(0), tableDb(1))
    }
    new DorisWrite(config, schema)
  }

  override def buildForStreaming(): StreamingWrite = {
    new DorisWrite(config, schema)
  }

  override def truncate(): WriteBuilder = {
    isTruncate = true
    this
  }
}
