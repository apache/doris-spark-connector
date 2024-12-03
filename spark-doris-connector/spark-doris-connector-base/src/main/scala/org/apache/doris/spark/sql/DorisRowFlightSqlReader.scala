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

import org.apache.doris.spark.client.entity.DorisReaderPartition
import org.apache.doris.spark.client.read.DorisFlightSqlReader
import org.apache.doris.spark.config.DorisOptions
import org.apache.doris.spark.exception.ShouldNeverHappenException

import scala.collection.JavaConverters.asScalaBufferConverter

class DorisRowFlightSqlReader(partition: DorisReaderPartition) extends DorisFlightSqlReader(partition)  {

  private val rowOrder: Seq[String] = config.getValue(DorisOptions.DORIS_READ_FIELDS).split(",")

  override def next(): AnyRef = {
    if (!hasNext) {
      throw new ShouldNeverHappenException
    }
    val row: DorisRow = new DorisRow(rowOrder)
    rowBatch.next.asScala.zipWithIndex.foreach {
      case (s, index) if index < row.values.size => row.values.update(index, s)
      case _ => // nothing
    }
    row
  }
}
