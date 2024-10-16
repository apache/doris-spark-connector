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

import org.apache.doris.spark.client.DorisReaderPartition
import org.apache.doris.spark.config.{DorisConfig, DorisConfigOptions}
import org.apache.doris.spark.rdd.{AbstractDorisRDD, AbstractDorisRDDIterator, DorisPartition}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkContext, TaskContext}

private[spark] class ScalaDorisRowRDD(sc: SparkContext, params: Map[String, String] = Map.empty,
                                      schema: StructType)
  extends AbstractDorisRDD[Row](sc, params) {

  override def compute(split: Partition, context: TaskContext): ScalaDorisRowRDDIterator = {
    new ScalaDorisRowRDDIterator(dorisCfg, context, split.asInstanceOf[DorisPartition].dorisPartition, schema)
  }
}

private[spark] class ScalaDorisRowRDDIterator(config: DorisConfig, context: TaskContext,
                                              partition: DorisReaderPartition, schema: StructType)
  extends AbstractDorisRDDIterator[Row](config, context, partition) {

  override def initReader(config: DorisConfig): Unit = {
    config.setProperty(DorisConfigOptions.DORIS_READ_FIELDS, schema.map(f => s"`${f.name}``").mkString(","))
    config.setProperty(DorisConfigOptions.DORIS_VALUE_READER_CLASS, classOf[DorisRowThriftReader].getName)
  }

  override def createValue(value: Object): Row = {
    value.asInstanceOf[ScalaDorisRow]
  }
}