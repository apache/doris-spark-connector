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

package org.apache.doris.spark.rdd

import org.apache.doris.spark.client.entity.DorisReaderPartition
import org.apache.doris.spark.client.read.{DorisFlightSqlReader, DorisThriftReader}
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

private[spark] class DorisRDD[T: ClassTag](
    sc: SparkContext,
    params: Map[String, String] = Map.empty)
    extends AbstractDorisRDD[T](sc, params) {
  override def compute(split: Partition, context: TaskContext): ScalaDorisRDDIterator[T] = {
    new ScalaDorisRDDIterator(context, split.asInstanceOf[DorisPartition].dorisPartition)
  }
}

private[spark] class ScalaDorisRDDIterator[T](
                                               context: TaskContext,
                                               partition: DorisReaderPartition)
  extends AbstractDorisRDDIterator[T](context, partition) {

  override def initReader(config: DorisConfig): Unit = {
    config.getValue(DorisOptions.READ_MODE).toLowerCase match {
      case "thrift" => config.setProperty(DorisOptions.DORIS_VALUE_READER_CLASS, classOf[DorisThriftReader].getName)
      case "arrow" => config.setProperty(DorisOptions.DORIS_VALUE_READER_CLASS, classOf[DorisFlightSqlReader].getName)
      case rm: String => throw new IllegalArgumentException("Unknown read mode: " + rm)
    }
  }

  override def createValue(value: Object): T = {
    value.asInstanceOf[T]
  }
}
