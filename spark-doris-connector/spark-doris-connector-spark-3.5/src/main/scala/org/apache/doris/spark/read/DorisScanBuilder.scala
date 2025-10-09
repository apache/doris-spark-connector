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

package org.apache.doris.spark.read

import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.read.expression.V2ExpressionBuilder
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownLimit, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

class DorisScanBuilder(config: DorisConfig, schema: StructType) extends DorisScanBuilderBase(config, schema)
  with SupportsPushDownV2Filters
  with SupportsPushDownLimit {

  private var pushDownPredicates: Array[Predicate] = Array[Predicate]()

  private val expressionBuilder = new V2ExpressionBuilder(config.getValue(DorisOptions.DORIS_FILTER_QUERY_IN_MAX_COUNT))

  private var limitSize: Int = -1

  override def build(): Scan = new DorisScanV2(config, schema, pushDownPredicates, limitSize)

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (pushed, unsupported) = predicates.partition(predicate => {
      expressionBuilder.buildOpt(predicate).isDefined
    })
    this.pushDownPredicates = pushed
    unsupported
  }

  override def pushedPredicates(): Array[Predicate] = pushDownPredicates

  override def pushLimit(i: Int): Boolean = {
    limitSize = i
    true
  }

}
