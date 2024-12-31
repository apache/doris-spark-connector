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
import org.apache.doris.spark.util.DorisDialects
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

class DorisScan(config: DorisConfig, schema: StructType, filters: Array[Filter]) extends AbstractDorisScan(config, schema) with Logging {
  override protected def compiledFilters(): Array[String] = {
    val inValueLengthLimit = config.getValue(DorisOptions.DORIS_FILTER_QUERY_IN_MAX_COUNT)
    filters.map(DorisDialects.compileFilter(_, inValueLengthLimit)).filter(_.isDefined).map(_.get)
  }
}
