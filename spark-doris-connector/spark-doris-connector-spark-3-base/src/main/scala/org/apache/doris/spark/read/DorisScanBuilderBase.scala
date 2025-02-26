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
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

protected[spark] abstract class DorisScanBuilderBase(config: DorisConfig, schema: StructType) extends ScanBuilder
  with SupportsPushDownRequiredColumns {

  protected var readSchema: StructType = {
    if (config.contains(DorisOptions.DORIS_READ_FIELDS)) {
      val dorisReadFields = config.getValue(DorisOptions.DORIS_READ_FIELDS).split(",").map(_.trim.replaceAll("`", ""))
      doPruneColumns(schema, dorisReadFields)
    } else {
      schema
    }
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    doPruneColumns(readSchema, requiredSchema.fieldNames)
  }

  private def doPruneColumns(originSchema: StructType, requiredCols: Array[String]): StructType = {
    if (requiredCols.nonEmpty) {
      val fields = originSchema.fields.filter(
        field => requiredCols.contains(field.name)
      )
      if (fields.isEmpty) {
        throw new IllegalArgumentException("No required columns found")
      }
      StructType(fields)
    } else originSchema
  }

}
