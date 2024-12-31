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

package org.apache.doris.spark.catalog

import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

abstract class DorisTableProviderBase extends TableProvider {

  protected var t: Table = _

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (t == null) t = getTable(options)
    t.schema()
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    if (t != null) t
    else {
      val dorisConfig = DorisConfig.fromMap(properties, false)
      val tableIdentifier = dorisConfig.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER)
      val tableIdentifierArr = tableIdentifier.split("\\.")
      newTableInstance(Identifier.of(Array[String](tableIdentifierArr(0)), tableIdentifierArr(1)), dorisConfig, Some(schema))
    }
  }

  private def getTable(options: CaseInsensitiveStringMap): Table = {
    if (t != null) t
    else {
      val dorisConfig = DorisConfig.fromMap(options, false)
      val tableIdentifier = dorisConfig.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER)
      val tableIdentifierArr = tableIdentifier.split("\\.")
      newTableInstance(Identifier.of(Array[String](tableIdentifierArr(0)), tableIdentifierArr(1)), dorisConfig, None)
    }
  }

  def newTableInstance(identifier: Identifier, config: DorisConfig, schema: Option[StructType]): Table;

}
