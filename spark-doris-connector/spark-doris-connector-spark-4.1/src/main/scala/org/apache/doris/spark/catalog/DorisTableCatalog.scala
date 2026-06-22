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

import org.apache.doris.spark.config.DorisConfig
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.types.StructType

class DorisTableCatalog extends DorisTableCatalogBase {
  override def dropNamespace(strings: Array[String], b: Boolean): Boolean = throw new UnsupportedOperationException()
  override def newTableInstance(identifier: Identifier, config: DorisConfig, schema: Option[StructType]): Table =
    new DorisTable(identifier, config, schema)
}
