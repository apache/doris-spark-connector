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

import org.apache.doris.spark.client.DorisFrontendClient
import org.apache.doris.spark.config.DorisConfig
import org.apache.doris.spark.read.DorisScanBuilder
import org.apache.doris.spark.rest.models.Schema
import org.apache.doris.spark.util.SchemaConvertors
import org.apache.doris.spark.write.DorisWriteBuilder
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._
import scala.language.implicitConversions


class DorisTable(identifier: Identifier, config: DorisConfig, schema: Option[StructType]) extends Table with SupportsRead with SupportsWrite {

  private lazy val frontend:DorisFrontendClient = new DorisFrontendClient(config)

  override def name(): String = identifier.toString

  override def schema(): StructType = schema.getOrElse({
    val dorisSchema = frontend.getTableSchema(identifier.namespace()(0), identifier.name())
    dorisSchema
  })

  override def capabilities(): util.Set[TableCapability] = {
    Set(BATCH_READ,
      BATCH_WRITE,
      STREAMING_WRITE).asJava
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    new DorisScanBuilder(config: DorisConfig, schema())
  }

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = {
    new DorisWriteBuilder(config, logicalWriteInfo.schema())
  }

  private implicit def dorisSchemaToStructType(dorisSchema: Schema): StructType = {
    StructType(dorisSchema.getProperties.asScala.map(field => {
      StructField(field.getName, SchemaConvertors.toCatalystType(field.getType, field.getPrecision, field.getScale))
    }))
  }

}
