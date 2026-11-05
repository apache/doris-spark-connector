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
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util
import scala.collection.JavaConverters._

class DorisTableBaseTest {

  @Test
  def readPreservesConfiguredIdentifierQuoting(): Unit = {
    val tableIdentifier = "`hive`.`db`.`order`"
    val config = DorisConfig.fromMap(Map(
      "doris.fenodes" -> "127.0.0.1:8030",
      "doris.table.identifier" -> tableIdentifier,
      "doris.user" -> "root",
      "doris.password" -> "").asJava, false)
    val table = new DorisTableBase(
      Identifier.of(Array("hive", "db"), "order"), config, Some(StructType(Nil))) {
      override protected def createScanBuilder(readConfig: DorisConfig, schema: StructType): ScanBuilder = {
        assertEquals(tableIdentifier, readConfig.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER))
        null
      }

      override protected def createWriteBuilder(writeConfig: DorisConfig, schema: StructType): WriteBuilder =
        throw new UnsupportedOperationException()
    }

    table.newScanBuilder(new CaseInsensitiveStringMap(util.Collections.emptyMap[String, String]()))
  }
}
