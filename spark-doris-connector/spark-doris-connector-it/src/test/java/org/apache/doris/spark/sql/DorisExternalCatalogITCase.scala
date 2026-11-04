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

import org.apache.doris.spark.container.AbstractMySqlCatalogTestBase
import org.apache.doris.spark.container.AbstractMySqlCatalogTestBase.renderOptions
import org.apache.doris.spark.sparkContextFunctions
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

/** Covers each Spark read API against a real MySQL-backed external catalog. */
class DorisExternalCatalogITCase extends AbstractMySqlCatalogTestBase {

  private var session: SparkSession = _

  @Before
  def startSpark(): Unit = {
    session = SparkSession.builder()
      .appName("doris-external-catalog-it")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  @After
  def stopSpark(): Unit = {
    if (session != null) session.stop()
  }

  @Test
  def testDataFrameRead(): Unit = {
    // Leave read mode and Flight port unset to exercise the default Arrow path.
    val data = session.read.format("doris").options(connectorOptions("order").asScala).load()
    assertEquals(1, data.rdd.getNumPartitions)
    assertEquals(Seq(Row(1, "alpha", 10), Row(2, "beta", 20), Row(3, null, null)),
      data.orderBy("id").collect().toSeq)
    assertEquals(Seq(Row("beta", 2), Row(null, 3)),
      data.select("name", "id").where("id >= 2").orderBy("id").collect().toSeq)
    assertEquals(1, data.limit(1).collect().length)
  }

  @Test
  def testSparkSqlRead(): Unit = {
    session.sql("CREATE TEMPORARY VIEW external_catalog_source USING doris OPTIONS(" +
      renderOptions(connectorOptions("order")) + ")")
    assertEquals(Seq(Row("beta", 2), Row(null, 3)), session.sql(
      "SELECT name,id FROM external_catalog_source WHERE id >= 2 ORDER BY id").collect().toSeq)
  }

  @Test
  def testRddRead(): Unit = {
    val options = connectorOptions("order").asScala.toMap
    val rows = session.sparkContext.dorisRDD(
      tableIdentifier = Some(options("doris.table.identifier")),
      query = Some("id >= 2"),
      cfg = Some(options + ("doris.read.fields" -> "name,id")))
      .collect().map(_.asInstanceOf[Array[AnyRef]].toSeq)
      .sortBy(_(1).asInstanceOf[java.lang.Number].intValue()).toSeq
    assertEquals(Seq(Seq("beta", 2), Seq(null, 3)), rows)
  }

  @Test
  def testEmptyTableRead(): Unit = {
    assertEquals(0L, session.read.format("doris")
      .options(connectorOptions("empty_records").asScala).load().count())
  }
}
