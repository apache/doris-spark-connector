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

import org.apache.doris.spark.container.AbstractContainerTestBase.{assertEqualsInAnyOrder, getDorisQueryConnection}
import org.apache.doris.spark.container.{AbstractContainerTestBase, ContainerUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._
class DorisWriterITCase extends AbstractContainerTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[DorisReaderITCase])

  val DATABASE: String = "test"
  val TABLE_CSV: String = "tbl_csv"
  val TABLE_JSON: String = "tbl_json"
  val TABLE_JSON_TBL: String = "tbl_json_tbl"

  @Test
  @throws[Exception]
  def testSinkCsvFormat(): Unit = {
    initializeTable(TABLE_CSV)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_csv", 1),
      ("spark_csv", 2)
    )).toDF("name", "age")
    df.write
      .format("doris")
      .option("doris.fenodes", getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_CSV)
      .option("user", getDorisUsername)
      .option("password", getDorisPassword)
      .option("sink.properties.column_separator", ",")
      .option("sink.properties.line_delimiter", "\n")
      .option("sink.properties.format", "csv")
      .mode(SaveMode.Append)
      .save()
    session.stop()

    Thread.sleep(10000)
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_CSV),
      2)
    val expected = util.Arrays.asList("doris_csv,1", "spark_csv,2")
    checkResultInAnyOrder("testSinkCsvFormat", expected.toArray(), actual.toArray)
  }

  @Test
  @throws[Exception]
  def testSinkJsonFormat(): Unit = {
    initializeTable(TABLE_JSON)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_json", 1),
      ("spark_json", 2)
    )).toDF("name", "age")
    df.write
      .format("doris")
      .option("doris.fenodes", getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_JSON)
      .option("user", getDorisUsername)
      .option("password", getDorisPassword)
      .option("sink.properties.read_json_by_line", "true")
      .option("sink.properties.format", "json")
      .option("doris.sink.auto-redirect", "false")
      .mode(SaveMode.Append)
      .save()
    session.stop()

    Thread.sleep(10000)
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_JSON),
      2)
    val expected = util.Arrays.asList("doris_json,1", "spark_json,2");
    checkResultInAnyOrder("testSinkJsonFormat", expected.toArray, actual.toArray)
  }

  @Test
  @throws[Exception]
  def testSQLSinkFormat(): Unit = {
    initializeTable(TABLE_JSON_TBL)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_tbl", 1),
      ("spark_tbl", 2)
    )).toDF("name", "age")
    df.createTempView("mock_source")
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_sink
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_JSON_TBL}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}"
         |)
         |""".stripMargin)
    session.sql(
      """
        |insert into test_sink select  name,age from mock_source
        |""".stripMargin)
    session.stop()

    Thread.sleep(10000)
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_JSON_TBL),
      2)
    val expected = util.Arrays.asList("doris_tbl,1", "spark_tbl,2");
    checkResultInAnyOrder("testSQLSinkFormat", expected.toArray, actual.toArray)
  }


  @throws[Exception]
  private def initializeTable(table: String): Unit = {
    ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
      String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
      String.format(
        "CREATE TABLE %s.%s ( \n" + "`name` varchar(256),\n" + "`age` int\n" + ") " +
          "DISTRIBUTED BY HASH(`name`) BUCKETS 1\n" +
          "PROPERTIES (\n" +
          "\"replication_num\" = \"1\"\n" + ")\n", DATABASE, table)
    )
  }

  private def checkResultInAnyOrder(testName: String, expected: Array[AnyRef], actual: Array[AnyRef]): Unit = {
    LOG.info("Checking DorisSourceITCase result. testName={}, actual={}, expected={}", testName, actual, expected)
    assertEqualsInAnyOrder(expected.toList.asJava, actual.toList.asJava)
  }

}
