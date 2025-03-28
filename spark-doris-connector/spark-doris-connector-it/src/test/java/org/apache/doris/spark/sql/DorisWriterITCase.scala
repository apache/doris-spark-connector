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
import org.apache.doris.spark.rest.models.DataModel
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

/**
 * it case for doris writer.
 */
class DorisWriterITCase extends AbstractContainerTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[DorisWriterITCase])

  val DATABASE: String = "test_doris_write"
  val TABLE_CSV: String = "tbl_csv"
  val TABLE_CSV_HIDE_SEP: String = "tbl_csv_hide_sep"
  val TABLE_GROUP_COMMIT: String = "tbl_group_commit"
  val TABLE_JSON: String = "tbl_json"
  val TABLE_JSON_EMPTY_PARTITION: String = "tbl_json_empty_partition"
  val TABLE_JSON_TBL: String = "tbl_json_tbl"
  val TABLE_JSON_TBL_OVERWRITE: String = "tbl_json_tbl_overwrite"
  val TABLE_JSON_TBL_ARROW: String = "tbl_json_tbl_arrow"

  @Test
  @throws[Exception]
  def testSinkCsvFormat(): Unit = {
    initializeTable(TABLE_CSV, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_csv", 1),
      ("spark_csv", 2)
    )).toDF("name", "age")
    df.write
      .format("doris")
      .option("doris.fenodes", getFenodes)
      .option("doris.sink.auto-redirect", false)
      .option("doris.table.identifier", DATABASE + "." + TABLE_CSV)
      .option("user", getDorisUsername)
      .option("password", getDorisPassword)
      .option("sink.properties.column_separator", ",")
      .option("sink.properties.line_delimiter", "\n")
      .option("sink.properties.format", "csv")
      .option("doris.sink.batch.interval.ms", "5000")
      .option("doris.sink.batch.size", "1")
      .mode(SaveMode.Append)
      .save()
    session.stop()

    Thread.sleep(15000)
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
  def testSinkCsvFormatHideSep(): Unit = {
    initializeTable(TABLE_CSV_HIDE_SEP, DataModel.AGGREGATE)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_csv", 1),
      ("spark_csv", 2)
    )).toDF("name", "age")
    df.write
      .format("doris")
      .option("doris.fenodes", getFenodes + "," + getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_CSV_HIDE_SEP)
      .option("user", getDorisUsername)
      .option("password", getDorisPassword)
      .option("sink.properties.column_separator", "\\x01")
      .option("sink.properties.line_delimiter", "\\x02")
      .option("sink.properties.format", "csv")
      .mode(SaveMode.Append)
      .save()
    session.stop()

    Thread.sleep(10000)
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_CSV_HIDE_SEP),
      2)
    val expected = util.Arrays.asList("doris_csv,1", "spark_csv,2")
    checkResultInAnyOrder("testSinkCsvFormatHideSep", expected.toArray(), actual.toArray)
  }

  @Test
  @throws[Exception]
  def testSinkGroupCommit(): Unit = {
    initializeTable(TABLE_GROUP_COMMIT, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_csv", 1),
      ("spark_csv", 2)
    )).toDF("name", "age")
    df.write
      .format("doris")
      .option("doris.fenodes", getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_GROUP_COMMIT)
      .option("user", getDorisUsername)
      .option("password", getDorisPassword)
      .option("sink.properties.group_commit", "sync_mode")
      .mode(SaveMode.Append)
      .save()
    session.stop()

    Thread.sleep(10000)
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_GROUP_COMMIT),
      2)
    val expected = util.Arrays.asList("doris_csv,1", "spark_csv,2")
    checkResultInAnyOrder("testSinkGroupCommit", expected.toArray(), actual.toArray)
  }

  @Test
  @throws[Exception]
  def testSinkEmptyPartition(): Unit = {
    initializeTable(TABLE_JSON_EMPTY_PARTITION, DataModel.AGGREGATE)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_json", 1)
    )).toDF("name", "age")
    df.repartition(2).write
      .format("doris")
      .option("doris.fenodes", getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_JSON_EMPTY_PARTITION)
      .option("user", getDorisUsername)
      .option("password", getDorisPassword)
      .option("sink.properties.read_json_by_line", "true")
      .option("sink.properties.format", "json")
      .option("doris.sink.auto-redirect", "false")
      .option("doris.sink.enable-2pc", "true")
      .mode(SaveMode.Append)
      .save()
    session.stop()

    Thread.sleep(10000)
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_JSON_EMPTY_PARTITION),
      2)
    val expected = util.Arrays.asList("doris_json,1");
    checkResultInAnyOrder("testSinkEmptyPartition", expected.toArray, actual.toArray)
  }

  @Test
  @throws[Exception]
  def testSinkArrowFormat(): Unit = {
    initializeTable(TABLE_JSON_TBL_ARROW, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris_json", 1),
      ("spark_json", 2)
    )).toDF("name", "age")
    df.write
      .format("doris")
      .option("doris.fenodes", getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_JSON_TBL_ARROW)
      .option("user", getDorisUsername)
      .option("password", getDorisPassword)
      .option("sink.properties.format", "arrow")
      .option("doris.sink.batch.size", "1")
      .option("doris.sink.enable-2pc", "true")
      .mode(SaveMode.Append)
      .save()
    session.stop()

    Thread.sleep(10000)
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_JSON_TBL_ARROW),
      2)
    val expected = util.Arrays.asList("doris_json,1", "spark_json,2");
    checkResultInAnyOrder("testSinkArrowFormat", expected.toArray, actual.toArray)
  }

  @Test
  @throws[Exception]
  def testSinkJsonFormat(): Unit = {
    initializeTable(TABLE_JSON, DataModel.UNIQUE)
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
    initializeTable(TABLE_JSON_TBL, DataModel.UNIQUE_MOR)
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

  @Test
  @throws[Exception]
  def testSQLSinkOverwrite(): Unit = {
    initializeTable(TABLE_JSON_TBL_OVERWRITE, DataModel.DUPLICATE)
    // init history data
    ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("insert into %s.%s  values ('history-doris',1118)", DATABASE, TABLE_JSON_TBL_OVERWRITE),
      String.format("insert into %s.%s  values ('history-spark',1110)", DATABASE, TABLE_JSON_TBL_OVERWRITE))

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
         | "table.identifier"="${DATABASE + "." + TABLE_JSON_TBL_OVERWRITE}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}",
         | "doris.query.port"="${getQueryPort}",
         | "doris.sink.label.prefix"="doris-label-customer",
         | "doris.sink.enable-2pc"="true"
         |)
         |""".stripMargin)
    session.sql(
      """
        |insert overwrite table test_sink select  name,age from mock_source
        |""".stripMargin)
    session.stop()

    Thread.sleep(10000)
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_JSON_TBL_OVERWRITE),
      2)
    val expected = util.Arrays.asList("doris_tbl,1", "spark_tbl,2");
    checkResultInAnyOrder("testSQLSinkOverwrite", expected.toArray, actual.toArray)
  }

  private def initializeTable(table: String, dataModel: DataModel): Unit = {
    val max = if (DataModel.AGGREGATE == dataModel) "MAX" else ""
    val morProps = if (!(DataModel.UNIQUE_MOR == dataModel)) "" else ",\"enable_unique_key_merge_on_write\" = \"false\""
    val model = if (dataModel == DataModel.UNIQUE_MOR) DataModel.UNIQUE.toString else dataModel.toString
    ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
      String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
      String.format("CREATE TABLE %s.%s ( \n"
        + "`name` varchar(256),\n"
        + "`age` int %s\n"
        + ") "
        + " %s KEY(`name`) "
        + " DISTRIBUTED BY HASH(`name`) BUCKETS 1\n"
        + "PROPERTIES ("
        + "\"replication_num\" = \"1\"\n" + morProps + ")", DATABASE, table, max, model))
  }

  private def checkResultInAnyOrder(testName: String, expected: Array[AnyRef], actual: Array[AnyRef]): Unit = {
    LOG.info("Checking DorisWriterFailoverITCase result. testName={}, actual={}, expected={}", testName, actual, expected)
    assertEqualsInAnyOrder(expected.toList.asJava, actual.toList.asJava)
  }
}
