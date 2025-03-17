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

import org.apache.doris.spark.DorisTestBase
import org.apache.spark.sql.types.{ArrayType, DataTypes, DecimalType, MapType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.junit.Test

import java.sql.{Date, ResultSet, Statement, Timestamp}
import scala.collection.mutable.ListBuffer

class DorisWriterITCase extends DorisTestBase {

  val DATABASE: String = "test"
  val TABLE_CSV: String = "tbl_csv"
  val TABLE_JSON: String = "tbl_json"
  val TABLE_JSON_TBL: String = "tbl_json_tbl"
  val TABLE_ALL_TYPE_TBL: String = "tbl_all_type_tbl"

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
      .option("doris.fenodes", DorisTestBase.getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_CSV)
      .option("user", DorisTestBase.USERNAME)
      .option("password", DorisTestBase.PASSWORD)
      .option("sink.properties.column_separator", ",")
      .option("sink.properties.line_delimiter", "\n")
      .option("sink.properties.format", "csv")
      .mode(SaveMode.Append)
      .save()
    session.stop()

    Thread.sleep(10000)
    val actual = queryResult(TABLE_CSV);
    val expected = ListBuffer(List("doris_csv", 1), List("spark_csv", 2))
    assert(expected.equals(actual))
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
      .option("doris.fenodes", DorisTestBase.getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_JSON)
      .option("user", DorisTestBase.USERNAME)
      .option("password", DorisTestBase.PASSWORD)
      .option("sink.properties.read_json_by_line", "true")
      .option("sink.properties.format", "json")
      .option("doris.sink.auto-redirect", "false")
      .mode(SaveMode.Append)
      .save()
    session.stop()

    Thread.sleep(10000)
    val actual = queryResult(TABLE_JSON);
    val expected = ListBuffer(List("doris_json", 1), List("spark_json", 2))
    assert(expected.equals(actual))
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
         | "fenodes"="${DorisTestBase.getFenodes}",
         | "user"="${DorisTestBase.USERNAME}",
         | "password"="${DorisTestBase.PASSWORD}"
         |)
         |""".stripMargin)
    session.sql(
      """
        |insert into test_sink select  name,age from mock_source
        |""".stripMargin)
    session.stop()

    Thread.sleep(10000)
    val actual = queryResult(TABLE_JSON_TBL);
    val expected = ListBuffer(List("doris_tbl", 1), List("spark_tbl", 2))
    assert(expected.equals(actual))
  }

  // @Test
  @throws[Exception]
  def testAllTypeCsvSink(): Unit = {
    initializeAllTypeTable(TABLE_ALL_TYPE_TBL)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("c0", DataTypes.BooleanType),
      StructField("c1", DataTypes.ShortType),
      StructField("c2", DataTypes.ShortType),
      StructField("c3", DataTypes.IntegerType),
      StructField("c4", DataTypes.LongType),
      StructField("c5", DataTypes.StringType),
      StructField("c6", DataTypes.FloatType),
      StructField("c7", DataTypes.DoubleType),
      StructField("c8", DecimalType(20, 4)),
      StructField("c9", DataTypes.DateType),
      StructField("c10", DataTypes.TimestampType),
      StructField("c11", DataTypes.StringType),
      StructField("c12", DataTypes.StringType),
      StructField("c13", DataTypes.StringType),
      StructField("c14", ArrayType(DataTypes.IntegerType)),
      StructField("c15", MapType(DataTypes.StringType, DataTypes.IntegerType)),
      StructField("c16", StructType(Seq(StructField("name", DataTypes.StringType), StructField("id", DataTypes.IntegerType)))),
      StructField("c17", DataTypes.StringType),
      StructField("c18", DataTypes.StringType)
    ))
    val rdd = session.sparkContext.parallelize(Seq(
      Row(1, 11, 111, 1111, 11111, 111111, 1111111, 1.1, 1.11, 1.1111, "2025-01-01", "2025-01-01 12:34:56", "abcdefghij",
        "abcdefghij", "abcdefghij", Array(1, 2, 3), Map("a" -> 1, "b" -> 2), Row("a", 1), "{\"a\":1,\"b\":2}", "{\"a\":\"c\",\"b\":2}")
    ))
    val df = session.createDataFrame(rdd, schema)
    df.createTempView("mock_source")
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_sink
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_ALL_TYPE_TBL}",
         | "fenodes"="${DorisTestBase.getFenodes}",
         | "user"="${DorisTestBase.USERNAME}",
         | "password"="${DorisTestBase.PASSWORD}"
         |)
         |""".stripMargin)
    session.sql(
      """
        |insert into test_sink select * from mock_source
        |""".stripMargin)
    session.stop()

    Thread.sleep(10000)
    val actual = queryAllTypeResult(TABLE_ALL_TYPE_TBL);
    val expected = ListBuffer(List(1, 11, 111, 1111, 11111, 111111, "1111111", 1.1.toFloat, 1.11, BigDecimal(1.1111),
      Date.valueOf("2025-01-01"), Timestamp.valueOf("2025-01-01 12:34:56"), "abcdefghij", "abcdefghij", "abcdefghij",
      "[1, 2, 3]", "{\"a\":1, \"b\":2}", "{\"name\":\"a\", \"id\":1}", "{\"a\":1, \"b\":2}", "{\"a\":\"c\", \"b\":2}"))
    assert(expected.equals(actual))
  }

  private def queryResult(table: String): ListBuffer[Any] = {
    val actual = new ListBuffer[Any]
    try {
      val sinkStatement: Statement = DorisTestBase.connection.createStatement
      try {
        val sinkResultSet: ResultSet = sinkStatement.executeQuery(String.format("select name,age from %s.%s order by 1", DATABASE, table))
        while (sinkResultSet.next) {
          val row = List(sinkResultSet.getString("name"), sinkResultSet.getInt("age"))
          actual += row
        }
      } finally if (sinkStatement != null) sinkStatement.close()
    }
    actual
  }

  @throws[Exception]
  private def initializeTable(table: String): Unit = {
    try {
      val statement: Statement = DorisTestBase.connection.createStatement
      try {
        statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE))
        statement.execute(String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table))
        statement.execute(String.format(
          "CREATE TABLE %s.%s ( \n" + "`name` varchar(256),\n" + "`age` int\n" + ") " +
          "DISTRIBUTED BY HASH(`name`) BUCKETS 1\n" +
          "PROPERTIES (\n" +
          "\"replication_num\" = \"1\"\n" + ")\n", DATABASE, table))
      } finally if (statement != null) statement.close()
    }
  }

  @throws[Exception]
  private def initializeAllTypeTable(table: String): Unit = {
    try {
      val statement: Statement = DorisTestBase.connection.createStatement
      try {
        statement.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE))
        statement.execute(String.format("DROP TABLE IF EXISTS %s.%s FORCE", DATABASE, table))
        statement.execute(String.format(
          "CREATE TABLE `%s`.`%s` (`id` int NOT NULL,`c0` boolean NULL,`c1` tinyint NULL,`c2` smallint NULL," +
            "`c3` int NULL,`c4` bigint NULL,`c5` largeint NULL,`c6` float NULL,`c7` double NULL,`c8` decimal(20,4) NULL," +
            "`c9` date NULL,`c10` datetime NULL,`c11` char(10) NULL,`c12` varchar(255) NULL,`c13` string NULL," +
            "`c14` array<int> NULL,`c15` map<string,int> NULL,`c16` struct<name:string,id:int> NULL,`c17` json NULL," +
            "`c18` variant) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 " +
            "PROPERTIES (\"replication_allocation\" = \"tag.location.default: 1\");", DATABASE, table))
      } finally if (statement != null) statement.close()
    }
  }

  private def queryAllTypeResult(table: String): ListBuffer[Any] = {
    val actual = new ListBuffer[Any]
    try {
      val sinkStatement: Statement = DorisTestBase.connection.createStatement
      try {
        val sinkResultSet: ResultSet = sinkStatement.executeQuery(String.format("select * from %s.%s order by 1", DATABASE, table))
        while (sinkResultSet.next) {
          val row = List(
            sinkResultSet.getInt("id"),
            sinkResultSet.getBoolean("c0"),
            sinkResultSet.getInt("c1"),
            sinkResultSet.getInt("c2"),
            sinkResultSet.getInt("c3"),
            sinkResultSet.getLong("c4"),
            sinkResultSet.getString("c5"),
            sinkResultSet.getFloat("c6"),
            sinkResultSet.getDouble("c7"),
            sinkResultSet.getBigDecimal("c8"),
            sinkResultSet.getDate("c9"),
            sinkResultSet.getTimestamp("c10"),
            sinkResultSet.getString("c11"),
            sinkResultSet.getString("c12"),
            sinkResultSet.getString("c13"),
            sinkResultSet.getString("c14"),
            sinkResultSet.getString("c15"),
            sinkResultSet.getString("c16"),
            sinkResultSet.getString("c17"),
            sinkResultSet.getString("c18")
          )
          actual += row
        }
      } finally if (sinkStatement != null) sinkStatement.close()
    }
    actual
  }

}
