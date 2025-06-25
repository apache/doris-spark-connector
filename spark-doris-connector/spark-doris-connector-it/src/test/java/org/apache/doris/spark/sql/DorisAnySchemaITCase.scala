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

import org.apache.doris.spark.container.{AbstractContainerTestBase, ContainerUtils}
import org.apache.doris.spark.container.AbstractContainerTestBase.{assertEqualsInAnyOrder, getDorisQueryConnection}
import org.apache.doris.spark.rest.models.DataModel
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

class DorisAnySchemaITCase extends AbstractContainerTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[DorisAnySchemaITCase])

  val DATABASE: String = "example_db"
  /*
   *   CREATE TABLE table4
   *   (
   *       siteid INT DEFAULT '10',
   *       citycode SMALLINT,
   *       username VARCHAR(32) DEFAULT '',
   *       pv BIGINT DEFAULT '0'
   *   )
   *   UNIQUE KEY(siteid, citycode, username)
   *   DISTRIBUTED BY HASH(siteid) BUCKETS 10
   *   PROPERTIES("replication_num" = "1");
   */
  val dorisTable = "table4"

  /*
   *   CREATE TABLE table2
   *   (
   *       siteid INT DEFAULT '10',
   *       citycode SMALLINT,
   *       username VARCHAR(32) DEFAULT '',
   *       pv BIGINT DEFAULT '0'
   *   )
   *   UNIQUE KEY(siteid, citycode, username)
   *   DISTRIBUTED BY HASH(siteid) BUCKETS 10
   *   PROPERTIES("replication_num" = "1");
   */
  val dorisSourceTable = "table2"

  /*
   *   CREATE TABLE table5
   *   (
   *       siteid INT DEFAULT '10',
   *       citycode SMALLINT,
   *       username VARCHAR(32) DEFAULT '',
   *       pv BIGINT DEFAULT '0',
   *       p_value BIGINT DEFAULT '0'
   *   )
   *   UNIQUE KEY(siteid, citycode, username)
   *   DISTRIBUTED BY HASH(siteid) BUCKETS 10
   *   PROPERTIES("replication_num" = "1");
   */
  val dorisPartialTable = "table5"

  @Test
  def jsonDataWriteTest(): Unit = {
    initializeTable(dorisTable, DataModel.UNIQUE)

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = spark.createDataFrame(Seq(
        (0, 0, "user1", 100),
        (1, 0, "user2", 100),
        (3, 0, "user2", 200)
      )).toDF("siteid", "citycode", "username", "pv")
      df.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + dorisTable)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.sink.properties.format", "json")
        .option("sink.batch.size", 2)
        .option("sink.max-retries", 2)
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      val actual = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, dorisTable),
        4)
      val expected = util.Arrays.asList("0,0,user1,100", "1,0,user2,100", "3,0,user2,200");
      checkResultInAnyOrder("testSinkJsonFormat", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }

  @Test
  def jsonDataWriteWithPartialUpdateTest(): Unit = {
    initializePartialTable(dorisPartialTable, DataModel.UNIQUE)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val df = spark.createDataFrame(Seq(
        (0, 0, "user4", 100),
        (1, 0, "user5", 100),
        (3, 0, "user6", 200)
      )).toDF("siteid", "citycode", "username", "pv")
      df.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + dorisPartialTable)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.sink.properties.format", "json")
        .option("doris.sink.properties.partial_columns", "true")
        .option("doris.write.fields", "siteid,citycode,username,pv")
        .option("sink.batch.size", 2)
        .option("sink.max-retries", 2)
        .mode(SaveMode.Append)
        .save()
      spark.stop()
      Thread.sleep(2000)
      val actual = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, dorisPartialTable),
        5)
      val expected = util.Arrays.asList("0,0,user4,100,0", "1,0,user5,100,0", "3,0,user6,200,0");
      checkResultInAnyOrder("testSinkJsonFormat", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }

  @Test
  def jsonDataWriteSqlTest(): Unit = {
    initializeTable(dorisTable, DataModel.UNIQUE)
    initializeTable(dorisSourceTable, DataModel.UNIQUE)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val doris = spark.sql(
        s"""
           |CREATE TEMPORARY VIEW test_lh
           |USING doris
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + dorisTable}",
           | "fenodes"="${getFenodes}",
           | "user"="${getDorisUsername}",
           | "password"="${getDorisPassword}"
           |)
           |""".stripMargin)
      spark.sql(
        """
          |insert into test_lh values (0, 0, "user1", 100), (1, 0, "user2", 100),(2, 1, "user3", 100),(3, 0, "user2", 200)
          |""".stripMargin)

      spark.sql(
        s"""
           |CREATE TEMPORARY VIEW table2
           |USING doris
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + dorisSourceTable}",
           | "fenodes"="${getFenodes}",
           | "user"="${getDorisUsername}",
           | "password"="${getDorisPassword}"
           |)
           |""".stripMargin)

      spark.sql(
        """
          |insert into table2 values (0, 0, "user1", 100), (1, 0, "user2", 100),(2, 1, "user3", 100),(3, 0, "user2", 200)
          |""".stripMargin)

      spark.sql(
        """
          |insert into test_lh select siteid, citycode + 1, username, pv + 1 from table2
          |""".stripMargin)

      Thread.sleep(2000)
      val actual = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, dorisTable),
        4)
      val expected = util.Arrays.asList("0,0,user1,100", "1,0,user2,100", "2,1,user3,100", "3,0,user2,200",
        "0,1,user1,101", "1,1,user2,101", "2,2,user3,101", "3,1,user2,201");
      checkResultInAnyOrder("testSinkJsonFormat", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }

  @Test
  def jsonDataWriteWithPartialUpdateSqlTest(): Unit = {
    initializePartialTable(dorisPartialTable, DataModel.UNIQUE)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val doris = spark.sql(
        s"""
           |CREATE TEMPORARY VIEW test_lh
           |USING doris
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + dorisPartialTable}",
           | "fenodes"="${getFenodes}",
           | "user"="${getDorisUsername}",
           | "password"="${getDorisPassword}",
           | "doris.sink.properties.format" = "csv",
           | "doris.sink.properties.partial_columns" = "true",
           | "doris.write.fields" = "siteid,citycode,username,pv"
           |)
           |""".stripMargin)
      spark.sql(
        """
          |INSERT INTO test_lh (siteid, citycode, username, pv) VALUES (0, 0, 'user1',3)
          |""".stripMargin)

      Thread.sleep(2000)
      val actual = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, dorisPartialTable),
        5)
      val expected = util.Arrays.asList("0,0,user1,3,0");
      checkResultInAnyOrder("testSinkJsonFormat", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }

  @Test
  def jsonDataWriteWithPartialUpdateSqlTest1(): Unit = {
    initializePartialTable(dorisPartialTable, DataModel.UNIQUE)
    initializeTable(dorisSourceTable, DataModel.UNIQUE)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      val doris = spark.sql(
        s"""
           |CREATE TEMPORARY VIEW test_lh
           |USING doris
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + dorisPartialTable}",
           | "fenodes"="${getFenodes}",
           | "user"="${getDorisUsername}",
           | "password"="${getDorisPassword}",
           | "doris.sink.properties.format" = "json",
           | "doris.sink.properties.partial_columns" = "true"
           |)
           |""".stripMargin)

      spark.sql(
        s"""
           |CREATE TEMPORARY VIEW table2
           |USING doris
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + dorisSourceTable}",
           | "fenodes"="${getFenodes}",
           | "user"="${getDorisUsername}",
           | "password"="${getDorisPassword}"
           |)
           |""".stripMargin)

      spark.sql(
        """
          |INSERT INTO table2 (siteid, citycode, username, pv) VALUES (0, 0, 'user1',3)
          |""".stripMargin)

      spark.sql(
        """
          |insert into test_lh(siteid,citycode,username, pv) select siteid,citycode+1 as citycode,username,pv+1 as pv from table2
          |""".stripMargin)
      Thread.sleep(2000)
      val actual = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select * from %s.%s", DATABASE, dorisPartialTable),
        5)
      val expected = util.Arrays.asList("0,1,user1,4,0");
      checkResultInAnyOrder("testSinkJsonFormat", expected.toArray, actual.toArray)
    } finally {
      spark.stop()
    }
  }

  private def initializeTable(table: String, dataModel: DataModel): Unit = {
    val morProps = if (!(DataModel.UNIQUE_MOR == dataModel)) "" else ",\"enable_unique_key_merge_on_write\" = \"false\""
    val model = if (dataModel == DataModel.UNIQUE_MOR) DataModel.UNIQUE.toString else dataModel.toString
    ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
      String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
      String.format("CREATE TABLE %s.%s ( \n"
        + " siteid INT DEFAULT '10',"
        + " citycode SMALLINT, "
        + " username VARCHAR(32) DEFAULT '',"
        + " pv BIGINT DEFAULT '0' "
        + " )"
        + " %s KEY(siteid, citycode, username) "
        + " DISTRIBUTED BY HASH(`siteid`) BUCKETS 1\n"
        + "PROPERTIES ("
        + "\"replication_num\" = \"1\"\n" + morProps + ")", DATABASE, table, model))
  }

  private def initializePartialTable(table: String, dataModel: DataModel): Unit = {
    val morProps = if (!(DataModel.UNIQUE_MOR == dataModel)) "" else ",\"enable_unique_key_merge_on_write\" = \"false\""
    val model = if (dataModel == DataModel.UNIQUE_MOR) DataModel.UNIQUE.toString else dataModel.toString
    ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
      String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
      String.format("CREATE TABLE %s.%s ( \n"
        + " siteid INT DEFAULT '10',"
        + " citycode SMALLINT, "
        + " username VARCHAR(32) DEFAULT '',"
        + " pv BIGINT DEFAULT '0', "
        + " p_value BIGINT DEFAULT '0' "
        + " )"
        + " %s KEY(siteid, citycode, username) "
        + " DISTRIBUTED BY HASH(`siteid`) BUCKETS 1\n"
        + "PROPERTIES ("
        + "\"replication_num\" = \"1\"\n" + morProps + ")", DATABASE, table, model))
  }

  private def checkResultInAnyOrder(testName: String, expected: Array[AnyRef], actual: Array[AnyRef]): Unit = {
    LOG.info("Checking DorisAnySchemaITCase result. testName={}, actual={}, expected={}", testName, actual, expected)
    assertEqualsInAnyOrder(expected.toList.asJava, actual.toList.asJava)
  }
}
