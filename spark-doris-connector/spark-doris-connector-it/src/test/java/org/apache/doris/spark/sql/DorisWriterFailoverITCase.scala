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
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.junit.rules.ExpectedException
import org.junit.{Before, Rule, Test}
import org.slf4j.LoggerFactory

import java.util
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.JavaConverters._
import scala.util.control.Breaks._

/**
 * Test DorisWriter failover.
 */
class DorisWriterFailoverITCase extends AbstractContainerTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[DorisWriterFailoverITCase])
  val DATABASE = "test_doris_failover"
  val TABLE_WRITE_TBL_RETRY = "tbl_write_tbl_retry"
  val TABLE_WRITE_TBL_TASK_RETRY = "tbl_write_tbl_task_retry"
  val TABLE_WRITE_TBL_PRECOMMIT_FAIL = "tbl_write_tbl_precommit_fail"
  val TABLE_WRITE_TBL_COMMIT_FAIL = "tbl_write_tbl_commit_fail"
  val TABLE_WRITE_TBL_FAIL_BEFORE_STOP = "tbl_write_tbl_fail_before_stop"

  val _thrown: ExpectedException = ExpectedException.none

  @Rule
  def thrown: ExpectedException = _thrown

  @Before
  def setUp(): Unit = {
    ContainerUtils.executeSQLStatement(getDorisQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE))
  }

  @Test
  def testFailoverForRetry(): Unit = {
    initializeTable(TABLE_WRITE_TBL_RETRY, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris", "1234"),
      ("spark", "123456"),
      ("catalog", "12345678")
    )).toDF("name", "address")
    df.createTempView("mock_source")

    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_sink
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_WRITE_TBL_RETRY}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}",
         | "doris.sink.batch.interval.ms"="1000",
         | "doris.sink.batch.size"="1",
         | "doris.sink.max-retries"="100",
         | "doris.sink.enable-2pc"="false"
         |)
         |""".stripMargin)

    val service = Executors.newSingleThreadExecutor()
    val future = service.submit(new Runnable {
      override def run(): Unit = {
        session.sql("INSERT INTO test_sink SELECT * FROM mock_source")
      }
    })

    val query = String.format("SELECT * FROM %s.%s", DATABASE, TABLE_WRITE_TBL_RETRY)
    var result: util.List[String] = null
    val connection = getDorisQueryConnection(DATABASE)
    breakable {
      while (true) {
        try {
          // query may be failed
          result = ContainerUtils.executeSQLStatement(connection, LOG, query, 2)
        } catch {
          case ex: Exception =>
            LOG.error("Failed to query result, cause " + ex.getMessage)
        }

        // until insert 1 rows
        if (result.size >= 1){
          Thread.sleep(5000)
          ContainerUtils.executeSQLStatement(
            connection,
            LOG,
            String.format("ALTER TABLE %s.%s MODIFY COLUMN address varchar(256)", DATABASE, TABLE_WRITE_TBL_RETRY))
          break
        }
      }
    }

    future.get(60, TimeUnit.SECONDS)
    session.stop()
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_WRITE_TBL_RETRY),
      2)
    val expected = util.Arrays.asList("doris,1234", "spark,123456", "catalog,12345678");
    checkResultInAnyOrder("testFailoverForRetry", expected.toArray, actual.toArray)
  }


  /**
   * Test failover for task retry and sink.max-retries=0
   */
  @Test
  def testFailoverForTaskRetry(): Unit = {
    initializeTable(TABLE_WRITE_TBL_TASK_RETRY, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[1,100]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris", "cn"),
      ("spark", "us"),
      ("catalog", "uk")
    )).toDF("name", "address")
    df.createTempView("mock_source")

    var uuid = UUID.randomUUID().toString
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_sink
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_WRITE_TBL_TASK_RETRY}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}",
         | "doris.sink.batch.size"="1",
         | "doris.sink.batch.interval.ms"="1000",
         | "doris.sink.max-retries"="0",
         | "doris.sink.enable-2pc"="true",
         | "doris.sink.label.prefix"='${uuid}'
         |)
         |""".stripMargin)

    val service = Executors.newSingleThreadExecutor()
    val future = service.submit(new Runnable {
      override def run(): Unit = {
        session.sql("INSERT INTO test_sink SELECT * FROM mock_source")
      }
    })

    val query = "show transaction from " + DATABASE + " where label like '" + uuid + "%'"
    var result: List[String] = null
    val connection = getDorisQueryConnection(DATABASE)
    breakable {
      while (true) {
        try {
          // query may be failed
          result = ContainerUtils.executeSQLStatement(connection, LOG, query, 15).asScala.toList
        } catch {
          case ex: Exception =>
            LOG.error("Failed to query result, cause " + ex.getMessage)
        }

        // until insert 1 rows
        if (result.size >= 1 && result.forall(s => s.contains("PRECOMMITTED"))){
          faultInjectionOpen()
          Thread.sleep(3000)
          faultInjectionClear()
          break
        }
      }
    }

    future.get(60, TimeUnit.SECONDS)
    session.stop()
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_WRITE_TBL_TASK_RETRY),
      2)
    val expected = util.Arrays.asList("doris,cn", "spark,us", "catalog,uk");
    checkResultInAnyOrder("testFailoverForTaskRetry", expected.toArray, actual.toArray)
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
        + "`name` varchar(32),\n"
        + "`address` varchar(4) %s\n"
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

  @Test
  def testForWriteExceptionBeforeStop(): Unit = {
    initializeTable(TABLE_WRITE_TBL_FAIL_BEFORE_STOP, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      val df = session.createDataFrame(Seq(
        ("doris", "cn"),
        ("spark", "us"),
        ("catalog", "uk")
      )).toDF("name", "address")
      thrown.expect(classOf[SparkException])
      thrown.expectMessage("Only unique key merge on write support partial update")
      df.write.format("doris")
        .option("table.identifier", DATABASE + "." + TABLE_WRITE_TBL_FAIL_BEFORE_STOP)
        .option("fenodes", getFenodes)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.sink.properties.partial_columns", "true")
        .option("doris.sink.net.buffer.size", "1")
        .mode("append")
        .save()
    } finally {
      session.stop()
    }
  }

}
