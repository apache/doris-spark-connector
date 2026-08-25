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
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.sql.SparkSession
import org.hamcrest.{CoreMatchers, Description, Matcher}
import org.junit.rules.ExpectedException
import org.junit.{Before, Rule, Test}
import org.slf4j.LoggerFactory

import java.util
import java.util.concurrent.{Executors, Future, TimeUnit}
import scala.collection.JavaConverters._

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
    LOG.info("start to test testFailoverForRetry.")
    // Use a UNIQUE (primary-key) table: without 2PC a retried batch may re-load rows that already
    // landed, and the unique key on `name` makes that re-load idempotent. The three rows have
    // distinct names, so dedup removes duplicates without dropping any legitimate row.
    initializeTable(TABLE_WRITE_TBL_RETRY, DataModel.UNIQUE, "varchar(256) NOT NULL")
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("doris", "1234"),
      ("spark", null),
      ("catalog", null)
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
         | "doris.sink.retry.interval.ms"="10000",
         | "doris.sink.batch.size"="1",
         | "doris.sink.max-retries"="3",
         | "doris.sink.enable-2pc"="false",
         | "doris.sink.properties.strict_mode"="true"
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
    try {
      waitForCondition(future, "at least one row to be loaded") {
        try {
          // query may fail while the load is being retried
          result = ContainerUtils.executeSQLStatement(connection, LOG, query, 2)
        } catch {
          case ex: Exception =>
            LOG.error("Failed to query result, cause " + ex.getMessage)
        }
        result != null && result.size >= 1
      }
      ContainerUtils.executeSQLStatement(
        connection,
        LOG,
        String.format("ALTER TABLE %s.%s MODIFY COLUMN address varchar(256) NULL", DATABASE, TABLE_WRITE_TBL_RETRY))

      future.get(60, TimeUnit.SECONDS)
    } finally {
      service.shutdownNow()
      session.stop()
    }
    val actual = ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("select * from %s.%s", DATABASE, TABLE_WRITE_TBL_RETRY),
      2)
    val expected = util.Arrays.asList("doris,1234", "spark,null", "catalog,null");
    checkResultInAnyOrder("testFailoverForRetry", expected.toArray, actual.toArray)
  }


  /**
   * Test failover for task retry and sink.max-retries=0
   */
  @Test
  def testFailoverForTaskRetry(): Unit = {
    LOG.info("start to test testFailoverForTaskRetry.")
    initializeTable(TABLE_WRITE_TBL_TASK_RETRY, DataModel.DUPLICATE)
    val session = SparkSession.builder().master("local[1,2]").getOrCreate()
    import session.implicits._
    val df = session.sparkContext.parallelize(Seq(
      ("doris", "cn"),
      ("spark", "us"),
      ("catalog", "uk")
    ), 1).mapPartitions { records =>
      val attempt = TaskContext.get().attemptNumber()
      var emitted = 0
      new Iterator[(String, String)] {
        override def hasNext: Boolean = {
          if (attempt == 0 && emitted == 1) {
            throw new RuntimeException("Trigger task retry after the first row")
          }
          records.hasNext
        }

        override def next(): (String, String) = {
          emitted += 1
          records.next()
        }
      }
    }.toDF("name", "address")
    df.createTempView("mock_source")

    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_sink
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_WRITE_TBL_TASK_RETRY}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}",
         | "doris.sink.batch.size"="100",
         | "doris.sink.batch.interval.ms"="1000",
         | "doris.sink.max-retries"="0",
         | "doris.sink.enable-2pc"="true"
         |)
         |""".stripMargin)

    try {
      session.sql("INSERT INTO test_sink SELECT * FROM mock_source")
    } finally {
      session.stop()
    }

    val expected = util.Arrays.asList("doris,cn", "spark,us", "catalog,uk");
    var actual = util.Collections.emptyList[String]()
    val connection = getDorisQueryConnection
    try {
      waitForCondition("task retry rows to become visible") {
        actual = ContainerUtils.executeSQLStatement(
          connection,
          LOG,
          String.format("select * from %s.%s", DATABASE, TABLE_WRITE_TBL_TASK_RETRY),
          2)
        actual.size() >= expected.size()
      }
    } finally {
      connection.close()
    }
    checkResultInAnyOrder("testFailoverForTaskRetry", expected.toArray, actual.toArray)
  }

  private def waitForCondition(description: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
    while (System.nanoTime() < deadline) {
      if (condition) {
        return
      }
      TimeUnit.MILLISECONDS.sleep(100)
    }
    throw new AssertionError(s"Timed out waiting for $description")
  }

  private def waitForCondition(future: Future[_], description: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
    while (System.nanoTime() < deadline) {
      if (future.isDone) {
        future.get()
        throw new AssertionError(s"Load completed before observing $description")
      }
      if (condition) {
        if (future.isDone) {
          future.get()
          throw new AssertionError(s"Load completed before observing $description")
        }
        return
      }
      Thread.sleep(100)
    }
    throw new AssertionError(s"Timed out waiting for $description")
  }

  private def initializeTable(
      table: String,
      dataModel: DataModel,
      addressType: String = "varchar(4)"): Unit = {
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
        + "`address` %s %s\n"
        + ") "
        + " %s KEY(`name`) "
        + " DISTRIBUTED BY HASH(`name`) BUCKETS 1\n"
        + "PROPERTIES ("
        + "\"replication_num\" = \"1\"\n" + morProps + ")", DATABASE, table, addressType, max, model))
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
