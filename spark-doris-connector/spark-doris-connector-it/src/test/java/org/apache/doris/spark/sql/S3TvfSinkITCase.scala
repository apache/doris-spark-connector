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

import org.apache.doris.spark.container.{AbstractS3TvfTestBase, ContainerUtils}
import org.apache.doris.spark.container.AbstractContainerTestBase.getDorisQueryConnection
import org.apache.doris.spark.container.AbstractS3TvfTestBase.uniqueName
import org.apache.spark.TaskContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{Before, Test}
import org.slf4j.LoggerFactory

import java.util
import java.util.regex.Pattern
import scala.collection.JavaConverters._

/** Integration coverage for Spark batch writes through the Doris S3 TVF. */
class S3TvfSinkITCase extends AbstractS3TvfTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[S3TvfSinkITCase])
  private val database = "test_s3_tvf_sink"

  @Before
  def createDatabase(): Unit = {
    executeSql(s"CREATE DATABASE IF NOT EXISTS $database")
  }

  @Test
  def testMultiFileBatchWrite(): Unit = {
    val table = uniqueName("duplicate_multi_file")
    val prefix = uniqueName("objects") + "/"
    val labelPrefix = uniqueName("label")
    createDuplicateTable(
      table,
      "`id` INT, `name` VARCHAR(128), `note` VARCHAR(128) NULL")

    withSpark("local[1]") { session =>
      import session.implicits._
      val rows = Seq(
        (1, "doris", "中文"),
        (2, "spark", "quote-'and-\""),
        (3, "null-value", null.asInstanceOf[String]))
        .toDF("id", "name", "note")
        .coalesce(1)

      rows.write
        .format("doris")
        .options(s3TvfSinkOptions(
          s"$database.$table", prefix, labelPrefix, 2).asScala)
        .mode(SaveMode.Append)
        .save()
    }

    assertResult(
      table,
      "id,name,note",
      util.Arrays.asList(
        "1,doris,中文",
        "2,spark,quote-'and-\"",
        "3,null-value,null"),
      columnCount = 3)

    val keys = listObjectKeys(prefix).asScala
    assertEquals(2, keys.size)
    val keyPattern =
      Pattern.quote(prefix + labelPrefix + "_" + table + "_") +
        "[0-9a-f-]{36}_0_[0-9]+\\.json"
    assertTrue(keys.forall(_.matches(keyPattern)))
    assertFalse(keys.exists(_.contains("//")))
  }

  @Test
  def testParallelTaskWrite(): Unit = {
    val table = uniqueName("parallel")
    val prefix = uniqueName("parallel_objects")
    val labelPrefix = uniqueName("parallel_label")
    createDuplicateTable(table, "`id` INT, `task_value` VARCHAR(128)")

    withSpark("local[2]") { session =>
      val rows = session.range(0, 20)
        .selectExpr("CAST(id AS INT) AS id", "concat('value-', id) AS task_value")
        .repartition(2)
      rows.write
        .format("doris")
        .options(s3TvfSinkOptions(
          s"$database.$table", prefix, labelPrefix, 100).asScala)
        .mode(SaveMode.Append)
        .save()
    }

    val count = querySingleValue(s"SELECT COUNT(*) FROM $database.$table")
    assertEquals("20", count)

    val keys = listObjectKeys(prefix + "/").asScala
    assertEquals(2, keys.size)
    val keyPattern =
      (Pattern.quote(prefix + "/" + labelPrefix + "_" + table + "_") +
        "([0-9a-f-]{36})_([0-9]+)_0\\.json").r
    val taskFiles = keys.map {
      case keyPattern(uuid, partition) => uuid -> partition
      case key => throw new AssertionError("Unexpected S3 TVF object key: " + key)
    }
    assertEquals(Set("0", "1"), taskFiles.map(_._2).toSet)
    assertEquals(2, taskFiles.map(_._1).toSet.size)
  }

  @Test
  def testEmptyPartitionDoesNotInsert(): Unit = {
    val table = uniqueName("empty_partition")
    val prefix = uniqueName("empty_partition_objects")
    val labelPrefix = uniqueName("empty_partition_label")
    createDuplicateTable(table, "`id` INT, `name` VARCHAR(128)")

    withSpark("local[2]") { session =>
      import session.implicits._
      val rows = session.sparkContext.parallelize(Seq((1, "only-row")), 2)
        .toDF("id", "name")
      assertEquals(2, rows.rdd.getNumPartitions)
      rows.write
        .format("doris")
        .options(s3TvfSinkOptions(
          s"$database.$table", prefix, labelPrefix, 100).asScala)
        .mode(SaveMode.Append)
        .save()
    }

    assertEquals("1", querySingleValue(s"SELECT COUNT(*) FROM $database.$table"))
    assertEquals(1, listObjectKeys(prefix + "/").size())
  }

  @Test
  def testPartialColumnUpdate(): Unit = {
    val table = uniqueName("partial_update")
    val prefix = uniqueName("partial_update_objects")
    val labelPrefix = uniqueName("partial_update_label")
    createUniqueTable(table, "`id` INT, `name` VARCHAR(128), `score` INT")
    executeSql(s"INSERT INTO $database.$table VALUES (1, 'before', 10)")

    withSpark("local[1]") { session =>
      import session.implicits._
      val options = s3TvfSinkOptions(
        s"$database.$table", prefix, labelPrefix, 100)
      options.put("sink.properties.columns", "id,name")
      options.put("sink.properties.partial_columns", "true")
      Seq((1, "after"))
        .toDF("id", "name")
        .write
        .format("doris")
        .options(options.asScala)
        .mode(SaveMode.Append)
        .save()
    }

    assertResult(
      table,
      "id,name,score",
      util.Collections.singletonList("1,after,10"),
      columnCount = 3)
  }

  @Test
  def testInsertFailureRetainsObjects(): Unit = {
    val table = uniqueName("failed_insert")
    val prefix = uniqueName("failed_objects")
    val labelPrefix = uniqueName("failed_label")
    createDuplicateTable(table, "`id` INT, `name` VARCHAR(128)")

    var writeFailed = false
    withSpark("local[1]") { session =>
      import session.implicits._
      val options = s3TvfSinkOptions(
        s"$database.$table", prefix, labelPrefix, 100)
      options.put("sink.properties.invalid-variable", "true")
      try {
        Seq((1, "failed-row"))
          .toDF("id", "name")
          .write
          .format("doris")
          .options(options.asScala)
          .mode(SaveMode.Append)
          .save()
      } catch {
        case _: Throwable => writeFailed = true
      }
    }
    assertTrue("Expected the Doris INSERT to fail", writeFailed)

    val keys = listObjectKeys(prefix + "/").asScala
    assertTrue("Expected failed task attempts to retain staged objects", keys.nonEmpty)
  }

  @Test
  def testTaskRetryUsesNewUuid(): Unit = {
    val table = uniqueName("task_retry")
    val prefix = uniqueName("retry_objects")
    val labelPrefix = uniqueName("retry_label")
    createDuplicateTable(table, "`id` INT, `name` VARCHAR(128)")

    withSpark("local[1,2]") { session =>
      import session.implicits._
      session.sparkContext.parallelize(Seq(
        (1, "doris"),
        (2, "spark"),
        (3, "catalog")
      ), 1).mapPartitions { records =>
        val attempt = TaskContext.get().attemptNumber()
        var emitted = 0
        new Iterator[(Int, String)] {
          override def hasNext: Boolean = {
            if (attempt == 0 && emitted == 1) {
              throw new RuntimeException("Trigger task retry after the first row")
            }
            records.hasNext
          }

          override def next(): (Int, String) = {
            emitted += 1
            records.next()
          }
        }
      }.toDF("id", "name")
        .write
        .format("doris")
        .options(s3TvfSinkOptions(
          s"$database.$table", prefix, labelPrefix, 1).asScala)
        .mode(SaveMode.Append)
        .save()
    }

    assertEquals("3", querySingleValue(s"SELECT COUNT(*) FROM $database.$table"))
    val keys = listObjectKeys(prefix + "/").asScala
    val keyPattern =
      (Pattern.quote(prefix + "/" + labelPrefix + "_" + table + "_") +
        "([0-9a-f-]{36})_[0-9]+_[0-9]+\\.json").r
    val uuids = keys.map {
      case keyPattern(uuid) => uuid
      case key => throw new AssertionError("Unexpected S3 TVF object key: " + key)
    }.toSet
    assertTrue("Expected task retry to use a new UUID", uuids.size >= 2)
  }

  private def createDuplicateTable(table: String, columns: String): Unit = {
    createTable(table, columns, s"DUPLICATE KEY(`id`)", "")
  }

  private def createUniqueTable(table: String, columns: String): Unit = {
    createTable(
      table,
      columns,
      "UNIQUE KEY(`id`)",
      ", \"enable_unique_key_merge_on_write\" = \"true\"")
  }

  private def createTable(
      table: String,
      columns: String,
      keyDefinition: String,
      additionalProperties: String): Unit = {
    executeSql(
      s"DROP TABLE IF EXISTS $database.$table",
      s"CREATE TABLE $database.$table ($columns) $keyDefinition " +
        "DISTRIBUTED BY HASH(`id`) BUCKETS 1 " +
        s"""PROPERTIES ("replication_num" = "1"$additionalProperties)""")
  }

  private def assertResult(
      table: String,
      columns: String,
      expected: util.List[String],
      columnCount: Int): Unit = {
    ContainerUtils.checkResult(
      getDorisQueryConnection,
      LOG,
      expected,
      s"SELECT $columns FROM $database.$table ORDER BY $columns",
      columnCount,
      true)
  }

  private def querySingleValue(sql: String): String = {
    ContainerUtils.executeSQLStatement(getDorisQueryConnection, LOG, sql, 1).get(0)
  }

  private def executeSql(sql: String*): Unit = {
    ContainerUtils.executeSQLStatement(getDorisQueryConnection, LOG, sql: _*)
  }

  private def withSpark(master: String)(test: SparkSession => Unit): Unit = {
    val session = SparkSession.builder()
      .appName("s3-tvf-it")
      .master(master)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    try {
      test(session)
    } finally {
      session.stop()
    }
  }
}
