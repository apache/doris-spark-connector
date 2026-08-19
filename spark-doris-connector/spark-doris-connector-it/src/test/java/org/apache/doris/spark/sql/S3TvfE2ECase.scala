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
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertTrue
import org.junit.Test
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

/** End-to-end coverage for reading from Doris and writing through the S3 TVF. */
class S3TvfE2ECase extends AbstractS3TvfTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[S3TvfE2ECase])

  @Test
  def testDorisToDorisThroughS3Tvf(): Unit = {
    val database = uniqueName("test_s3_tvf_e2e")
    val sourceTable = "source_table"
    val targetTable = "target_table"
    val prefix = uniqueName("e2e_objects")
    val labelPrefix = uniqueName("e2e_label")

    executeSql(
      s"CREATE DATABASE $database",
      createTableSql(database, sourceTable),
      createTableSql(database, targetTable),
      s"INSERT INTO $database.$sourceTable VALUES " +
        "(1, 'alpha', 10.25, '2026-08-18', '中文')," +
        "(2, 'beta', -2.50, '2026-08-19', 'quote-''and-\"')," +
        "(3, 'gamma', 0.00, '2026-08-20', NULL)," +
        "(4, 'delta', 9999.99, '2026-08-21', 'done')")

    val session = SparkSession.builder()
      .appName("s3-tvf-e2e")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    try {
      val source = session.read
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", s"$database.$sourceTable")
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .load()
        .select("id", "name", "amount", "event_date", "note")
        .repartition(2)
      source.createOrReplaceTempView("s3_tvf_source")

      val sinkOptions = s3TvfSinkOptions(
        s"$database.$targetTable", prefix, labelPrefix, 2)
      session.sql(
        s"CREATE TEMPORARY VIEW s3_tvf_sink USING doris OPTIONS(" +
          renderOptions(sinkOptions) + ")")
      session.sql(
        "INSERT INTO s3_tvf_sink " +
          "SELECT id, name, amount, event_date, note FROM s3_tvf_source")
    } finally {
      session.stop()
    }

    ContainerUtils.checkResult(
      getDorisQueryConnection,
      LOG,
      util.Arrays.asList(
        "1,alpha,10.25,2026-08-18,中文",
        "2,beta,-2.50,2026-08-19,quote-'and-\"",
        "3,gamma,0.00,2026-08-20,null",
        "4,delta,9999.99,2026-08-21,done"),
      s"SELECT id,name,amount,event_date,note " +
        s"FROM $database.$targetTable ORDER BY id",
      5,
      true)
    assertTrue(listObjectKeys(prefix + "/").size() >= 2)
  }

  private def createTableSql(database: String, table: String): String = {
    s"CREATE TABLE $database.$table (" +
      "`id` INT, `name` VARCHAR(128), `amount` DECIMAL(12,2), " +
      "`event_date` DATE, `note` VARCHAR(128) NULL) " +
      "DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 " +
      "PROPERTIES (\"replication_num\" = \"1\")"
  }

  private def renderOptions(options: java.util.Map[String, String]): String = {
    options.asScala.toSeq
      .sortBy(_._1)
      .map { case (key, value) =>
        s"'${escapeSqlLiteral(key)}'='${escapeSqlLiteral(value)}'"
      }
      .mkString(",")
  }

  private def escapeSqlLiteral(value: String): String = value.replace("'", "''")

  private def executeSql(sql: String*): Unit = {
    ContainerUtils.executeSQLStatement(getDorisQueryConnection, LOG, sql: _*)
  }
}
