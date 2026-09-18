// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.sql

import org.apache.doris.spark.container.ContainerUtils
import org.apache.doris.spark.container.instance.DorisCustomerContainer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.{AfterClass, Assume, BeforeClass, Test}
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.util
import java.util.UUID
import scala.collection.JavaConverters._

/** Opt-in integration test for Spark DataFrame S3 TVF writes with an AWS IAM role. */
class S3TvfIamRoleITCase {
  import S3TvfIamRoleITCase._

  @Test
  def testWritesThroughIamRole(): Unit = {
    val table = "iam_role_" + UUID.randomUUID().toString.replace("-", "")
    try {
      executeSql(
        s"CREATE DATABASE IF NOT EXISTS `$database`",
        s"CREATE TABLE `$database`.`$table` (`id` INT, `name` VARCHAR(64)) " +
          "DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 " +
          "PROPERTIES (\"replication_num\" = \"1\")")

      val session = SparkSession.builder()
        .appName("s3-tvf-iam-role-it")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
      try {
        import session.implicits._
        Seq((1, "doris"), (2, "spark"))
          .toDF("id", "name")
          .write
          .format("doris")
          .options(sinkOptions(table).asScala)
          .mode(SaveMode.Append)
          .save()
      } finally {
        session.stop()
      }

      val connection = doris.getQueryConnection
      val rows = try {
        ContainerUtils.executeSQLStatement(
          connection,
          LOG,
          s"SELECT id,name FROM `$database`.`$table` ORDER BY id",
          2)
      } finally {
        connection.close()
      }
      assertEquals(util.Arrays.asList("1,doris", "2,spark"), rows)
    } finally {
      executeSql(s"DROP TABLE IF EXISTS `$database`.`$table`")
    }
  }

  private def sinkOptions(table: String): util.Map[String, String] = {
    val options = new util.HashMap[String, String]()
    options.put("doris.fenodes", doris.getFenodes)
    options.put("doris.query.port", doris.getQueryPort.toString)
    options.put("doris.table.identifier", s"$database.$table")
    options.put("user", doris.getUsername)
    options.put("password", doris.getPassword)
    options.put("doris.sink.mode", "tvf")
    options.put("doris.sink.label.prefix", "iam_role_" + UUID.randomUUID())
    options.put("doris.sink.s3.endpoint", requiredProperty("s3_endpoint"))
    options.put("doris.sink.s3.region", requiredProperty("s3_region"))
    options.put("doris.sink.s3.bucket", requiredProperty("s3_bucket"))
    options.put("doris.sink.s3.prefix", System.getProperty("s3_prefix", "doris-spark-connector-it"))
    options.put("doris.sink.s3.role-arn", requiredProperty("s3_role_arn"))
    optionalProperty("s3_external_id")
      .foreach(options.put("doris.sink.s3.external-id", _))
    options
  }

  private def executeSql(sql: String*): Unit = {
    val connection: Connection = doris.getQueryConnection
    try {
      ContainerUtils.executeSQLStatement(connection, LOG, sql: _*)
    } finally {
      connection.close()
    }
  }

  private def requiredProperty(name: String): String = {
    optionalProperty(name).getOrElse(
      throw new IllegalArgumentException("Missing required system property: " + name))
  }

  private def optionalProperty(name: String): Option[String] = {
    Option(System.getProperty(name)).map(_.trim).filter(_.nonEmpty)
  }
}

object S3TvfIamRoleITCase {
  private val LOG = LoggerFactory.getLogger(classOf[S3TvfIamRoleITCase])
  private val database = "test_s3_tvf_iam_role"
  private var doris: DorisCustomerContainer = _

  @BeforeClass
  def useExternalEnvironment(): Unit = {
    Assume.assumeTrue(
      "IAM role ITCase requires -Ds3_tvf_iam_role_it=true",
      java.lang.Boolean.getBoolean("s3_tvf_iam_role_it"))
    Assume.assumeTrue(
      "IAM role ITCase requires -Dcustomer_env=true",
      java.lang.Boolean.getBoolean("customer_env"))
    Seq("s3_endpoint", "s3_region", "s3_bucket", "s3_role_arn")
      .foreach(name => {
        val value = System.getProperty(name)
        if (value == null || value.trim.isEmpty) {
          throw new IllegalArgumentException("Missing required system property: " + name)
        }
      })
    doris = new DorisCustomerContainer()
    doris.startContainer()
  }

  @AfterClass
  def closeExternalEnvironment(): Unit = {
    if (doris != null) {
      doris.close()
    }
  }
}
