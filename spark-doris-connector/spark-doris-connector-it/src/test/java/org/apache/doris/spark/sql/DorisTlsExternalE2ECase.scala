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

import org.apache.doris.spark.sparkContextFunctions
import org.apache.doris.spark.container.ExternalDorisTlsTestEnvironment
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.junit.Assert.assertEquals
import org.junit.{Assume, Test}

import java.sql.Connection
import java.util.{Arrays, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/** End-to-end coverage for an externally managed Doris TLS cluster. */
class DorisTlsExternalE2ECase {

  @Test
  def testDataFrameRddAndSqlTlsReadWrite(): Unit = {
    Assume.assumeTrue(
      "External Doris TLS E2E is disabled",
      ExternalDorisTlsTestEnvironment.isEnabled(System.getProperties))

    val environment = ExternalDorisTlsTestEnvironment.fromSystemProperties()
    val database = "spark_connector_tls_e2e_" + UUID.randomUUID().toString.replace("-", "")
    var session: SparkSession = null
    var databaseCreated = false
    var primaryFailure: Throwable = null

    try {
      createDatabase(environment, database)
      databaseCreated = true
      createTablesAndSourceRows(environment, database)

      session = SparkSession.builder()
        .appName("doris-external-tls-e2e")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()

      val sourceTable = database + ".source_table"
      val thriftRows = session.read
        .format("doris")
        .options(environment.connectorOptions(sourceTable, "thrift").asScala)
        .load()
        .withColumn("entry_api", lit("dataframe"))
        .withColumn("read_mode", lit("thrift"))
      val adbcRows = session.read
        .format("doris")
        .options(environment.connectorOptions(sourceTable, "arrow").asScala)
        .load()
        .withColumn("entry_api", lit("dataframe"))
        .withColumn("read_mode", lit("arrow"))

      val sourceSchema = StructType(Seq(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true)))
      val rawRdd = session.sparkContext.dorisRDD(
        tableIdentifier = Some(sourceTable),
        cfg = Some(environment.connectorOptions(sourceTable, "thrift").asScala.toMap))
      val rddRows = session.createDataFrame(
        rawRdd.map(value => Row.fromSeq(value.asInstanceOf[Array[AnyRef]].toSeq)),
        sourceSchema)
        .withColumn("entry_api", lit("rdd"))
        .withColumn("read_mode", lit("thrift"))

      createDorisTemporaryView(
        session,
        "tls_sql_source",
        environment.connectorOptions(sourceTable, "arrow"))
      val sqlRows = session.sql("SELECT id, name FROM tls_sql_source")
        .withColumn("entry_api", lit("sql"))
        .withColumn("read_mode", lit("arrow"))

      thriftRows
        .unionByName(adbcRows)
        .unionByName(rddRows)
        .unionByName(sqlRows)
        .coalesce(1)
        .createOrReplaceTempView("tls_combined_source")

      createDorisTemporaryView(
        session,
        "tls_sink",
        environment.connectorOptions(database + ".sink_table", null))
      session.sql(
        "INSERT INTO tls_sink " +
          "SELECT id, name, entry_api, read_mode FROM tls_combined_source")

      assertSinkRows(environment, database)
    } catch {
      case failure: Throwable =>
        primaryFailure = failure
        throw failure
    } finally {
      cleanup(session, environment, database, databaseCreated, primaryFailure)
    }
  }

  private def createDorisTemporaryView(
      session: SparkSession,
      viewName: String,
      options: java.util.Map[String, String]): Unit = {
    val renderedOptions = options.asScala.toSeq
      .sortBy(_._1)
      .map { case (key, value) =>
        s"'${escapeSqlLiteral(key)}'='${escapeSqlLiteral(value)}'"
      }
      .mkString(",")
    session.sql(
      s"CREATE TEMPORARY VIEW $viewName USING doris OPTIONS($renderedOptions)")
  }

  private def escapeSqlLiteral(value: String): String = value.replace("'", "''")

  private def createDatabase(
      environment: ExternalDorisTlsTestEnvironment,
      database: String): Unit = {
    withConnection(environment.openConnection()) { connection =>
      val statement = connection.createStatement()
      try {
        statement.execute("CREATE DATABASE " + database)
      } finally {
        statement.close()
      }
    }
  }

  private def createTablesAndSourceRows(
      environment: ExternalDorisTlsTestEnvironment,
      database: String): Unit = {
    withConnection(environment.openConnection(database)) { connection =>
      val statement = connection.createStatement()
      try {
        statement.execute(
          "CREATE TABLE source_table (" +
            "id INT, name VARCHAR(32)) " +
            "DUPLICATE KEY(id) " +
            "DISTRIBUTED BY HASH(id) BUCKETS 1 " +
            "PROPERTIES('replication_num'='1')")
        statement.execute(
          "CREATE TABLE sink_table (" +
            "id INT, name VARCHAR(32), entry_api VARCHAR(16), read_mode VARCHAR(16)) " +
            "DUPLICATE KEY(id) " +
            "DISTRIBUTED BY HASH(id) BUCKETS 1 " +
            "PROPERTIES('replication_num'='1')")
        statement.execute(
          "INSERT INTO source_table VALUES " +
            "(1, 'alpha'), (2, 'beta'), (3, 'gamma')")
      } finally {
        statement.close()
      }
    }
  }

  private def assertSinkRows(
      environment: ExternalDorisTlsTestEnvironment,
      database: String): Unit = {
    val actual = ArrayBuffer.empty[String]
    withConnection(environment.openConnection(database)) { connection =>
      val statement = connection.createStatement()
      try {
        val resultSet = statement.executeQuery(
          "SELECT id, name, entry_api, read_mode " +
            "FROM sink_table ORDER BY id, entry_api, read_mode")
        try {
          while (resultSet.next()) {
            actual += Seq(
              resultSet.getInt("id").toString,
              resultSet.getString("name"),
              resultSet.getString("entry_api"),
              resultSet.getString("read_mode")).mkString(",")
          }
        } finally {
          resultSet.close()
        }
      } finally {
        statement.close()
      }
    }

    val expected = Arrays.asList(
      "1,alpha,dataframe,arrow",
      "1,alpha,dataframe,thrift",
      "1,alpha,rdd,thrift",
      "1,alpha,sql,arrow",
      "2,beta,dataframe,arrow",
      "2,beta,dataframe,thrift",
      "2,beta,rdd,thrift",
      "2,beta,sql,arrow",
      "3,gamma,dataframe,arrow",
      "3,gamma,dataframe,thrift",
      "3,gamma,rdd,thrift",
      "3,gamma,sql,arrow")
    assertEquals(expected, actual.asJava)
  }

  private def cleanup(
      session: SparkSession,
      environment: ExternalDorisTlsTestEnvironment,
      database: String,
      databaseCreated: Boolean,
      primaryFailure: Throwable): Unit = {
    var cleanupFailure: Throwable = null
    if (session != null) {
      try {
        session.stop()
      } catch {
        case failure: Throwable => cleanupFailure = failure
      }
    }

    if (databaseCreated) {
      try {
        withConnection(environment.openConnection()) { connection =>
          val statement = connection.createStatement()
          try {
            statement.execute("DROP DATABASE " + database)
          } finally {
            statement.close()
          }
        }
      } catch {
        case failure: Throwable =>
          if (cleanupFailure == null) {
            cleanupFailure = failure
          } else {
            cleanupFailure.addSuppressed(failure)
          }
      }
    }

    if (cleanupFailure != null) {
      if (primaryFailure == null) {
        throw cleanupFailure
      }
      primaryFailure.addSuppressed(cleanupFailure)
    }
  }

  private def withConnection(connection: Connection)(action: Connection => Unit): Unit = {
    try {
      action(connection)
    } finally {
      connection.close()
    }
  }
}
