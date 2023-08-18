package org.apache.doris.spark.sql

import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, DriverWrapper}
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, Driver, DriverManager}
import java.util.Properties
import scala.collection.JavaConverters.enumerationAsScalaIteratorConverter
import scala.util.Try


object JdbcUtils {
  private val logger: Logger = LoggerFactory.getLogger("org.apache.doris.spark.sql.JdbcUtils")


  private def createConnectionFactory(driverClass: String, url: String, user: String, password: String): Connection = {
    DriverRegistry.register(driverClass)
    val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
      case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
      case d if d.getClass.getCanonicalName == driverClass => d
    }.getOrElse {
      throw new IllegalStateException(
        s"Did not find registered driver with class $driverClass")
    }

    val connectionProperties: Properties = new Properties()
    connectionProperties.setProperty("user", user)
    connectionProperties.setProperty("password", password)
    driver.connect(url, connectionProperties)
  }
  
  def truncateAndCreateTableFromSchema(driverClass: String, url: String, user: String, password: String,
                                        databaseName: String, tableName: String, schema: StructType): Unit = {
    val connection = createConnectionFactory(driverClass, url, user, password)
    val statement = connection.createStatement
    try {
      statement.setQueryTimeout(30)
      if(
        Try {
          statement.executeUpdate(truncateQuery(databaseName, tableName))
        }.isFailure
      ) statement.executeUpdate(createTableQueryFromDataFrame(databaseName, tableName, schema))
    } finally {
      statement.close()
      connection.close()
    }
  }


  private def createTableQueryFromDataFrame(database: String, tableName: String, schema: StructType): String = {
    val fields = schema
      .zipWithIndex
      .map { case (schema, index) =>
        (schema.name, if (index < 3 && schema.dataType.simpleString == "string") "varchar(65533)" else schema.dataType.simpleString)
      }

    val sql =
      s"""
         |CREATE TABLE IF NOT EXISTS `${database}`.`${tableName}` (
         |  ${fields.map(field => s"`${field._1}` ${field._2}").mkString(", ")}
         |)
         |ENGINE=OLAP
         |DUPLICATE KEY(${fields.take(3).map(field => s"`${field._1}`").mkString(", ")})
         |DISTRIBUTED BY HASH(${fields.take(3).map(field => s"`${field._1}`").mkString(", ")}) BUCKETS auto
         |PROPERTIES (
         |  "replication_allocation" = "tag.location.default: 1",
         |  "compression" = "zstd"
         |)
         |""".stripMargin
    logger.warn(sql)
    sql
  }

  private def truncateQuery(database: String, tableName: String): String = {
    val sql =
      s"""
         |TRUNCATE TABLE `${database}`.`${tableName}`
         |""".stripMargin
    logger.warn(sql)
    sql
  }

}
