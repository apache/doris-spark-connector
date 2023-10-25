package org.apache.doris.spark.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties

object JdbcUtils {

  def getJdbcUrl(host: String, port: Int): String = s"jdbc:mysql://$host:$port/information_schema"

  def getConnection(url: String, props: Properties): Connection = {

    DriverManager.getConnection(url, props)
  }

  def getTruncateQuery(table: String): String = s"TRUNCATE TABLE $table"

}
