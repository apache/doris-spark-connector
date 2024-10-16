package org.apache.doris.spark.util

import java.sql.{Connection, DriverManager}

object JdbcUtils {

  def withConnection[T](host: String, port: Int, user: String, passwd: String)(f: Connection => T): T = {
    val connection = DriverManager.getConnection(s"jdbc:mysql://$host:$port", user, passwd)
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

}
