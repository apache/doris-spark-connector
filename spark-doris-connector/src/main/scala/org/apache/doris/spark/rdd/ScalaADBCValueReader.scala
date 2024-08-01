package org.apache.doris.spark.rdd

import org.apache.arrow.adbc.core.{AdbcConnection, AdbcDriver, AdbcStatement}
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
import org.apache.arrow.flight.Location
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.doris.spark.cfg.{ConfigurationOptions, Settings, SparkSettings}
import org.apache.doris.spark.exception.ShouldNeverHappenException
import org.apache.doris.spark.rest.{PartitionDefinition, RestService}
import org.apache.doris.spark.serialization.RowBatch
import org.apache.doris.spark.sql.{SchemaUtils, Utils}
import org.apache.doris.spark.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import scala.collection.mutable

class ScalaADBCValueReader(partition: PartitionDefinition, settings: Settings) extends AbstractValueReader with Logging {

  private[this] val eos: AtomicBoolean = new AtomicBoolean(false)

  private lazy val schema = RestService.getSchema(SparkSettings.fromProperties(settings.asProperties()), log)

  private lazy val conn: AdbcConnection = {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val driver = new FlightSqlDriver(allocator)
    val params = mutable.HashMap[String, AnyRef]().asJava
    AdbcDriver.PARAM_URI.set(params, Location.forGrpcInsecure(
      settings.getProperty(ConfigurationOptions.DORIS_FENODES).split(":")(0),
      settings.getIntegerProperty(ConfigurationOptions.DORIS_ARROW_FLIGHT_SQL_PORT)
    ).getUri.toString)
    AdbcDriver.PARAM_USERNAME.set(params, settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_USER))
    AdbcDriver.PARAM_PASSWORD.set(params, settings.getProperty(ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD))
    val database = driver.open(params)
    database.connect()
  }

  private lazy val stmt: AdbcStatement = conn.createStatement()

  private lazy val queryResult: AdbcStatement.QueryResult = {
    val flightSql = Utils.generateQueryStatement(settings.getProperty(ConfigurationOptions.DORIS_READ_FIELD, "*").split(","),
      settings.getProperty(SchemaUtils.DORIS_BITMAP_COLUMNS, "").split(","),
      settings.getProperty(SchemaUtils.DORIS_HLL_COLUMNS, "").split(","),
      s"`${partition.getDatabase}`.`${partition.getTable}`",
      settings.getProperty(ConfigurationOptions.DORIS_FILTER_QUERY, ""),
      Some(partition)
    )
    log.info(s"flightSql: $flightSql")
    stmt.setSqlQuery(flightSql)
    stmt.executeQuery()
  }

  private lazy val arrowReader: ArrowReader = queryResult.getReader

  override def hasNext: Boolean = {
    if (!eos.get && (rowBatch == null || !rowBatch.hasNext)) {
      eos.set(!arrowReader.loadNextBatch())
      if (!eos.get) {
        rowBatch = new RowBatch(arrowReader, schema)
      }
    }
    !eos.get
  }

  /**
   * get next value.
   *
   * @return next value
   */
  override def next: AnyRef = {
    if (!hasNext) {
      logError(SHOULD_NOT_HAPPEN_MESSAGE)
      throw new ShouldNeverHappenException
    }
    rowBatch.next
  }

  override def close(): Unit = {
    if (rowBatch != null) {
      rowBatch.close()
    }
    if (arrowReader != null) {
      arrowReader.close()
    }
    if (queryResult != null) {
      queryResult.close()
    }
    if (stmt != null) {
      stmt.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

}
