package org.apache.doris.spark.client

object URLs {

  private val HTTP_SCHEMA = "http"

  private val HTTPS_SCHEMA = "https"

  private def schema(enableHttps: Boolean): String = if (enableHttps) HTTPS_SCHEMA else HTTP_SCHEMA

  def aliveBackend(feNode: String, enableHttps: Boolean = false) = s"${schema(enableHttps)}://$feNode/api/backends?is_alive=true"

  def tableSchema(host: String, port: Int, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$host:$port/api/$database/$table/_schema"

  def queryPlan(host: String, port: Int, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$host:$port/api/$database/$table/_query_plan"

  def streamLoad(host: String, port: Int, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$host:$port/api/$database/$table/_stream_load"

  def streamLoad2PC(host: String, port: Int, database: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$host:$port/api/$database/_stream_load_2pc"

  def getFrontEndNodes(host: String, port: Int, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$host:$port/rest/v2/manager/node/frontends"

}