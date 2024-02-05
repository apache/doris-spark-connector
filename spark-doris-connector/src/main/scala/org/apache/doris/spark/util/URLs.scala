package org.apache.doris.spark.util

object URLs {

  private val HTTP_SCHEMA = "http"

  private val HTTPS_SCHEMA = "https"

  private def schema(enableHttps: Boolean): String = if (enableHttps) HTTPS_SCHEMA else HTTP_SCHEMA

  def aliveBackend(feNode: String, enableHttps: Boolean = false) = s"${schema(enableHttps)}://$feNode/api/backends?is_alive=true"

  def tableSchema(feNode: String, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/$table/_schema"

  def queryPlan(feNode: String, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/$table/_query_plan"

  def streamLoad(feNode: String, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/$table/_stream_load"

  def streamLoad2PC(feNode: String, database: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/_stream_load_2pc"

}
