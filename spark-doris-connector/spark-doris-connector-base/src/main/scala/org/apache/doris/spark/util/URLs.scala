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

package org.apache.doris.spark.util

object URLs {

  private val HTTP_SCHEMA = "http"

  private val HTTPS_SCHEMA = "https"

  private def schema(enableHttps: Boolean): String = if (enableHttps) HTTPS_SCHEMA else HTTP_SCHEMA

  def aliveBackend(feNode: String, enableHttps: Boolean = false) = s"${schema(enableHttps)}://$feNode/api/backends?is_alive=true"

  def aliveBackend(host: String, port: Int, enableHttps: Boolean) = s"${schema(enableHttps)}://$host:$port/api/backends?is_alive=true"

  def tableSchema(feNode: String, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/$table/_schema"

  def tableSchema(host: String, port: Int, database: String, table: String, enableHttps: Boolean): String =
    tableSchema(s"$host:$port", database, table, enableHttps)

  def queryPlan(feNode: String, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/$table/_query_plan"

  def queryPlan(host: String, port: Int, database: String, table: String, enableHttps: Boolean): String =
    queryPlan(s"$host:$port", database, table, enableHttps)

  def streamLoad(feNode: String, database: String, table: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/$table/_stream_load"

  def streamLoad(host: String, port: Int, database: String, table: String, enableHttps: Boolean): String =
    streamLoad(s"$host:$port", database, table, enableHttps)

  def streamLoad2PC(feNode: String, database: String, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$feNode/api/$database/_stream_load_2pc"

  def streamLoad2PC(host: String, port: Int, database: String, enableHttps: Boolean): String =
    streamLoad2PC(s"$host:$port", database, enableHttps)

  def getFrontEndNodes(host: String, port: Int, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$host:$port/rest/v2/manager/node/frontends"

  def copyIntoUpload(host: String, port: Int, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$host:$port/copy/upload"

  def copyIntoQuery(host: String, port: Int, enableHttps: Boolean = false) =
    s"${schema(enableHttps)}://$host:$port/copy/query"

}
