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
