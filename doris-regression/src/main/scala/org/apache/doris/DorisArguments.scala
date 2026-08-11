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

package org.apache.doris

final class DorisArguments private (options: Map[String, String]) {
  import DorisArguments._

  def feAddress: String = required(FE_ADDRESS)

  def readTableIdentifier: String = required(READ_TABLE_IDENTIFIER)

  def writeTableIdentifier: String = required(WRITE_TABLE_IDENTIFIER)

  def database: String = required(DATABASE)

  def user: String = required(USER)

  def password: String = required(PASSWORD)

  def tlsOptions: Map[String, String] = options.collect {
    case (name, value) if TLS_ARGUMENT_NAMES.contains(name) =>
      TLS_ARGUMENT_NAMES(name) -> value
  }

  private def required(name: String): String = {
    options.getOrElse(name, throw new IllegalArgumentException(s"Missing required argument: $name"))
  }
}

object DorisArguments {
  private val FE_ADDRESS = "--doris-fe-address"
  private val READ_TABLE_IDENTIFIER = "--doris-read-table-identifier"
  private val WRITE_TABLE_IDENTIFIER = "--doris-write-table-identifier"
  private val DATABASE = "--doris-database"
  private val USER = "--doris-user"
  private val PASSWORD = "--doris-password"

  private val TLS_ARGUMENT_NAMES = Map(
    "--doris-enable-tls" -> "doris.enable.tls",
    "--doris-tls-ca-certificate-path" -> "doris.tls.ca-certificate-path",
    "--doris-tls-skip-hostname-verification" -> "doris.tls.skip-hostname-verification",
    "--doris-tls-excluded-protocols" -> "doris.tls.excluded-protocols"
  )

  private val ARGUMENT_NAMES = TLS_ARGUMENT_NAMES.keySet ++ Set(
    FE_ADDRESS,
    READ_TABLE_IDENTIFIER,
    WRITE_TABLE_IDENTIFIER,
    DATABASE,
    USER,
    PASSWORD
  )

  def parse(args: Array[String]): DorisArguments = {
    require(args.length % 2 == 0, "Arguments must be name-value pairs")

    val options = args.grouped(2).foldLeft(Map.empty[String, String]) { (parsed, argument) =>
      val name = argument(0)
      require(ARGUMENT_NAMES.contains(name), s"Unknown argument: $name")
      require(!parsed.contains(name), s"Duplicate argument: $name")
      parsed.updated(name, argument(1))
    }
    new DorisArguments(options)
  }
}
