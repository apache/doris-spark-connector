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

import org.apache.spark.sql.SparkSession
import org.junit.Test

class DorisAnySchemaITCase {

  val dorisFeNodes = "10.148.39.2:8030"
  val dorisUser = "root"
  val dorisPwd = "admin"
  val dorisTable = "example_db.table4"

  @Test
  def jsonDataWriteTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.createDataFrame(Seq(
      (0, 0, "user1", 100),
      (1, 0, "user2", 100),
      (3, 0, "user2", 200)
    )).toDF("siteid", "citycode", "username", "pv")
    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("doris.table.identifier", dorisTable)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("doris.sink.properties.format", "json")
      .option("sink.batch.size", 2)
      .option("sink.max-retries", 2)
      .save()
    spark.stop()
  }

  @Test
  def jsonDataWriteWithPartialUpdateTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.createDataFrame(Seq(
      (0, 0, "user4", 100),
      (1, 0, "user5", 100),
      (3, 0, "user6", 200)
    )).toDF("siteid", "citycode", "username", "pv")
    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("doris.table.identifier", dorisTable)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("doris.sink.properties.format", "json")
      .option("doris.sink.properties.partial_columns", "true")
      .option("doris.write.fields", "siteid,citycode,username")
      .option("sink.batch.size", 2)
      .option("sink.max-retries", 2)
      .save()
    spark.stop()
  }

  @Test
  def jsonDataWriteSqlTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val doris = spark.sql(
      s"""
         |CREATE TEMPORARY VIEW test_lh
         |USING doris
         |OPTIONS(
         | "table.identifier"="${dorisTable}",
         | "fenodes"="${dorisFeNodes}",
         | "user"="${dorisUser}",
         | "password"="${dorisPwd}"
         |)
         |""".stripMargin)
    spark.sql(
      """
        |insert into test_lh values (0, 0, "user1", 100), (1, 0, "user2", 100),(2, 1, "user3", 100),(3, 0, "user2", 200)
        |""".stripMargin)
    spark.stop()
  }

  @Test
  def jsonDataWriteWithPartialUpdateSqlTest(): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val doris = spark.sql(
      s"""
         |CREATE TEMPORARY VIEW test_lh
         |USING doris
         |OPTIONS(
         | "table.identifier"="${dorisTable}",
         | "fenodes"="${dorisFeNodes}",
         | "user"="${dorisUser}",
         | "password"="${dorisPwd}",
         | "doris.sink.properties.format" = "json",
         | "doris.sink.properties.partial_columns" = "true",
         | "doris.write.fields" = "siteid,citycode,username"
         |)
         |""".stripMargin)
    spark.sql(
      """
        | desc test_lh
        |""".stripMargin).show

    spark.sql(
      """
        |insert into test_lh(siteid,citycode,username) values (0, 0, "user1"), (1, 0, "user2")
        |""".stripMargin)
    spark.stop()
  }
}
