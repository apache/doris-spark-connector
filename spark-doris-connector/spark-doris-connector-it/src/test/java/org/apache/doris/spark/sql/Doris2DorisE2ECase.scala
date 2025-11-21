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

import org.apache.doris.spark.container.AbstractContainerTestBase.getDorisQueryConnection
import org.apache.doris.spark.container.{AbstractContainerTestBase, ContainerUtils}
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.slf4j.LoggerFactory

import java.util

object Doris2DorisE2ECase {
  @Parameterized.Parameters(name = "readMode: {0}, flightSqlPort: {1}")
  def parameters(): java.util.Collection[Array[AnyRef]] = {
    import java.util.Arrays
    Arrays.asList(
      Array("thrift": java.lang.String, -1: java.lang.Integer),
      Array("arrow": java.lang.String, 9611: java.lang.Integer)
    )
  }
}

/**
 * Read Doris to Write Doris.
 */
@RunWith(classOf[Parameterized])
class Doris2DorisE2ECase(readMode: String, flightSqlPort: Int) extends AbstractContainerTestBase{

  private val LOG = LoggerFactory.getLogger(classOf[Doris2DorisE2ECase])
  val DATABASE = "test_doris_e2e"
  val TABLE_READ_TBL_ALL_TYPES = "tbl_read_tbl_all_types"
  val TABLE_WRITE_TBL_ALL_TYPES = "tbl_write_tbl_all_types"

  @Before
  def setUp(): Unit = {
    ContainerUtils.executeSQLStatement(getDorisQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE))
  }

  @Test
  def testAllTypeE2ESQL(): Unit = {
    val sourceInitSql: Array[String] = ContainerUtils.parseFileContentSQL("container/ddl/read_all_type.sql")
    ContainerUtils.executeSQLStatement(getDorisQueryConnection(DATABASE), LOG, sourceInitSql: _*)

    val targetInitSql: Array[String] = ContainerUtils.parseFileContentSQL("container/ddl/write_all_type.sql")
    ContainerUtils.executeSQLStatement(getDorisQueryConnection(DATABASE), LOG, targetInitSql: _*)

    val session = SparkSession.builder().master("local[*]").getOrCreate()
    
    // Drop temporary views if they exist (for parameterized tests that reuse SparkSession)
    try {
      session.sql("DROP TEMPORARY VIEW IF EXISTS test_source")
    } catch {
      case _: Exception => // Ignore if view doesn't exist
    }
    try {
      session.sql("DROP TEMPORARY VIEW IF EXISTS test_sink")
    } catch {
      case _: Exception => // Ignore if view doesn't exist
    }
    
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_source
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_READ_TBL_ALL_TYPES}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}",
         | "doris.read.mode"="${readMode}",
         | "doris.read.arrow-flight-sql.port"="${flightSqlPort}"
         |)
         |""".stripMargin)

    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_sink
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_WRITE_TBL_ALL_TYPES}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}"
         |)
         |""".stripMargin)

    session.sql(
      """
        |insert into test_sink select * from test_source
        |""".stripMargin)
    session.stop()

    val excepted =
      util.Arrays.asList(
        "1,true,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,3.14,2.71828,12345.6789,2025-03-11,2025-03-11T12:34:56,A,Hello, Doris!,This is a string,[\"Alice\", \"Bob\"],{\"key1\":\"value1\", \"key2\":\"value2\"},{\"name\": \"Tom\", \"age\": 30},{\"key\":\"value\"},{\"data\":123,\"type\":\"variant\"}",
        "2,false,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-1.23,1.0E-4,-9999.9999,2024-12-25,2024-12-25T23:59:59,B,Doris Test,Another string!,[\"Charlie\", \"David\"],{\"k1\":\"v1\", \"k2\":\"v2\"},{\"name\": \"Jerry\", \"age\": 25},{\"status\":\"ok\"},{\"data\":[1,2,3]}",
        "3,true,0,0,0,0,0,0.0,0.0,0.0000,2023-06-15,2023-06-15T08:00,C,Test Doris,Sample text,[\"Eve\", \"Frank\"],{\"alpha\":\"beta\"},{\"name\": \"Alice\", \"age\": 40},{\"nested\":{\"key\":\"value\"}},{\"variant\":\"test\"}",
        "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null");

    val query = String.format("select * from %s order by id", TABLE_WRITE_TBL_ALL_TYPES)
    ContainerUtils.checkResult(getDorisQueryConnection(DATABASE), LOG, excepted, query, 20, false)
  }
}
