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
import org.apache.doris.spark.rest.models.DataModel
import org.apache.doris.spark.sparkContextFunctions
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.fail
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.slf4j.LoggerFactory

import java.sql.{Date, Timestamp}

object DorisReaderITCase {
  @Parameterized.Parameters(name = "readMode: {0}, flightSqlPort: {1}")
  def parameters(): java.util.Collection[Array[AnyRef]] = {
    import java.util.Arrays
    Arrays.asList(
      Array("thrift": java.lang.String, -1: java.lang.Integer),
      Array("arrow": java.lang.String, 9611: java.lang.Integer)
    )
  }
}

@RunWith(classOf[Parameterized])
class DorisReaderITCase(readMode: String, flightSqlPort: Int) extends AbstractContainerTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[DorisReaderITCase])

  val DATABASE = "test_doris_read"
  val TABLE_READ = "tbl_read"
  val TABLE_READ_TBL = "tbl_read_tbl"
  val TABLE_READ_TBL_ALL_TYPES = "tbl_read_tbl_all_types"
  val TABLE_READ_TBL_BIT_MAP = "tbl_read_tbl_bitmap"

  @Test
  @throws[Exception]
  def testRddSource(): Unit = {

    initializeTable(TABLE_READ, DataModel.DUPLICATE)
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rddSource")
    val sc = new SparkContext(sparkConf)
    // sc.setLogLevel("DEBUG")
    val dorisSparkRDD = sc.dorisRDD(
      tableIdentifier = Some(DATABASE + "." + TABLE_READ),
      cfg = Some(Map(
        "doris.fenodes" -> getFenodes,
        "doris.request.auth.user" -> getDorisUsername,
        "doris.request.auth.password" -> getDorisPassword,
        "doris.fe.init.fetch" -> "false",
        "doris.read.mode" -> readMode,
        "doris.read.arrow-flight-sql.port" -> flightSqlPort.toString
      ))
    )
    val result = dorisSparkRDD.collect()
    sc.stop()

    assert(compareCollectResult(Array(Array("doris", 18), Array("spark", 10)), result))
  }

  @Test
  @throws[Exception]
  def testDataFrameSource(): Unit = {
    initializeTable(TABLE_READ_TBL, DataModel.UNIQUE)

    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val dorisSparkDF = session.read
      .format("doris")
      .option("doris.fenodes", getFenodes)
      .option("doris.table.identifier", DATABASE + "." + TABLE_READ_TBL)
      .option("doris.user", getDorisUsername)
      .option("doris.password", getDorisPassword)
      .option("doris.read.mode", readMode)
      .option("doris.read.arrow-flight-sql.port", flightSqlPort.toString)
      .load()

    val result = dorisSparkDF.collect().toList.toString()
    session.stop()
    assert("List([doris,18], [spark,10])".equals(result))
  }

  @Test
  @throws[Exception]
  def testSQLSource(): Unit = {
    initializeTable(TABLE_READ_TBL, DataModel.UNIQUE_MOR)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_source
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_READ_TBL}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}",
         | "doris.read.mode"="${readMode}",
         | "doris.read.arrow-flight-sql.port"="${flightSqlPort}"
         |)
         |""".stripMargin)

    val result = session.sql(
      """
        |select  name,age from test_source
        |""".stripMargin).collect().toList.toString()
    session.stop()

    assert("List([doris,18], [spark,10])".equals(result))
  }

  private def initializeTable(table: String, dataModel: DataModel): Unit = {
    val max = if (DataModel.AGGREGATE == dataModel) "MAX" else ""
    val morProps = if (!(DataModel.UNIQUE_MOR == dataModel)) "" else ",\"enable_unique_key_merge_on_write\" = \"false\""
    val model = if (dataModel == DataModel.UNIQUE_MOR) DataModel.UNIQUE.toString else dataModel.toString
    ContainerUtils.executeSQLStatement(
      getDorisQueryConnection,
      LOG,
      String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
      String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
      String.format("CREATE TABLE %s.%s ( \n"
        + "`name` varchar(256),\n"
        + "`age` int %s\n"
        + ") "
        + " %s KEY(`name`) "
        + " DISTRIBUTED BY HASH(`name`) BUCKETS 1\n"
        + "PROPERTIES ("
        + "\"replication_num\" = \"1\"\n" + morProps + ")", DATABASE, table, max, model),
      String.format("insert into %s.%s  values ('doris',18)", DATABASE, table),
      String.format("insert into %s.%s  values ('spark',10)", DATABASE, table))
  }

  private def compareCollectResult(a1: Array[AnyRef], a2: Array[AnyRef]): Boolean = if (a1.length == a2.length) {
    for (idx <- 0 until a1.length) {
      if (!a1(idx).isInstanceOf[Array[AnyRef]] || !a2(idx).isInstanceOf[Array[AnyRef]]) return false
      val arr1 = a1(idx).asInstanceOf[Array[AnyRef]]
      val arr2 = a2(idx).asInstanceOf[Array[AnyRef]]
      if (arr1.length != arr2.length) return false
      for (idx2 <- 0 until arr2.length) {
        if (arr1(idx2) != arr2(idx2)) return false
      }
    }
    true
  } else false

  @Test
  @throws[Exception]
  def testSQLSourceWithCondition(): Unit = {
    initializeTable(TABLE_READ_TBL, DataModel.AGGREGATE)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_source
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_READ_TBL}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}",
         | "doris.read.mode"="${readMode}",
         | "doris.read.arrow-flight-sql.port"="${flightSqlPort}"
         |)
         |""".stripMargin)

    val result = session.sql(
      """
        |select name,age from test_source where age = 18
        |""".stripMargin).collect().toList.toString()
    session.stop()

    assert("List([doris,18])".equals(result))
  }

  @Test
  def testReadAllType(): Unit = {
    val sourceInitSql: Array[String] = ContainerUtils.parseFileContentSQL("container/read_all_type.sql")
    ContainerUtils.executeSQLStatement(getDorisQueryConnection, LOG, sourceInitSql: _*)

    val session = SparkSession.builder().master("local[*]").getOrCreate()
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
    session.sql("desc test_source").show(true);
    val actualData = session.sql(
      """
        |select * from test_source order by id
        |""".stripMargin).collect()
    session.stop()

    val expectedData = Array(
      Row(1, true, 127, 32767, 2147483647, 9223372036854775807L, "170141183460469231731687303715884105727",
        3.14f, 2.71828, new java.math.BigDecimal("12345.6789"), Date.valueOf("2025-03-11"), Timestamp.valueOf("2025-03-11 12:34:56"), "A", "Hello, Doris!", "This is a string",
        """["Alice","Bob"]""", Map("key1" -> "value1", "key2" -> "value2"), """{"name":"Tom","age":30}""",
        """{"key":"value"}""", """{"type":"variant","data":123}"""),
      Row(2, false, -128, -32768, -2147483648, -9223372036854775808L, "-170141183460469231731687303715884105728",
        -1.23f, 0.0001, new java.math.BigDecimal("-9999.9999"), Date.valueOf("2024-12-25"), Timestamp.valueOf("2024-12-25 23:59:59"), "B", "Doris Test", "Another string!",
        """["Charlie","David"]""", Map("k1" -> "v1", "k2" -> "v2"), """{"name":"Jerry","age":25}""",
        """{"status":"ok"}""", """{"data":[1,2,3]}"""),
      Row(3, true, 0, 0, 0, 0, "0",
        0.0f, 0.0, new java.math.BigDecimal("0.0000"), Date.valueOf("2023-06-15"), Timestamp.valueOf("2023-06-15 08:00:00"), "C", "Test Doris", "Sample text",
        """["Eve","Frank"]""", Map("alpha" -> "beta"), """{"name":"Alice","age":40}""",
        """{"nested":{"key":"value"}}""", """{"variant":"test"}"""),
      Row(4, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null,
        null, null, null, null, null)
    )

    val differences = actualData.zip(expectedData).zipWithIndex.flatMap {
      case ((actualRow, expectedRow), rowIndex) =>
        actualRow.toSeq.zip(expectedRow.toSeq).zipWithIndex.collect {
          case ((actualValue, expectedValue), colIndex)
            if actualValue != expectedValue =>
            s"Row $rowIndex, Column $colIndex: actual=$actualValue, expected=$expectedValue"
        }
    }

    if (differences.nonEmpty) {
      fail(s"Data mismatch found:\n${differences.mkString("\n")}")
    }
  }

  @Test
  def testBitmapRead(): Unit = {
    val sourceInitSql: Array[String] = ContainerUtils.parseFileContentSQL("container/read_bitmap.sql")
    ContainerUtils.executeSQLStatement(getDorisQueryConnection, LOG, sourceInitSql: _*)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_source
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_READ_TBL_BIT_MAP}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}",
         | "doris.read.mode"="${readMode}",
         | "doris.read.arrow-flight-sql.port"="${flightSqlPort}"
         |)
         |""".stripMargin)
    session.sql("desc test_source").show(true);
    val actualData = session.sql(
      """
        |select * from test_source order by hour
        |""".stripMargin).collect()
    session.stop()

    assert("List([20200622,1,Read unsupported], [20200622,2,Read unsupported], [20200622,3,Read unsupported])".equals(actualData.toList.toString()))
  }

  @Test
  def testBitmapRead2String(): Unit = {
    if(readMode.equals("thrift")){
      return
    }
    val sourceInitSql: Array[String] = ContainerUtils.parseFileContentSQL("container/read_bitmap.sql")
    ContainerUtils.executeSQLStatement(getDorisQueryConnection, LOG, sourceInitSql: _*)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_source
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_READ_TBL_BIT_MAP}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}",
         | "doris.read.mode"="${readMode}",
         | "doris.read.arrow-flight-sql.port"="${flightSqlPort}",
         | "doris.read.bitmap-to-string"="true"
         |)
         |""".stripMargin)
    session.sql("desc test_source").show(true);
    val actualData = session.sql(
      """
        |select * from test_source order by hour
        |""".stripMargin).collect()
    session.stop()

    assert("List([20200622,1,243], [20200622,2,1,2,3,4,5,434543], [20200622,3,287667876573])"
      .equals(actualData.toList.toString()))
  }

  @Test
  def testBitmapRead2Base64(): Unit = {
    if(readMode.equals("thrift")){
      return
    }
    val sourceInitSql: Array[String] = ContainerUtils.parseFileContentSQL("container/read_bitmap.sql")
    ContainerUtils.executeSQLStatement(getDorisQueryConnection, LOG, sourceInitSql: _*)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    session.sql(
      s"""
         |CREATE TEMPORARY VIEW test_source
         |USING doris
         |OPTIONS(
         | "table.identifier"="${DATABASE + "." + TABLE_READ_TBL_BIT_MAP}",
         | "fenodes"="${getFenodes}",
         | "user"="${getDorisUsername}",
         | "password"="${getDorisPassword}",
         | "doris.read.mode"="${readMode}",
         | "doris.read.arrow-flight-sql.port"="${flightSqlPort}",
         | "doris.read.bitmap-to-base64"="true"
         |)
         |""".stripMargin)
    session.sql("desc test_source").show(true);
    val actualData = session.sql(
      """
        |select * from test_source order by hour
        |""".stripMargin).collect()
    session.stop()

    assert("List([20200622,1,AfMAAAA=], [20200622,2,AjswAQABAAAEAAYAAAABAAEABABvoQ==], [20200622,3,A91yV/pCAAAA])"
      .equals(actualData.toList.toString()))
  }

  @Test
  def testReadPushDownProject(): Unit = {
    val sourceInitSql: Array[String] = ContainerUtils.parseFileContentSQL("container/read_all_type.sql")
    ContainerUtils.executeSQLStatement(getDorisQueryConnection, LOG, sourceInitSql: _*)
    val session = SparkSession.builder().master("local[*]").getOrCreate()
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

    val intFilter = session.sql(
      """
        |select id,c1,c2 from test_source where id = 2 and c1 = false and c4 != 3
        |""".stripMargin).collect()

    assert("List([2,false,-128])".equals(intFilter.toList.toString()))

    val floatFilter = session.sql(
      """
        |select id,c3,c4,c7,c9 from test_source where c7 > 0 and c7 < 3.15
        |""".stripMargin).collect()

    assert("List([1,32767,2147483647,3.14,12345.6789])".equals(floatFilter.toList.toString()))

    val dateFilter = session.sql(
      """
        |select id,c10,c11 from test_source where c10 = '2025-03-11' and c13 like 'Hello%'
        |""".stripMargin).collect()

    assert("List([1,2025-03-11,2025-03-11 12:34:56.0])".equals(dateFilter.toList.toString()))

    val datetimeFilter = session.sql(
      """
        |select id,c11,c12 from test_source where c10 < '2025-03-11' and c11 = '2024-12-25 23:59:59'
        |""".stripMargin).collect()

    assert("List([2,2024-12-25 23:59:59.0,B])".equals(datetimeFilter.toList.toString()))

    val stringFilter = session.sql(
      """
        |select id,c13,c14 from test_source where c11 >= '2024-12-25 23:59:59' and c13 = 'Hello, Doris!'
        |""".stripMargin).collect()

    assert("List([1,Hello, Doris!,This is a string])".equals(stringFilter.toList.toString()))

    val nullFilter = session.sql(
      """
        |select id,c13,c14 from test_source where c14 is null
        |""".stripMargin).collect()

    assert("List([4,null,null])".equals(nullFilter.toList.toString()))

    val notNullFilter = session.sql(
      """
        |select id from test_source where c15 is not null and c12 in ('A', 'B')
        |""".stripMargin).collect()

    assert("List([1], [2])".equals(notNullFilter.toList.toString()))

    val likeFilter = session.sql(
      """
        |select id from test_source where c19 like '%variant%' and c13 like 'Test%'
        |""".stripMargin).collect()

    assert("List([3])".equals(likeFilter.toList.toString()))
    session.stop()
  }
}
