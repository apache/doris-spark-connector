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
import org.junit.{Assert, Ignore, Test}

import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._

@Ignore
class SchemaUtilsTest {

  @Test
  def rowColumnValueTest(): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    val df = spark.createDataFrame(Seq(
      (1, Date.valueOf("2023-09-08"), Timestamp.valueOf("2023-09-08 17:00:00"), Array(1, 2, 3), Map[String, String]("a" -> "1"))
    )).toDF("c1", "c2", "c3", "c4", "c5")

    val schema = df.schema

    df.queryExecution.toRdd.foreach(row => {

      val fields = schema.fields
      Assert.assertEquals(1, SchemaUtils.rowColumnValue(row, 0, fields(0).dataType))
      Assert.assertEquals("2023-09-08", SchemaUtils.rowColumnValue(row, 1, fields(1).dataType))
      Assert.assertEquals("2023-09-08 17:00:00.0", SchemaUtils.rowColumnValue(row, 2, fields(2).dataType))
      Assert.assertEquals("[1,2,3]", SchemaUtils.rowColumnValue(row, 3, fields(3).dataType))
      println(SchemaUtils.rowColumnValue(row, 4, fields(4).dataType))
      Assert.assertEquals(Map("a" -> "1").asJava, SchemaUtils.rowColumnValue(row, 4, fields(4).dataType))

    })

  }

}
