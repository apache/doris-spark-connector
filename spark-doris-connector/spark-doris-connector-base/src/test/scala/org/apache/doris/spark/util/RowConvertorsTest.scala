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

import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.{ArrayType, DataTypes, Decimal, DecimalType, MapType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.Assert
import org.junit.jupiter.api.Test

import java.sql.{Date, Timestamp}
import java.time.LocalDate
import java.util

class RowConvertorsTest {

  @Test def convertToCsv(): Unit = {

    val row = InternalRow(
      1,
      2.3.toFloat,
      4.5,
      6.toShort,
      7L,
      Decimal(BigDecimal(8910.11), 20, 4),
      CatalystTypeConverters.convertToCatalyst(Date.valueOf("2024-01-01")),
      CatalystTypeConverters.convertToCatalyst(Timestamp.valueOf("2024-01-01 12:34:56")),
      ArrayData.toArrayData(Array(1, 2, 3)),
      CatalystTypeConverters.convertToCatalyst(Map[String, String]("a" -> "1")),
      InternalRow(UTF8String.fromString("a"), 1),
      UTF8String.fromString("test")
    )

    val schema = StructType(Seq(
      StructField("c0", DataTypes.IntegerType),
      StructField("c1", DataTypes.FloatType),
      StructField("c2", DataTypes.DoubleType),
      StructField("c3", DataTypes.ShortType),
      StructField("c4", DataTypes.LongType),
      StructField("c5", DecimalType(20,4)),
      StructField("c6", DataTypes.DateType),
      StructField("c7", DataTypes.TimestampType),
      StructField("c8", ArrayType(DataTypes.IntegerType)),
      StructField("c9", MapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("c10", StructType(Seq(StructField("a", DataTypes.StringType), StructField("b", DataTypes.IntegerType)))),
      StructField("c11", DataTypes.StringType)
    ))
    val res = RowConvertors.convertToCsv(row, schema, ",")
    Assert.assertEquals("1,2.3,4.5,6,7,8910.1100,2024-01-01,2024-01-01 12:34:56.0,[1,2,3],{\"a\":\"1\"},{\"a\":\"a\",\"b\":1},test", res)
  }

  @Test def convertToJson(): Unit = {

    val row = InternalRow(
      1,
      2.3.toFloat,
      4.5,
      6.toShort,
      7L,
      Decimal(BigDecimal(8910.11), 20, 4),
      CatalystTypeConverters.convertToCatalyst(Date.valueOf("2024-01-01")),
      CatalystTypeConverters.convertToCatalyst(Timestamp.valueOf("2024-01-01 12:34:56")),
      ArrayData.toArrayData(Array(1, 2, 3)),
      CatalystTypeConverters.convertToCatalyst(Map[String, String]("a" -> "1")),
      InternalRow(UTF8String.fromString("a"), 1),
      UTF8String.fromString("test")
    )

    val schema = StructType(Seq(
      StructField("c0", DataTypes.IntegerType),
      StructField("c1", DataTypes.FloatType),
      StructField("c2", DataTypes.DoubleType),
      StructField("c3", DataTypes.ShortType),
      StructField("c4", DataTypes.LongType),
      StructField("c5", DecimalType(20,4)),
      StructField("c6", DataTypes.DateType),
      StructField("c7", DataTypes.TimestampType),
      StructField("c8", ArrayType(DataTypes.IntegerType)),
      StructField("c9", MapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("c10", StructType(Seq(StructField("a", DataTypes.StringType), StructField("b", DataTypes.IntegerType)))),
      StructField("c11", DataTypes.StringType)
    ))
    val res = RowConvertors.convertToJson(row, schema)
    Assert.assertEquals("{\"c0\":1,\"c1\":2.3,\"c10\":\"{\\\"a\\\":\\\"a\\\",\\\"b\\\":1}\",\"c11\":\"test\"," +
      "\"c2\":4.5,\"c3\":6,\"c4\":7,\"c5\":8910.1100,\"c6\":\"2024-01-01\"," +
      "\"c7\":\"2024-01-01 12:34:56.0\",\"c8\":\"[1,2,3]\",\"c9\":\"{\\\"a\\\":\\\"1\\\"}\"}", res)

  }

  @Test def convertValue(): Unit = {

    Assert.assertTrue(RowConvertors.convertValue(1, DataTypes.IntegerType, false).isInstanceOf[Int])
    Assert.assertTrue(RowConvertors.convertValue(2.3.toFloat, DataTypes.FloatType, false).isInstanceOf[Float])
    Assert.assertTrue(RowConvertors.convertValue(4.5, DataTypes.DoubleType, false).isInstanceOf[Double])
    Assert.assertTrue(RowConvertors.convertValue(6.toShort, DataTypes.ShortType, false).isInstanceOf[Short])
    Assert.assertTrue(RowConvertors.convertValue(7L, DataTypes.LongType, false).isInstanceOf[Long])
    Assert.assertTrue(RowConvertors.convertValue(Decimal(BigDecimal(8910.11), 20, 4), DecimalType(20, 4), false).isInstanceOf[Decimal])
    Assert.assertTrue(RowConvertors.convertValue(Date.valueOf("2024-01-01"), DataTypes.DateType, false).isInstanceOf[Int])
    Assert.assertTrue(RowConvertors.convertValue(LocalDate.now(), DataTypes.DateType, true).isInstanceOf[Int])
    Assert.assertTrue(RowConvertors.convertValue(Timestamp.valueOf("2024-01-01 12:34:56"), DataTypes.TimestampType, false).isInstanceOf[Long])
    val map = new util.HashMap[String, String]()
    map.put("a", "1")
    Assert.assertTrue(RowConvertors.convertValue(map, MapType(DataTypes.StringType, DataTypes.StringType), false).isInstanceOf[MapData])
    Assert.assertTrue(RowConvertors.convertValue("test", DataTypes.StringType, false).isInstanceOf[UTF8String])

  }

  @Test def convertValueArrayTypeInt(): Unit = {
    // Test ARRAY<INT> conversion
    val intList = new util.ArrayList[Any]()
    intList.add(1)
    intList.add(2)
    intList.add(3)
    
    val result = RowConvertors.convertValue(intList, ArrayType(DataTypes.IntegerType), false)
    Assert.assertTrue(result.isInstanceOf[ArrayData])
    
    val arrayData = result.asInstanceOf[ArrayData]
    Assert.assertEquals(3, arrayData.numElements())
    Assert.assertEquals(1, arrayData.getInt(0))
    Assert.assertEquals(2, arrayData.getInt(1))
    Assert.assertEquals(3, arrayData.getInt(2))
  }

  @Test def convertValueArrayTypeString(): Unit = {
    // Test ARRAY<STRING> conversion
    val stringList = new util.ArrayList[Any]()
    stringList.add("hello")
    stringList.add("world")
    
    val result = RowConvertors.convertValue(stringList, ArrayType(DataTypes.StringType), false)
    Assert.assertTrue(result.isInstanceOf[ArrayData])
    
    val arrayData = result.asInstanceOf[ArrayData]
    Assert.assertEquals(2, arrayData.numElements())
    Assert.assertEquals(UTF8String.fromString("hello"), arrayData.getUTF8String(0))
    Assert.assertEquals(UTF8String.fromString("world"), arrayData.getUTF8String(1))
  }

  @Test def convertValueArrayTypeWithNull(): Unit = {
    // Test ARRAY with null elements
    val list = new util.ArrayList[Any]()
    list.add(1)
    list.add(null)
    list.add(3)
    
    val result = RowConvertors.convertValue(list, ArrayType(DataTypes.IntegerType), false)
    Assert.assertTrue(result.isInstanceOf[ArrayData])
    
    val arrayData = result.asInstanceOf[ArrayData]
    Assert.assertEquals(3, arrayData.numElements())
    Assert.assertEquals(1, arrayData.getInt(0))
    Assert.assertTrue(arrayData.isNullAt(1))
    Assert.assertEquals(3, arrayData.getInt(2))
  }

  @Test def convertValueArrayTypeNested(): Unit = {
    // Test ARRAY<ARRAY<INT>> nested array
    val innerList1 = new util.ArrayList[Any]()
    innerList1.add(1)
    innerList1.add(2)
    
    val innerList2 = new util.ArrayList[Any]()
    innerList2.add(3)
    innerList2.add(4)
    innerList2.add(5)
    
    val outerList = new util.ArrayList[Any]()
    outerList.add(innerList1)
    outerList.add(innerList2)
    
    val nestedArrayType = ArrayType(ArrayType(DataTypes.IntegerType))
    val result = RowConvertors.convertValue(outerList, nestedArrayType, false)
    Assert.assertTrue(result.isInstanceOf[ArrayData])
    
    val outerArrayData = result.asInstanceOf[ArrayData]
    Assert.assertEquals(2, outerArrayData.numElements())
    
    val innerArrayData1 = outerArrayData.getArray(0).asInstanceOf[ArrayData]
    Assert.assertEquals(2, innerArrayData1.numElements())
    Assert.assertEquals(1, innerArrayData1.getInt(0))
    Assert.assertEquals(2, innerArrayData1.getInt(1))
    
    val innerArrayData2 = outerArrayData.getArray(1).asInstanceOf[ArrayData]
    Assert.assertEquals(3, innerArrayData2.numElements())
    Assert.assertEquals(3, innerArrayData2.getInt(0))
    Assert.assertEquals(4, innerArrayData2.getInt(1))
    Assert.assertEquals(5, innerArrayData2.getInt(2))
  }

  @Test def convertValueArrayTypeEmpty(): Unit = {
    // Test empty array
    val emptyList = new util.ArrayList[Any]()
    
    val result = RowConvertors.convertValue(emptyList, ArrayType(DataTypes.IntegerType), false)
    Assert.assertTrue(result.isInstanceOf[ArrayData])
    
    val arrayData = result.asInstanceOf[ArrayData]
    Assert.assertEquals(0, arrayData.numElements())
  }

  @Test def convertValueArrayTypeTypeMismatch(): Unit = {
    // Test type mismatch: Schema declares StringType but actual data is Integer
    // Should fallback to StringType conversion
    val intList = new util.ArrayList[Any]()
    intList.add(1)
    intList.add(2)
    intList.add(3)
    
    // Schema declares StringType but actual data is Integer
    // This should trigger the fallback mechanism
    val result = RowConvertors.convertValue(intList, ArrayType(DataTypes.StringType), false)
    Assert.assertTrue(result.isInstanceOf[ArrayData])
    
    val arrayData = result.asInstanceOf[ArrayData]
    Assert.assertEquals(3, arrayData.numElements())
    // Should be converted to strings
    Assert.assertEquals(UTF8String.fromString("1"), arrayData.getUTF8String(0))
    Assert.assertEquals(UTF8String.fromString("2"), arrayData.getUTF8String(1))
    Assert.assertEquals(UTF8String.fromString("3"), arrayData.getUTF8String(2))
  }

  @Test def convertValueArrayTypeLong(): Unit = {
    // Test ARRAY<LONG> conversion
    val longList = new util.ArrayList[Any]()
    longList.add(100L)
    longList.add(200L)
    longList.add(300L)
    
    val result = RowConvertors.convertValue(longList, ArrayType(DataTypes.LongType), false)
    Assert.assertTrue(result.isInstanceOf[ArrayData])
    
    val arrayData = result.asInstanceOf[ArrayData]
    Assert.assertEquals(3, arrayData.numElements())
    Assert.assertEquals(100L, arrayData.getLong(0))
    Assert.assertEquals(200L, arrayData.getLong(1))
    Assert.assertEquals(300L, arrayData.getLong(2))
  }

  @Test def convertValueArrayTypeDouble(): Unit = {
    // Test ARRAY<DOUBLE> conversion
    val doubleList = new util.ArrayList[Any]()
    doubleList.add(1.1)
    doubleList.add(2.2)
    doubleList.add(3.3)
    
    val result = RowConvertors.convertValue(doubleList, ArrayType(DataTypes.DoubleType), false)
    Assert.assertTrue(result.isInstanceOf[ArrayData])
    
    val arrayData = result.asInstanceOf[ArrayData]
    Assert.assertEquals(3, arrayData.numElements())
    Assert.assertEquals(1.1, arrayData.getDouble(0), 0.0001)
    Assert.assertEquals(2.2, arrayData.getDouble(1), 0.0001)
    Assert.assertEquals(3.3, arrayData.getDouble(2), 0.0001)
  }

}
