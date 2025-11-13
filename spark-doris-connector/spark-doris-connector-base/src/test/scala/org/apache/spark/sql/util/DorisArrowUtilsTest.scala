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
// software distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the specific language governing
// permissions and limitations under the License.

package org.apache.spark.sql.util

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.arrow.vector.types.TimeUnit
import org.apache.spark.sql.types.{DataTypes, TimestampType}
import org.junit.Assert
import org.junit.jupiter.api.Test

class DorisArrowUtilsTest {

  @Test
  def testToArrowTypeTimestampNTZ(): Unit = {
    try {
      val timestampNTZClass = Class.forName("org.apache.spark.sql.types.TimestampNTZType$")
      val instance = timestampNTZClass.getField("MODULE$").get(null)
      val timestampNTZType = instance.asInstanceOf[org.apache.spark.sql.types.DataType]

      // Test TimestampNTZType to Arrow type conversion
      val arrowType = DorisArrowUtils.toArrowType(timestampNTZType, null)

      // Should create Arrow Timestamp with null timezone
      Assert.assertTrue("Should be Arrow Timestamp type", arrowType.isInstanceOf[ArrowType.Timestamp])
      val tsType = arrowType.asInstanceOf[ArrowType.Timestamp]
      Assert.assertEquals("Should use MICROSECOND unit", TimeUnit.MICROSECOND, tsType.getUnit)
      Assert.assertNull("Should have null timezone for TimestampNTZ", tsType.getTimezone)

    } catch {
      case _: ClassNotFoundException =>
        println("TimestampNTZType not available (Spark < 3.4), skipping test")
    }
  }

  @Test
  def testToArrowTypeTimestampType(): Unit = {
    // Test regular TimestampType should require timezone
    val arrowType = DorisArrowUtils.toArrowType(TimestampType, "UTC")
    Assert.assertTrue("Should be Arrow Timestamp type", arrowType.isInstanceOf[ArrowType.Timestamp])
    val tsType = arrowType.asInstanceOf[ArrowType.Timestamp]
    Assert.assertEquals("Should use MICROSECOND unit", TimeUnit.MICROSECOND, tsType.getUnit)
    Assert.assertEquals("Should have timezone", "UTC", tsType.getTimezone)
  }

  @Test
  def testFromArrowTypeTimestampNTZ(): Unit = {
    try {
      val timestampNTZClass = Class.forName("org.apache.spark.sql.types.TimestampNTZType$")
      val instance = timestampNTZClass.getField("MODULE$").get(null)
      val expectedTimestampNTZType = instance.asInstanceOf[org.apache.spark.sql.types.DataType]

      // Test Arrow Timestamp without timezone (TimestampNTZ)
      val arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
      val result = DorisArrowUtils.fromArrowType(arrowType)

      // Should return TimestampNTZType in Spark 3.4+
      Assert.assertEquals("Should return TimestampNTZType", expectedTimestampNTZType.getClass, result.getClass)

    } catch {
      case _: ClassNotFoundException =>
        // Spark < 3.4, should fall back to TimestampType
        val arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
        val result = DorisArrowUtils.fromArrowType(arrowType)
        Assert.assertEquals("Should fallback to TimestampType in Spark < 3.4", TimestampType, result)
    }
  }

  @Test
  def testFromArrowTypeTimestampWithTimezone(): Unit = {
    // Test Arrow Timestamp with timezone should return TimestampType
    val arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")
    val result = DorisArrowUtils.fromArrowType(arrowType)
    Assert.assertEquals("Should return TimestampType", TimestampType, result)
  }

  @Test
  def testToArrowFieldTimestampNTZ(): Unit = {
    try {
      val timestampNTZClass = Class.forName("org.apache.spark.sql.types.TimestampNTZType$")
      val instance = timestampNTZClass.getField("MODULE$").get(null)
      val timestampNTZType = instance.asInstanceOf[org.apache.spark.sql.types.DataType]

      // Test creating Arrow field from TimestampNTZType
      val field = DorisArrowUtils.toArrowField("test_ntz", timestampNTZType, nullable = true, null)

      Assert.assertEquals("Field name should match", "test_ntz", field.getName)
      Assert.assertTrue("Field should be nullable", field.isNullable)
      val fieldType = field.getType.asInstanceOf[ArrowType.Timestamp]
      Assert.assertEquals("Should use MICROSECOND unit", TimeUnit.MICROSECOND, fieldType.getUnit)
      Assert.assertNull("Should have null timezone", fieldType.getTimezone)

    } catch {
      case _: ClassNotFoundException =>
        println("TimestampNTZType not available (Spark < 3.4), skipping test")
    }
  }

}
