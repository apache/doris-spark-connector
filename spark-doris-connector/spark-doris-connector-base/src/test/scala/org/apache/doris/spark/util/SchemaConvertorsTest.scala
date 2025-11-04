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

import org.apache.spark.sql.types.{ArrayType, DataTypes, DecimalType, MapType}
import org.junit.Assert
import org.junit.jupiter.api.Test

class SchemaConvertorsTest {

  @Test
  def toCatalystTypeTest(): Unit = {

    Assert.assertEquals(SchemaConvertors.toCatalystType("NULL_TYPE", -1, -1), DataTypes.NullType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("BOOLEAN", -1, -1), DataTypes.BooleanType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("TINYINT", -1, -1), DataTypes.ByteType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("SMALLINT", -1, -1), DataTypes.ShortType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("INT", -1, -1), DataTypes.IntegerType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("BIGINT", -1, -1), DataTypes.LongType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("FLOAT", -1, -1), DataTypes.FloatType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("DOUBLE", -1, -1), DataTypes.DoubleType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("DATE", -1, -1), DataTypes.DateType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("DATEV2", -1, -1), DataTypes.DateType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("DATETIME", -1, -1), DataTypes.TimestampType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("DATETIMEV2", -1, -1), DataTypes.TimestampType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("BINARY", -1, -1), DataTypes.BinaryType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("DECIMAL", 20, 4), DecimalType(20, 4))
    Assert.assertEquals(SchemaConvertors.toCatalystType("CHAR", -1, -1), DataTypes.StringType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("LARGEINT", -1, -1), DecimalType(38, 0))
    Assert.assertEquals(SchemaConvertors.toCatalystType("VARCHAR", -1, -1), DataTypes.StringType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("JSON", -1, -1), DataTypes.StringType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("JSONB", -1, -1), DataTypes.StringType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("DECIMALV2", 20, 4), DecimalType(20, 4))
    Assert.assertEquals(SchemaConvertors.toCatalystType("DECIMAL32", 20, 4), DecimalType(20, 4))
    Assert.assertEquals(SchemaConvertors.toCatalystType("DECIMAL64", 20, 4), DecimalType(20, 4))
    Assert.assertEquals(SchemaConvertors.toCatalystType("DECIMAL128", 20, 4), DecimalType(20, 4))
    Assert.assertEquals(SchemaConvertors.toCatalystType("TIME", -1, -1), DataTypes.DoubleType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("STRING", -1, -1), DataTypes.StringType)
    // ARRAY type should return ArrayType(StringType, containsNull = true) for backward compatibility
    val arrayType = SchemaConvertors.toCatalystType("ARRAY", -1, -1)
    Assert.assertTrue(arrayType.isInstanceOf[ArrayType])
    Assert.assertEquals(arrayType.asInstanceOf[ArrayType].elementType, DataTypes.StringType)
    Assert.assertTrue(arrayType.asInstanceOf[ArrayType].containsNull)
    Assert.assertEquals(SchemaConvertors.toCatalystType("MAP", -1, -1), MapType(DataTypes.StringType, DataTypes.StringType))
    Assert.assertEquals(SchemaConvertors.toCatalystType("STRUCT", -1, -1), DataTypes.StringType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("VARIANT", -1, -1), DataTypes.StringType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("IPV4", -1, -1), DataTypes.StringType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("IPV6", -1, -1), DataTypes.StringType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("BITMAP", -1, -1), DataTypes.StringType)
    Assert.assertEquals(SchemaConvertors.toCatalystType("HLL", -1, -1), DataTypes.StringType)

  }

}
