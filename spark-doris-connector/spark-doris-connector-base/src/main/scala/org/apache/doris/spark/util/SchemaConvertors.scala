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

import org.apache.doris.sdk.thrift.TScanColumnDesc
import org.apache.doris.spark.rest.models.{Field, Schema}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, MapType}

object SchemaConvertors {

  /**
   * Convert inferred element type string to Spark DataType
   * Used for ARRAY type precision inference
   * 
   * @param elementTypeString the inferred element type (e.g., "INT", "STRING", "ARRAY<INT>")
   * @param precision precision for decimal types
   * @param scale scale for decimal types
   * @return Spark DataType
   */
  def elementTypeStringToDataType(elementTypeString: String, precision: Int = -1, scale: Int = -1): DataType = {
    if (elementTypeString == null || elementTypeString.isEmpty) {
      return DataTypes.StringType // Default fallback
    }
    
    // Handle nested arrays (e.g., "ARRAY<INT>")
    if (elementTypeString.startsWith("ARRAY<") && elementTypeString.endsWith(">")) {
      val innerType = elementTypeString.substring(6, elementTypeString.length - 1)
      val elementType = elementTypeStringToDataType(innerType, precision, scale)
      return ArrayType(elementType, containsNull = true)
    }
    
    // Handle primitive types
    elementTypeString match {
      case "TINYINT" => DataTypes.ByteType
      case "SMALLINT" => DataTypes.ShortType
      case "INT" => DataTypes.IntegerType
      case "BIGINT" => DataTypes.LongType
      case "FLOAT" => DataTypes.FloatType
      case "DOUBLE" => DataTypes.DoubleType
      case "STRING" => DataTypes.StringType
      case "BOOLEAN" => DataTypes.BooleanType
      case "DATE" => DataTypes.DateType
      case "DATETIME" => DataTypes.TimestampType
      case "BINARY" => DataTypes.BinaryType
      case "DECIMAL" => if (precision > 0 && scale >= 0) DecimalType(precision, scale) else DecimalType(10, 0)
      case _ => DataTypes.StringType // Default fallback
    }
  }

  @throws[IllegalArgumentException]
  def toCatalystType(dorisType: String, precision: Int, scale: Int): DataType = {
    dorisType match {
      case "NULL_TYPE" => DataTypes.NullType
      case "BOOLEAN" => DataTypes.BooleanType
      case "TINYINT" => DataTypes.ByteType
      case "SMALLINT" => DataTypes.ShortType
      case "INT" => DataTypes.IntegerType
      case "BIGINT" => DataTypes.LongType
      case "FLOAT" => DataTypes.FloatType
      case "DOUBLE" => DataTypes.DoubleType
      case "DATE" => DataTypes.DateType
      case "DATEV2" => DataTypes.DateType
      case "DATETIME" => DataTypes.TimestampType
      case "DATETIMEV2" => DataTypes.TimestampType
      case "BINARY" => DataTypes.BinaryType
      case "DECIMAL" => DecimalType(precision, scale)
      case "CHAR" => DataTypes.StringType
      case "LARGEINT" => DataTypes.StringType
      case "VARCHAR" => DataTypes.StringType
      case "JSON" => DataTypes.StringType
      case "JSONB" => DataTypes.StringType
      case "DECIMALV2" => DecimalType(precision, scale)
      case "DECIMAL32" => DecimalType(precision, scale)
      case "DECIMAL64" => DecimalType(precision, scale)
      case "DECIMAL128" => DecimalType(precision, scale)
      case "TIME" => DataTypes.DoubleType
      case "STRING" => DataTypes.StringType
      case "ARRAY" => ArrayType(DataTypes.StringType, containsNull = true)
        // Default to ArrayType(StringType) for backward compatibility.
        // Actual element type is inferred from Arrow schema at runtime during data conversion.
        // RowBatch will read actual element types from Arrow ListVector, and RowConvertors
        // will handle type conversions automatically (e.g., Integer -> String if schema declares StringType).
      case "MAP" => MapType(DataTypes.StringType, DataTypes.StringType)
      case "STRUCT" => DataTypes.StringType
      case "VARIANT" => DataTypes.StringType
      case "IPV4" => DataTypes.StringType
      case "IPV6" => DataTypes.StringType
      case "BITMAP" => DataTypes.StringType // Placeholder only, no support for reading
      case "HLL" => DataTypes.StringType // Placeholder only, no support for reading
      case _ => throw new Exception("Unrecognized Doris type " + dorisType)
    }
  }

  def convertToSchema(tscanColumnDescs: Seq[TScanColumnDesc]): Schema = {
    val schema = new Schema(tscanColumnDescs.length)
    tscanColumnDescs.foreach(desc => {
      schema.put(new Field(desc.getName, desc.getType.name, "", 0, 0, ""))
    })
    schema
  }

}
