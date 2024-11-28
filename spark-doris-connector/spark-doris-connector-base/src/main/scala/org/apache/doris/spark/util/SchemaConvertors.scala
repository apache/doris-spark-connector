package org.apache.doris.spark.util

import org.apache.commons.lang3.StringUtils
import org.apache.doris.sdk.thrift.{TPrimitiveType, TScanColumnDesc}
import org.apache.doris.spark.rest.models.{Field, Schema}
import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType, MapType}

import scala.collection.JavaConverters._
import scala.collection.mutable

object SchemaConvertors {

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
      case "DATETIME" => DataTypes.StringType
      case "DATETIMEV2" => DataTypes.StringType
      case "BINARY" => DataTypes.BinaryType
      case "DECIMAL" => DecimalType(precision, scale)
      case "CHAR" => DataTypes.StringType
      case "LARGEINT" => DecimalType(38, 0)
      case "VARCHAR" => DataTypes.StringType
      case "JSON" => DataTypes.StringType
      case "JSONB" => DataTypes.StringType
      case "DECIMALV2" => DecimalType(precision, scale)
      case "DECIMAL32" => DecimalType(precision, scale)
      case "DECIMAL64" => DecimalType(precision, scale)
      case "DECIMAL128" => DecimalType(precision, scale)
      case "TIME" => DataTypes.DoubleType
      case "STRING" => DataTypes.StringType
      case "ARRAY" => DataTypes.StringType
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
      println(desc.getName + " " + desc.getType.name())
      schema.put(new Field(desc.getName, desc.getType.name, "", 0, 0, ""))
    })
    schema
  }

}
