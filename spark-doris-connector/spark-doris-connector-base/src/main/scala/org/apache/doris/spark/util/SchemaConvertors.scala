package org.apache.doris.spark.util

import org.apache.commons.lang3.StringUtils
import org.apache.doris.sdk.thrift.{TPrimitiveType, TScanColumnDesc}
import org.apache.doris.spark.client.{DorisSchema, Field}
import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType, MapType}

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

  def convertToSchema(tscanColumnDescs: Seq[TScanColumnDesc], readFields: Array[String], unsupportedCols: Array[String]): DorisSchema = {
    val fieldList = fieldUnion(readFields, unsupportedCols, tscanColumnDescs)
    DorisSchema(0, "", fieldList)
  }

  private def fieldUnion(readColumns: Array[String], unsupportedCols: Array[String], tScanColumnDescSeq: Seq[TScanColumnDesc]): List[Field] = {
    val fieldList = mutable.Buffer[Field]()
    var rcIdx = 0;
    var tsdIdx = 0;
    while (rcIdx < readColumns.length || tsdIdx < tScanColumnDescSeq.length) {
      if (rcIdx < readColumns.length) {
        if (StringUtils.equals(readColumns(rcIdx), tScanColumnDescSeq(tsdIdx).getName)) {
          fieldList += Field(tScanColumnDescSeq(tsdIdx).getName, tScanColumnDescSeq(tsdIdx).getType.name, "", "")
          rcIdx += 1
          tsdIdx += 1
        } else if (unsupportedCols.contains(readColumns(rcIdx))) {
          fieldList += Field(readColumns(rcIdx), TPrimitiveType.VARCHAR.name, "", "")
          rcIdx += 1
        }
      } else {
        fieldList += Field(tScanColumnDescSeq(tsdIdx).getName, tScanColumnDescSeq(tsdIdx).getType.name, "", "")
        tsdIdx += 1
      }
    }
    fieldList.toList
  }

}
