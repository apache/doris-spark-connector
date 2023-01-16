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

import scala.collection.JavaConverters._

import org.apache.doris.spark.cfg.Settings
import org.apache.doris.spark.exception.DorisException
import org.apache.doris.spark.rest.RestService
import org.apache.doris.spark.rest.models.{Field, Schema}
import org.apache.doris.thrift.TScanColumnDesc
import org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_READ_FIELD
import org.apache.spark.sql.types._

import org.slf4j.LoggerFactory

import scala.collection.mutable

private[spark] object SchemaUtils {
  private val logger = LoggerFactory.getLogger(SchemaUtils.getClass.getSimpleName.stripSuffix("$"))

  /**
   * discover Doris table schema from Doris FE.
   * @param cfg configuration
   * @return Spark Catalyst StructType
   */
  def discoverSchema(cfg: Settings): StructType = {
    val schema = discoverSchemaFromFe(cfg)
    convertToStruct(cfg.getProperty(DORIS_READ_FIELD), schema)
  }

  /**
   * discover Doris table schema from Doris FE.
   * @param cfg configuration
   * @return inner schema struct
   */
  def discoverSchemaFromFe(cfg: Settings): Schema = {
    RestService.getSchema(cfg, logger)
  }

  /**
   * convert inner schema struct to Spark Catalyst StructType
   * @param schema inner schema
   * @return Spark Catalyst StructType
   */
  def convertToStruct(dorisReadFields: String, schema: Schema): StructType = {
    var fieldList = new Array[String](schema.size())
    val fieldSet = new mutable.HashSet[String]()
    var fields = List[StructField]()
    if (dorisReadFields != null && dorisReadFields.length > 0) {
      fieldList = dorisReadFields.split(",")
      for (field <- fieldList) {
        fieldSet.add(field)
      }
      schema.getProperties.asScala.foreach(f =>
        if (fieldSet.contains(f.getName)) {
          fields :+= DataTypes.createStructField(f.getName, getCatalystType(f.getType, f.getPrecision, f.getScale), true)
        })
    } else {
      schema.getProperties.asScala.foreach(f =>
        fields :+= DataTypes.createStructField(f.getName, getCatalystType(f.getType, f.getPrecision, f.getScale), true)
      )
    }
    DataTypes.createStructType(fields.asJava)
  }

  /**
   * translate Doris Type to Spark Catalyst type
   * @param dorisType Doris type
   * @param precision decimal precision
   * @param scale decimal scale
   * @return Spark Catalyst type
   */
  def getCatalystType(dorisType: String, precision: Int, scale: Int): DataType = {
    dorisType match {
      case "NULL_TYPE"       => DataTypes.NullType
      case "BOOLEAN"         => DataTypes.BooleanType
      case "TINYINT"         => DataTypes.ByteType
      case "SMALLINT"        => DataTypes.ShortType
      case "INT"             => DataTypes.IntegerType
      case "BIGINT"          => DataTypes.LongType
      case "FLOAT"           => DataTypes.FloatType
      case "DOUBLE"          => DataTypes.DoubleType
      case "DATE"            => DataTypes.StringType
      case "DATEV2"          => DataTypes.StringType
      case "DATETIME"        => DataTypes.StringType
      case "DATETIMEV2"      => DataTypes.StringType
      case "BINARY"          => DataTypes.BinaryType
      case "DECIMAL"         => DecimalType(precision, scale)
      case "CHAR"            => DataTypes.StringType
      case "LARGEINT"        => DataTypes.StringType
      case "VARCHAR"         => DataTypes.StringType
      case "DECIMALV2"       => DecimalType(precision, scale)
      case "DECIMAL32"       => DecimalType(precision, scale)
      case "DECIMAL64"       => DecimalType(precision, scale)
      case "DECIMAL128I"     => DecimalType(precision, scale)
      case "TIME"            => DataTypes.DoubleType
      case "STRING"          => DataTypes.StringType
      case "HLL"             =>
        throw new DorisException("Unsupported type " + dorisType)
      case _                 =>
        throw new DorisException("Unrecognized Doris type " + dorisType)
    }
  }

  /**
   * convert Doris return schema to inner schema struct.
   * @param tscanColumnDescs Doris BE return schema
   * @return inner schema struct
   */
  def convertToSchema(tscanColumnDescs: Seq[TScanColumnDesc]): Schema = {
    val schema = new Schema(tscanColumnDescs.length)
    logger.info(s"column size: ${tscanColumnDescs.length}")
    tscanColumnDescs.foreach(desc => {
      if (desc == null) {
        logger.info("desc is null")
      }
      logger.info(s"descName: ${desc.getName}")
      logger.info(s"descTypeName: ${desc.getType.name}")
      schema.put(new Field(desc.getName, desc.getType.name, "", 0, 0, ""))
    })
    schema
  }
}
