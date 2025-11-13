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

package org.apache.spark.sql.execution.arrow

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.{TimeStampMicroVector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{DataTypes, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.DorisArrowUtils
import org.junit.Assert
import org.junit.jupiter.api.Test

import java.util

class DorisArrowWriterTest {

  @Test
  def testCreateWriterWithTimestampNTZ(): Unit = {
    try {
      val timestampNTZClass = Class.forName("org.apache.spark.sql.types.TimestampNTZType$")
      val instance = timestampNTZClass.getField("MODULE$").get(null)
      val timestampNTZType = instance.asInstanceOf[org.apache.spark.sql.types.DataType]

      // Create schema with TimestampNTZType
      val schema = StructType(Seq(
        StructField("id", DataTypes.IntegerType, nullable = false),
        StructField("ts_ntz", timestampNTZType, nullable = true)
      ))

      // Create Arrow schema and writer
      val arrowSchema = DorisArrowUtils.toArrowSchema(schema, null)
      val root = VectorSchemaRoot.create(arrowSchema, new RootAllocator(Long.MaxValue))
      val writer = DorisArrowWriter.create(root)

      Assert.assertNotNull("Writer should be created", writer)

      // Verify the schema
      val writerSchema = writer.schema
      Assert.assertEquals("Schema should have 2 fields", 2, writerSchema.fields.length)
      Assert.assertEquals("First field should be IntegerType", DataTypes.IntegerType, writerSchema.fields(0).dataType)
      Assert.assertEquals("Second field should be TimestampNTZType", timestampNTZType.getClass, writerSchema.fields(1).dataType.getClass)

      // Test writing data
      val row = new GenericInternalRow(Array[Any](1, 1234567890000L)) // microsecond timestamp
      writer.write(row)
      writer.finish()

      // Verify data was written
      val fieldVector = root.getVector("ts_ntz")
      Assert.assertTrue("Should be TimeStampVector", fieldVector.isInstanceOf[TimeStampMicroVector])
      val tsVector = fieldVector.asInstanceOf[TimeStampMicroVector]
      Assert.assertEquals("Should have 1 row", 1, root.getRowCount)
      Assert.assertEquals("Timestamp value should match", 1234567890000L, tsVector.get(0))

      root.close()

    } catch {
      case _: ClassNotFoundException =>
        println("TimestampNTZType not available (Spark < 3.4), skipping test")
    }
  }

  @Test
  def testTimestampNTZWriter(): Unit = {
    try {
      val timestampNTZClass = Class.forName("org.apache.spark.sql.types.TimestampNTZType$")
      val instance = timestampNTZClass.getField("MODULE$").get(null)
      val timestampNTZType = instance.asInstanceOf[org.apache.spark.sql.types.DataType]

      // Create Arrow field with TimestampNTZ (null timezone)
      val fieldType = new FieldType(true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), null)
      val field = new Field("ts_ntz", fieldType, null)
      val allocator = new RootAllocator(Long.MaxValue)
      val vector = field.createVector(allocator).asInstanceOf[TimeStampMicroVector]
      vector.allocateNew()

      // Create root and writer (should use DorisTimestampNTZWriter)
      val root = VectorSchemaRoot.create(
        new Schema(util.Collections.singletonList(field), null),
        allocator)
      val arrowWriter = DorisArrowWriter.create(root)

      // Write data
      val row = new GenericInternalRow(Array[Any](1234567890000L))
      arrowWriter.write(row)
      arrowWriter.finish()

      // Verify - get vector from root
      val actualVector = root.getVector("ts_ntz").asInstanceOf[TimeStampMicroVector]
      Assert.assertEquals("Should have 1 row", 1, root.getRowCount)
      Assert.assertEquals("Timestamp value should match", 1234567890000L, actualVector.get(0))

      root.close()
      allocator.close()

    } catch {
      case _: ClassNotFoundException =>
        println("TimestampNTZType not available (Spark < 3.4), skipping test")
    }
  }

}
