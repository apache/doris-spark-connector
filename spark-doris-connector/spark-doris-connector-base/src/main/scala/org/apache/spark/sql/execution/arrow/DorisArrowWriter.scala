/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.arrow

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.DorisArrowUtils

import scala.collection.JavaConverters._

/**
 * Helper object to detect TimestampNTZType using reflection for Spark 3.4+ compatibility.
 */
object TimestampNTZHelper {
  /**
   * Try to get TimestampNTZType using reflection for Spark 3.4+ compatibility.
   */
  private lazy val timestampNTZTypeOption: Option[DataType] = {
    try {
      val timestampNTZClass = Class.forName("org.apache.spark.sql.types.TimestampNTZType$")
      val instance = timestampNTZClass.getField("MODULE$").get(null)
      Some(instance.asInstanceOf[DataType])
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException | _: NoSuchMethodException =>
        None
    }
  }

  /**
   * Check if a DataType is TimestampNTZType (for Spark 3.4+).
   */
  def isTimestampNTZType(dt: DataType): Boolean = {
    timestampNTZTypeOption.exists(_.getClass == dt.getClass)
  }
}

/**
 * Copied from Spark 3.1.2. To avoid the package conflicts between spark 2 and spark 3.
 */
object DorisArrowWriter {

  def create(schema: StructType, timeZoneId: String): DorisArrowWriter = {
    val arrowSchema = DorisArrowUtils.toArrowSchema(schema, timeZoneId)
    val root = VectorSchemaRoot.create(arrowSchema, DorisArrowUtils.rootAllocator)
    create(root)
  }

  def create(root: VectorSchemaRoot): DorisArrowWriter = {
    val children = root.getFieldVectors().asScala.map { vector =>
      vector.allocateNew()
      createFieldWriter(vector)
    }
    new DorisArrowWriter(root, children.toArray)
  }

  private def createFieldWriter(vector: ValueVector): DorisArrowFieldWriter = {
    val field = vector.getField()
    val dataType = DorisArrowUtils.fromArrowField(field)
    
    // Check for TimestampNTZType first (Spark 3.4+)
    if (TimestampNTZHelper.isTimestampNTZType(dataType)) {
      // TimestampNTZType uses TimeStampMicroVector (without timezone)
      vector match {
        case tsVector: TimeStampVector if tsVector.getField.getType.asInstanceOf[org.apache.arrow.vector.types.pojo.ArrowType.Timestamp].getTimezone == null =>
          new DorisTimestampNTZWriter(tsVector)
        case _ =>
          throw new UnsupportedOperationException(
            s"TimestampNTZType requires TimeStampMicroVector without timezone, but got ${vector.getClass}")
      }
    } else {
      (dataType, vector) match {
        case (BooleanType, vector: BitVector) => new DorisBooleanWriter(vector)
        case (ByteType, vector: TinyIntVector) => new DorisByteWriter(vector)
        case (ShortType, vector: SmallIntVector) => new DorisShortWriter(vector)
        case (IntegerType, vector: IntVector) => new DorisIntegerWriter(vector)
        case (LongType, vector: BigIntVector) => new DorisLongWriter(vector)
        case (FloatType, vector: Float4Vector) => new DorisFloatWriter(vector)
        case (DoubleType, vector: Float8Vector) => new DorisDoubleWriter(vector)
        case (DecimalType.Fixed(precision, scale), vector: DecimalVector) =>
          new DorisDecimalWriter(vector, precision, scale)
        case (StringType, vector: VarCharVector) => new DorisStringWriter(vector)
        case (BinaryType, vector: VarBinaryVector) => new DorisBinaryWriter(vector)
        case (DateType, vector: DateDayVector) => new DorisDateWriter(vector)
        case (TimestampType, vector: TimeStampMicroTZVector) => new DorisTimestampWriter(vector)
        case (ArrayType(_, _), vector: ListVector) =>
          val elementVector = createFieldWriter(vector.getDataVector())
          new DorisArrayWriter(vector, elementVector)
        case (MapType(_, _, _), vector: MapVector) =>
          val structVector = vector.getDataVector.asInstanceOf[StructVector]
          val keyWriter = createFieldWriter(structVector.getChild(MapVector.KEY_NAME))
          val valueWriter = createFieldWriter(structVector.getChild(MapVector.VALUE_NAME))
          new DorisMapWriter(vector, structVector, keyWriter, valueWriter)
        case (StructType(_), vector: StructVector) =>
          val children = (0 until vector.size()).map { ordinal =>
            createFieldWriter(vector.getChildByOrdinal(ordinal))
          }
          new DorisStructWriter(vector, children.toArray)
        // Add explicit case for TimestampNTZType to avoid MatchError
        case (dt, _) if TimestampNTZHelper.isTimestampNTZType(dt) =>
          vector match {
            case tsVector: TimeStampVector if tsVector.getField.getType.asInstanceOf[org.apache.arrow.vector.types.pojo.ArrowType.Timestamp].getTimezone == null =>
              new DorisTimestampNTZWriter(tsVector)
            case _ =>
              throw new UnsupportedOperationException(
                s"TimestampNTZType requires TimeStampMicroVector without timezone, but got ${vector.getClass}")
          }
        case (dt, _) =>
          throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
      }
    }
  }
}

class DorisArrowWriter(val root: VectorSchemaRoot, fields: Array[DorisArrowFieldWriter]) {

  def schema: StructType = StructType(fields.map { f =>
    StructField(f.name, f.dataType, f.nullable)
  })

  private var count: Int = 0

  def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fields.size) {
      fields(i).write(row, i)
      i += 1
    }
    count += 1
  }

  def finish(): Unit = {
    root.setRowCount(count)
    fields.foreach(_.finish())
  }

  def reset(): Unit = {
    root.setRowCount(0)
    count = 0
    fields.foreach(_.reset())
  }
}

private[spark] abstract class DorisArrowFieldWriter {

  def valueVector: ValueVector

  def name: String = valueVector.getField().getName()
  def dataType: DataType = DorisArrowUtils.fromArrowField(valueVector.getField())
  def nullable: Boolean = valueVector.getField().isNullable()

  def setNull(): Unit
  def setValue(input: SpecializedGetters, ordinal: Int): Unit

  private[spark] var count: Int = 0

  def write(input: SpecializedGetters, ordinal: Int): Unit = {
    if (input.isNullAt(ordinal)) {
      setNull()
    } else {
      setValue(input, ordinal)
    }
    count += 1
  }

  def finish(): Unit = {
    valueVector.setValueCount(count)
  }

  def reset(): Unit = {
    valueVector.reset()
    count = 0
  }
}

private[spark] class DorisBooleanWriter(val valueVector: BitVector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, if (input.getBoolean(ordinal)) 1 else 0)
  }
}

private[spark] class DorisByteWriter(val valueVector: TinyIntVector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getByte(ordinal))
  }
}

private[spark] class DorisShortWriter(val valueVector: SmallIntVector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getShort(ordinal))
  }
}

private[spark] class DorisIntegerWriter(val valueVector: IntVector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[spark] class DorisLongWriter(val valueVector: BigIntVector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[spark] class DorisFloatWriter(val valueVector: Float4Vector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getFloat(ordinal))
  }
}

private[spark] class DorisDoubleWriter(val valueVector: Float8Vector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getDouble(ordinal))
  }
}

private[spark] class DorisDecimalWriter(
                                    val valueVector: DecimalVector,
                                    precision: Int,
                                    scale: Int) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val decimal = input.getDecimal(ordinal, precision, scale)
    if (decimal.changePrecision(precision, scale)) {
      valueVector.setSafe(count, decimal.toJavaBigDecimal)
    } else {
      setNull()
    }
  }
}

private[spark] class DorisStringWriter(val valueVector: VarCharVector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    val utf8ByteBuffer = utf8.getByteBuffer
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueVector.setSafe(count, utf8ByteBuffer, utf8ByteBuffer.position(), utf8.numBytes())
  }
}

private[spark] class DorisBinaryWriter(
                                   val valueVector: VarBinaryVector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes, 0, bytes.length)
  }
}

private[spark] class DorisDateWriter(val valueVector: DateDayVector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[spark] class DorisTimestampWriter(
                                      val valueVector: TimeStampMicroTZVector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

/**
 * Writer for TimestampNTZType (Spark 3.4+).
 * Uses TimeStampMicroVector without timezone instead of TimeStampMicroTZVector.
 */
private[spark] class DorisTimestampNTZWriter(
                                      val valueVector: TimeStampVector) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    // TimestampNTZType stores microsecond timestamp directly, same as TimestampType
    // The difference is that TimestampNTZType doesn't apply timezone conversion
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[spark] class DorisArrayWriter(
                                  val valueVector: ListVector,
                                  val elementWriter: DorisArrowFieldWriter) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val array = input.getArray(ordinal)
    var i = 0
    valueVector.startNewValue(count)
    while (i < array.numElements()) {
      elementWriter.write(array, i)
      i += 1
    }
    valueVector.endValue(count, array.numElements())
  }

  override def finish(): Unit = {
    super.finish()
    elementWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    elementWriter.reset()
  }
}

private[spark] class DorisStructWriter(
                                   val valueVector: StructVector,
                                   children: Array[DorisArrowFieldWriter]) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).setNull()
      children(i).count += 1
      i += 1
    }
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val struct = input.getStruct(ordinal, children.length)
    var i = 0
    valueVector.setIndexDefined(count)
    while (i < struct.numFields) {
      children(i).write(struct, i)
      i += 1
    }
  }

  override def finish(): Unit = {
    super.finish()
    children.foreach(_.finish())
  }

  override def reset(): Unit = {
    super.reset()
    children.foreach(_.reset())
  }
}

private[spark] class DorisMapWriter(
                                val valueVector: MapVector,
                                val structVector: StructVector,
                                val keyWriter: DorisArrowFieldWriter,
                                val valueWriter: DorisArrowFieldWriter) extends DorisArrowFieldWriter {

  override def setNull(): Unit = {}

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val map = input.getMap(ordinal)
    valueVector.startNewValue(count)
    val keys = map.keyArray()
    val values = map.valueArray()
    var i = 0
    while (i <  map.numElements()) {
      structVector.setIndexDefined(keyWriter.count)
      keyWriter.write(keys, i)
      valueWriter.write(values, i)
      i += 1
    }

    valueVector.endValue(count, map.numElements())
  }

  override def finish(): Unit = {
    super.finish()
    keyWriter.finish()
    valueWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    keyWriter.reset()
    valueWriter.reset()
  }
}
